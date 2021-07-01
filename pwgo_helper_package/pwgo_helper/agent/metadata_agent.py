"""Container module for the metadata agent service"""
import signal, json, asyncio
from asyncio.tasks import Task
from asyncio.futures import Future

import click
from pwgo_helper.agent.image_virtual_path_event_task import ImageVirtualPathEventTask
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent
from py_linq import Enumerable

from . import strings
from .event_dispatcher import EventDispatcher
from ..config import Configuration as ProgramConfiguration
from .config import Configuration as AgentConfiguration
from .autotagger import AutoTagger
from .pwgo_image import PiwigoImage
from .db_connection_pool import DbConnectionPool as DbPool
from .image_virtual_path_event_task import ImageVirtualPathEventTask
from .utilities import parse_sql

class MetadataAgent():
    """The main driver of the program"""
    def __init__(self, logger) -> None:
        self._logger = logger
        self._evt_monitor_task = None
        self._evt_dispatcher = None
        self._binlog_stream = None
        self._is_running = False
        self._stopping_task = None

    def __await__(self):
        yield from self.start().__await__()
        yield from self._evt_monitor_task.__await__()
        yield from self._stopping_task.__await__()

    def _signal_handler(self, sig):
        self._logger.info(strings.LOG_HANDLE_SIG(sig.name))
        forced_stop_cfg = {
            signal.SIGINT: True,
            signal.SIGTERM: True,
            signal.SIGQUIT: False
        }
        asyncio.ensure_future(self.stop(forced_stop_cfg[sig]))

    async def start(self):
        """starts the metadata agent service"""
        #attach signal handlers
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, self._signal_handler, signal.SIGINT)
        loop.add_signal_handler(signal.SIGTERM, self._signal_handler, signal.SIGTERM)
        loop.add_signal_handler(signal.SIGQUIT, self._signal_handler, signal.SIGQUIT)
        # do an initial sync of the face index
        face_idx_job = AutoTagger.sync_face_index()
        # rebuild the virtualfs
        virtualfs_job = ImageVirtualPathEventTask.rebuild_virtualfs()

        await asyncio.gather(face_idx_job, virtualfs_job)

        self._evt_dispatcher = await EventDispatcher.create(AgentConfiguration.get().workers)
        self._evt_monitor_task = await self._start_event_monitor()
        self._is_running = True
        await self.process_autotag_backlog()

    async def stop(self, force=False):
        """stops the metadata agent service"""
        try:
            if not self._stopping_task:
                self._stopping_task = asyncio.tasks.current_task()
            if self._evt_monitor_task and not self._evt_monitor_task.done():
                self._evt_monitor_task.request_cancel = True
            if self._evt_dispatcher and self._evt_dispatcher.state == "RUNNING":
                stop_dispatch_task = asyncio.create_task(self._evt_dispatcher.stop(force=force))
            else:
                stop_dispatch_task = Future()
                stop_dispatch_task.set_result(True)
            await asyncio.wait([stop_dispatch_task,self._evt_monitor_task], timeout=AgentConfiguration.get().stop_timeout)
            self._evt_dispatcher.get_results()

        finally:
            if self._binlog_stream:
                self._binlog_stream.close()
            self._is_running = False

    async def _start_event_monitor(self) -> Task:
        """Starts a BinLogStreamReader to monitor for mysql events
        that need to be handled"""

        db_cfg = AgentConfiguration.get().pwgo_db_config
        self._logger.info("Monitoring %s for metadata changes", db_cfg["name"])

        blog_args = {
            "connection_settings": {
                "host": db_cfg["host"],
                "port": db_cfg["port"],
                "user": db_cfg["user"],
                "passwd": db_cfg["passwd"]
            },
            "server_id": 1,
            "only_schemas": [db_cfg["name"], AgentConfiguration.get().msg_db],
            "only_tables": AgentConfiguration.get().event_tables.keys(),
            "only_events": [WriteRowsEvent],
            "blocking": False,
            "resume_stream": True,
        }
        self._binlog_stream = BinLogStreamReader(**blog_args)

        mon_task = asyncio.create_task(self._event_monitor())
        mon_task.request_cancel = False
        await asyncio.sleep(0)
        return mon_task

    async def _event_monitor(self):
        task = asyncio.tasks.current_task()
        while not task.request_cancel:
            if self._evt_dispatcher.state != "RUNNING":
                try:
                    _ = self._evt_dispatcher.get_results()
                # pylint: disable=broad-except
                except Exception as err:
                    self._logger.exception(str(err))
                finally:
                    self._stopping_task = asyncio.create_task(self.stop(force=True))
                    raise RuntimeError("dispatcher is not running...stopping metadata agent")

            for evt in self._binlog_stream:
                self._logger.debug("Processing %s on %s affecting %s rows"
                    , type(evt), evt.table, len(evt.rows)
                )

                for row in evt.rows:
                    await self._evt_dispatcher.queue_event(row)

                self._logger.debug("Event queued...listening for new events")

            await asyncio.sleep(.1)

    async def process_autotag_backlog(self):
        """process any existing images that are waiting in the auto tag album
            and initialize the tags for any previously autotagged images"""
        self._logger.debug("processing any autotag backlog photos")
        async with DbPool.get().acquire_dict_cursor(db=AgentConfiguration.get().pwgo_db_config["name"]) as (cur, _):
            sql = """
                SELECT i.id, i.file, i.path
                FROM images i
                JOIN image_category ic
                ON ic.image_id = i.id
                WHERE ic.category_id = %s
            """
            await cur.execute(sql, (AgentConfiguration.get().auto_tag_alb))
            tag_imgs = [PiwigoImage(**row) for row in await cur.fetchall()]

        autotag_tasks = []
        for img in tag_imgs:
            async with AutoTagger.create(img) as tagger:
                autotag_tasks.append(asyncio.create_task(tagger.autotag_image()))
        await asyncio.gather(*autotag_tasks)

        async def add_img_tags(img_id, tag_ids):
            async with AutoTagger.create(img_id) as tagger:
                await tagger.add_tags(tag_ids)

        async with DbPool.get().acquire_dict_cursor(db=AgentConfiguration.get().pwgo_db_config["name"]) as (cur, _):
            # initialize the tags for any previously autotagged images
            # this is in case a new tag has been added to the piwigo table
            # that matches one that was previously detected by rekognition
            sql = """
                SELECT il.piwigo_image_id, t.id tag_id
                FROM rekognition.image_labels il
                JOIN tags t
                ON t.name = il.label
                LEFT JOIN image_tag it
                ON it.image_id = il.piwigo_image_id
                    AND it.tag_id = t.id
                WHERE it.image_id IS NULL
            """
            await cur.execute(sql)
            img_tags = [(r["piwigo_image_id"],r["tag_id"]) for r in await cur.fetchall()]

        if img_tags:
            imgs = Enumerable(img_tags).group_by(key_names=['img_id'], key=lambda x: x[0])
            tag_coros = []
            for img_grp in imgs:
                tag_ids = [t[1] for t in img_grp]
                tag_coros.append(add_img_tags(img_grp.key.img_id,tag_ids))
            await asyncio.gather(*tag_coros)

@click.command("agent")
@click.option(
    "--piwigo-galleries-host-path",
    help="""Host path of the piwigo gallieries folder. Can be any path that can be opened
    as a virtual filesystem by the fs package""",
    required=True,
)
@click.option(
    "--pwgo-db-host",help="hostname for piwigo db connection",type=str,required=True
)
@click.option(
    "--pwgo-db-port",help="port for piwigo db connection",type=int,required=False,default=3306
)
@click.option(
    "--pwgo-db-user",help="username for piwigo db connection",type=str,required=True
)
@click.option(
    "--pwgo-db-pw",help="password for piwigo db connection",type=str,required=True
)
@click.option(
    "--pwgo-db-name",help="name of the piwigo database",type=str,required=False,default="piwigo"
)
@click.option(
    "--rek-db-host",help="hostname for rekognition db connection",type=str,required=False
)
@click.option(
    "--rek-db-port",help="port for rekognition db connection",type=int,required=False,default=3306
)
@click.option(
    "--rek-db-user",help="username for rekognition db connection",type=str,required=False
)
@click.option(
    "--rek-db-pw",help="password for rekognition db connection",type=str,required=False
)
@click.option(
    "--rek-db-name",help="name of the rekognition database",type=str,required=False,default="rekognition"
)
@click.option(
    "--image-crop-save-path",
    help="Indicates the directory to which to save crops of faces detected in images. Crops are not saved by default.",
    type=click.Path(exists=True,file_okay=False)
)
@click.option(
    "--virtualfs-root",
    help="path to the root of the album-based virtual filesystem",
    type=click.Path(exists=True,file_okay=False)
)
@click.option(
    "--virtualfs-allow-broken-links",
    help="create symlinks even when source file path is not found",
    is_flag=True, default=True
)
@click.option(
    "--virtualfs-remove-empty-dirs",
    help="remove directories in virtual fs root that become empty when a symlink is removed",
    is_flag=True, default=True
)
@click.option(
    "--workers",
    help="Number of workers to handle event queue",
    type=int,
    default=5
)
@click.option(
    "--worker-error-limit",
    help="Number of processing errors to allow before quitting",
    type=int,
    default=5
)
@click.option(
    "-d", "--debug",
    help="Enable debug mode for asyncio event loop",
    is_flag=True
)
@click.option(
    "--initialize-db",
    help="Run database initialization scripts at startup",
    is_flag=True
)
def agent_entry(**kwargs):
    """Command used to auto generate related tags when a new tag is inserted in database"""
    logger = ProgramConfiguration.get().create_logger(__name__)

    logger.info("metadata_agent entry")
    async def exec_metadata_agent(**kwargs):
        logger.debug("initializing database connection pool...")
        await DbPool.initialize(
            kwargs["pwgo_db_host"],
            kwargs["pwgo_db_port"],
            kwargs["pwgo_db_user"],
            kwargs["pwgo_db_pw"]
        )
        for key,val in kwargs.items():
            show_val = val
            if key in ["pwgo_db_pw","rek_db_pw"]:
                show_val = "OMITTED"
            logger.debug(strings.LOG_AGNT_OPT(key,show_val))
        await AgentConfiguration.initialize(**kwargs)
        if kwargs["initialize_db"]:
            logger.debug(strings.LOG_INITIALIZE_DB)
            prg_cfg = ProgramConfiguration.get()
            exec_scripts = [
                prg_cfg.piwigo_db_scripts.create_category_paths,
                prg_cfg.piwigo_db_scripts.create_implicit_tags,
                prg_cfg.piwigo_db_scripts.create_image_metadata,
                prg_cfg.piwigo_db_scripts.create_image_virtual_paths,
                prg_cfg.piwigo_db_scripts.create_image_category_triggers,
                prg_cfg.piwigo_db_scripts.create_tags_triggers,
                prg_cfg.piwigo_db_scripts.create_image_tag_triggers,
                prg_cfg.piwigo_db_scripts.create_pwgo_message,
                prg_cfg.rekognition_db_scripts.create_rekognition_db,
                prg_cfg.rekognition_db_scripts.create_image_labels,
                prg_cfg.rekognition_db_scripts.create_index_faces,
                prg_cfg.rekognition_db_scripts.create_processed_faces
            ]
            async with DbPool.get().acquire_connection() as conn:
                async with conn.cursor() as cur:
                    for sql in exec_scripts:
                        stmts = parse_sql(sql)
                        for stmt in stmts:
                            await cur.execute(stmt)
                await conn.commit()
        logger.debug("starting and awaiting metadata agent")
        await MetadataAgent(logger)
        logger.debug("metadata agent returned")
        logger.debug("releasing database connection pool resources...")
        DbPool.get().terminate()

    loop = asyncio.get_event_loop()
    loop.set_debug(kwargs["debug"])
    loop.run_until_complete(exec_metadata_agent(**kwargs))
    logger.info("metadata agent exit stage left")