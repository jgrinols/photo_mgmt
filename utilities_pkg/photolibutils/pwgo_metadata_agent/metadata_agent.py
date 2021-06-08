"""Container module for the metadata agent service"""
import signal, json, asyncio, logging
from asyncio.tasks import Task
from asyncio.futures import Future

import click
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent
from path import Path
from py_linq import Enumerable

from . import strings
from .event_dispatcher import EventDispatcher
from .constants import Constants
from .autotagger import AutoTagger
from .pwgo_image import PiwigoImage
from .db_connection_pool import DbConnectionPool

logger = logging.getLogger(Constants.LOGGER_NAME)

class MetadataAgent():
    """The main driver of the program"""
    def __init__(self) -> None:
        self._evt_monitor_task = None
        self._evt_dispatcher = None
        self._binlog_stream = None
        self._is_running = False
        self._stopping_task = None
        with open(Constants.MYSQL_CFG_FILE) as cfg_file:
            self.db_cfg = json.load(cfg_file)

    def __await__(self):
        yield from self.start().__await__()
        yield from self._evt_monitor_task.__await__()
        yield from self._stopping_task.__await__()

    def _signal_handler(self, sig):
        logger.info(strings.LOG_HANDLE_SIG(sig.name))
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
        await AutoTagger.sync_face_index()

        self._evt_dispatcher = await EventDispatcher.create(Constants.WORKERS_CNT)
        self._evt_monitor_task = await self._start_event_monitor()
        self._is_running = True
        await self.process_autotag_backlog()

    async def stop(self, force=False):
        """stops the metadata agent service"""
        if not self._stopping_task:
            self._stopping_task = asyncio.tasks.current_task()
        if self._evt_monitor_task and not self._evt_monitor_task.done():
            self._evt_monitor_task.request_cancel = True
        if self._evt_dispatcher and self._evt_dispatcher.state == "RUNNING":
            stop_dispatch_task = asyncio.create_task(self._evt_dispatcher.stop(force=force))
        else:
            stop_dispatch_task = Future()
            stop_dispatch_task.set_result(True)
        await asyncio.wait([stop_dispatch_task,self._evt_monitor_task], timeout=Constants.STOP_TIMEOUT)

        if self._binlog_stream:
            self._binlog_stream.close()
        self._is_running = False

    async def _start_event_monitor(self) -> Task:
        """Starts a BinLogStreamReader to monitor for mysql events
        that need to be handled"""

        logger.info("Monitoring %s for metadata changes", Constants.PWGO_DB)

        blog_args = {
            "connection_settings": {
                "host": self.db_cfg["host"],
                "port": self.db_cfg["port"],
                "user": self.db_cfg["user"],
                "passwd": self.db_cfg["passwd"]
            },
            "server_id": 1,
            "only_schemas": [Constants.PWGO_DB, Constants.MSG_DB],
            "only_tables": Constants.EVENT_TABLES.keys(),
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
                except Exception:
                    logger.exception()
                finally:
                    self._stopping_task = asyncio.create_task(self.stop(force=True))
                    raise RuntimeError("dispatcher is not running...stopping metadata agent")

            for evt in self._binlog_stream:
                logger.debug("Processing %s on %s affecting %s rows"
                    , type(evt), evt.table, len(evt.rows)
                )

                for row in evt.rows:
                    await self._evt_dispatcher.queue_event(row)

                logger.debug("Event queued...listening for new events")

            await asyncio.sleep(.1)

    async def process_autotag_backlog(self):
        """process any existing images that are waiting in the auto tag album
            and initialize the tags for any previously autotagged images"""
        logger.debug("processing any autotag backlog photos")
        async with DbConnectionPool.get().acquire_dict_cursor(db=Constants.PWGO_DB) as (cur, _):
            sql = """
                SELECT i.id, i.file, i.path
                FROM images i
                JOIN image_category ic
                ON ic.image_id = i.id
                WHERE ic.category_id = %s
            """
            await cur.execute(sql, (Constants.AUTO_TAG_ALB))
            tag_imgs = [PiwigoImage(**row) for row in await cur.fetchall()]

        autotag_tasks = []
        for img in tag_imgs:
            async with AutoTagger.create(img) as tagger:
                autotag_tasks.append(asyncio.create_task(tagger.autotag_image()))
        await asyncio.gather(*autotag_tasks)

        async def add_img_tags(img_id, tag_ids):
            async with AutoTagger.create(img_id) as tagger:
                await tagger.add_tags(tag_ids)

        async with DbConnectionPool.get().acquire_dict_cursor(db=Constants.PWGO_DB) as (cur, _):
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

@click.command()
@click.option(
    "--piwigo-galleries-host-path",
    help="""Host path of the piwigo gallieries folder. Can be any path that can be opened
    as a virtual filesystem by the fs package""",
    required=True,
)
@click.option(
    "-db", "--database-config",
    help="json file specifying database connection parameters for piwigo database",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "--rekognition-config",
    help="json file specifying configuration values for aws rekognition api access",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "--image-crop-save-path",
    help="Indicates the directory to which to save crops of faces detected in images. Crops are not saved by default.",
    type=click.Path(exists=True,file_okay=False)
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
    "--dry-run",
    help="Don't perform any actions--just log what actions would be taken",
    is_flag=True
)
@click.option(
    "-v", "--verbosity",
    help="specifies the verbosity of the log output",
    type=click.Choice(["CRITICAL","ERROR","WARNING","INFO","DEBUG"]),
    default="INFO"
)
def entry(
    piwigo_galleries_host_path,
    database_config,
    rekognition_config,
    image_crop_save_path,
    workers,
    worker_error_limit,
    debug,
    dry_run,
    verbosity
):
    """Command used to auto generate related tags when a new tag is inserted in database"""
    logger.setLevel(verbosity)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(verbosity)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s: %(message)s"))
    logger.addHandler(console_handler)

    logger.info("Initializing metadata agent...")
    async def exec_metadata_agent(const_init):
        logger.debug("initializing database connection pool...")
        await DbConnectionPool.initialize(const_init["mysql_cfg_file"])
        logger.debug("initializing program configuration values...")
        await Constants.initialize_program_configs(**const_init)
        logger.debug("starting and awaiting metadata agent")
        await MetadataAgent()
        logger.debug("metadata agent returned")
        logger.debug("releasing database connection pool resources...")
        DbConnectionPool.get().terminate()

    const_init = {
        "rekognition_cfg_file": rekognition_config,
        "pwgo_galleries_host_path": piwigo_galleries_host_path,
        "mysql_cfg_file": database_config,
        "img_crop_path": Path(image_crop_save_path),
        "worker_count": workers,
        "worker_error_limit": worker_error_limit,
        "debug": debug,
        "dry_run": dry_run
    }
    loop = asyncio.get_event_loop()
    loop.set_debug(debug)
    loop.run_until_complete(exec_metadata_agent(const_init))
    logger.info("metadata agent exit stage left")
