"""Container module for the metadata agent service"""
import logging, signal, json, threading
import asyncio, contextvars

import click, click_log
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent
from path import Path
from py_linq import Enumerable

from photolibutils.pwgo_metadata_agent.event_dispatcher import EventDispatcher
from photolibutils.pwgo_metadata_agent.constants import Constants
from photolibutils.pwgo_metadata_agent.autotagger import AutoTagger
from photolibutils.pwgo_metadata_agent.pwgo_image import PiwigoImage
from photolibutils.pwgo_metadata_agent.utilities import DbConnectionPool

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

async def main():
    """initializes the metadata agent and starts the service"""
    # setup db connection pool for this event loop
    out_conns = contextvars.ContextVar("DB connection pool")
    out_conns.set(await DbConnectionPool.create(Constants.MYSQL_CFG_FILE))

    monitor_started = threading.Event()
    evt_queue = asyncio.Queue()

    # do an initial sync of the face index
    await AutoTagger.sync_face_index()

    with open(Constants.MYSQL_CFG_FILE) as cfg_file:
        db_cfg = json.load(cfg_file)

    def start_event_monitor() -> None:
        """Starts a blocking BinLogStreamReader to monitor for mysql events
        that need to be handled"""

        logger.info("Monitoring %s for metadata changes", Constants.PWGO_DB)
        stream = BinLogStreamReader(
            connection_settings = {
                "host": db_cfg["host"],
                "port": db_cfg["port"],
                "user": db_cfg["user"],
                "passwd": db_cfg["passwd"]
            },
            server_id = 1,
            only_schemas = [Constants.PWGO_DB, Constants.MSG_DB],
            only_tables = Constants.EVENT_TABLES.keys(),
            only_events = [WriteRowsEvent],
            blocking = True,
            resume_stream = True
        )

        def event_monitor(main_loop):
            try:
                monitor_started.set()
                for evt in stream:
                    logger.debug("Processing %s on %s affecting %s rows"
                        , type(evt), evt.table, len(evt.rows)
                    )

                    cmd_rows = [r for r in evt.rows if r["values"]["message_type"] == "COMMAND"]
                    for cmd_msg in [json.loads(r["values"]["message"]) for r in cmd_rows]:
                        if "target" in cmd_msg and cmd_msg["target"] == Constants.STOP_MSG["target"]:
                            if cmd_msg["text"] == Constants.STOP_MSG["text"]:
                                logger.info("Event monitor: Received STOP message. Exiting...")
                                raise SystemExit()

                    fut = asyncio.run_coroutine_threadsafe(evt_queue.put(evt), main_loop)
                    fut.result()

                    logger.debug("Event queued...listening for new events")

            except SystemExit:
                logger.debug("Executing event monitor tear down")
                stream.close()
                Constants.MYSQL_CONN_POOL.get().terminate()
                main_loop.stop()
                return

        loop = asyncio.get_running_loop()
        loop.run_in_executor(None, event_monitor, loop)

    start_event_monitor()

    evt_dispatcher = await EventDispatcher.create(evt_queue, Constants.WORKERS_CNT)
    await process_autotag_backlog(evt_dispatcher)
    await evt_dispatcher


async def process_autotag_backlog(evt_dispatcher):
    """process any existing images that are waiting in the auto tag album
        and initialize the tags for any previously autotagged images"""
    logger.debug("processing any autotag backlog photos")
    async with Constants.MYSQL_CONN_POOL.get().acquire_dict_cursor(db=Constants.PWGO_DB) as (cur, _):
        sql = """
            SELECT i.id, i.file, i.path
            FROM images i
            JOIN image_category ic
            ON ic.image_id = i.id
            WHERE ic.category_id = %s
        """
        await cur.execute(sql, (Constants.AUTO_TAG_ALB))
        tag_imgs = [PiwigoImage(**row) for row in await cur.fetchall()]

    autotag_coros = []
    for img in tag_imgs:
        coro = evt_dispatcher.handle_autotag_image(img)
        autotag_coros.append(coro)
    await asyncio.gather(*autotag_coros)

    async def add_img_tags(img_id, tag_ids):
        async with AutoTagger.create(img_id) as tagger:
            await tagger.add_tags(tag_ids)

    async with Constants.MYSQL_CONN_POOL.get().acquire_dict_cursor(db=Constants.PWGO_DB) as (cur, _):
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

def handle_exit(*_):
    """Handles SIGTERM, SIGINT, and SIGQUIT by inserting a stop message into the messaging db
    The message is received by the event monitor which causes it to exit gracefully. Also raises
    KeyboardInterrupt to allow for cleanup in the main thread"""
    #pylint: disable=no-member
    logger.info("Program exiting. Cleaning up resources.")

    try:
        with open(Constants.MYSQL_CFG_FILE) as cfg_file:
            db_cfg = json.load(cfg_file)
        conn = pymysql.connect(
            host = db_cfg["host"],
            port = db_cfg["port"],
            user = db_cfg["user"],
            passwd = db_cfg["passwd"],
            db = "messaging"
        )
        cur = conn.cursor(pymysql.cursors.DictCursor)
        sql = """
            INSERT INTO pwgo_message (message_type, message)
            VALUES (%s, %s)
        """
        cur.execute(sql, ("COMMAND", json.dumps(Constants.STOP_MSG)))
        conn.commit()

    finally:
        conn.close()

    Constants.MYSQL_CONN_POOL.get().terminate()

signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGQUIT, handle_exit)

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
    "-d", "--debug",
    help="Run program in debug mode",
    is_flag=True
)
@click_log.simple_verbosity_option()
def entry(
    piwigo_galleries_host_path,
    database_config,
    rekognition_config,
    image_crop_save_path,
    workers,
    debug
):
    """Command used to auto generate related tags when a new tag is inserted in database"""
    async def exec_metadata_agent():
        const_init = {
            "rekognition_cfg_file": rekognition_config,
            "pwgo_galleries_host_path": piwigo_galleries_host_path,
            "mysql_cfg_file": database_config,
            "img_crop_path": Path(image_crop_save_path),
            "worker_count": workers,
            "debug": debug
        }

        await Constants.initialize_program_configs(**const_init)
        await main()

    loop = asyncio.get_event_loop()
    loop.set_debug(debug)
    loop.create_task(exec_metadata_agent())
    loop.run_forever()
