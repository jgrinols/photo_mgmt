import sys, logging, json, time, signal, threading
import click
import click_log
import pymysql
import pymysql.cursors
from threading import Thread, Timer, Semaphore
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

def main(prog_cfg):
    def add_tags():
        logger.info(f"generating any missing tags...")
        logger.debug("connecting to db...")
        try:
            db = pymysql.connect(
                host = prog_cfg["db"]["host"],
                port = prog_cfg["db"]["port"],
                user = prog_cfg["db"]["user"],
                passwd = prog_cfg["db"]["passwd"],
                db = prog_cfg["db"]["db"]
            )
            init_cur = db.cursor(pymysql.cursors.DictCursor)
            sql = """
                INSERT INTO piwigo.image_tag ( image_id, tag_id )
                SELECT DISTINCT it.image_id, imp.implied_tag_id 
                FROM piwigo.image_tag it
                JOIN piwigo.expanded_implicit_tags imp
                ON imp.triggered_by_tag_id = it.tag_id
                LEFT JOIN piwigo.image_tag it2
                ON it2.image_id = it.image_id AND it2.tag_id = imp.implied_tag_id
                WHERE it2.image_id IS NULL;
            """
            logger.debug(f"executing sql: {sql}...")
            init_cur.execute(sql)
            logger.info("finished inserting missing tags")
        
        finally:
            db.close()

    def start_countdown(delay, sem):
        #behold, the hacktastic (mis)use of a semaphore
        sem.release()
        time.sleep(delay)
        sem.acquire()
        if sem._value == 0:
            add_tags()
        else:
            logger.debug("It appears as though another countdown has started...skipping previously scheduled update")

    # do an initial sync up before starting the monitor
    add_tags()
    
    logger.info(f"Monitoring {prog_cfg['db']['db']}.image_tag for changes")
    stream = BinLogStreamReader(
        connection_settings = {
            "host": prog_cfg["db"]["host"],
            "port": prog_cfg["db"]["port"],
            "user": prog_cfg["db"]["user"],
            "passwd": prog_cfg["db"]["passwd"]
        },
        server_id = 1,
        only_schemas = [prog_cfg["db"]["db"]],
        only_tables = prog_cfg["db"]["tables"],
        only_events = [WriteRowsEvent],
        blocking = True,
        resume_stream = True
    )

    try:
        sem = Semaphore(0)
        for event in stream:
            if type(event) is WriteRowsEvent:
                logger.debug("Received a write event...processing...")
                logger.debug(event.dump)
                delay = prog_cfg["delay"]
                logger.info(f"Scheduling update for +{delay}s")
                #using daemon countdown threads so that they will die if the main thread dies
                Thread(target=start_countdown, daemon=True, args=[delay, sem]).start()

    except KeyboardInterrupt:
        logger.debug("catching keyboard interrupt in event monitor")
        stream.close()


def handleTerm(sig, frame):
   logger.info(f"\nHandling {signal.Signals(sig).name}...")
   raise KeyboardInterrupt

signal.signal(signal.SIGTERM, handleTerm)
signal.signal(signal.SIGINT, handleTerm)
signal.signal(signal.SIGQUIT, handleTerm)

@click.command()
@click.option(
    "-db", "--database-config",
    help="json file specifying database connection parameters for piwigo database",
    type=click.File('r'),
    required=True,
)
@click.option(
    "-w", "--wait-secs",
    help="number of seconds to wait before inserting autotags (used to batch sql queries)",
    type=int,
    default=30,
)
@click_log.simple_verbosity_option(logger)
def entry(
    database_config,
    wait_secs
):
    """Command used to auto generate related tags when a new tag is inserted in database"""
    logger.debug(database_config)
    db = json.load(database_config)

    main({ "db": db, "delay": wait_secs })
