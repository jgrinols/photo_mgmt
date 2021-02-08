import sys
import logging
import json
import signal
import click
import click_log
import pymysql
import pymysql.cursors
from path import Path
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import WriteRowsEvent, DeleteRowsEvent

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

def main(prog_cfg):
    logger.info(f"initializing virtualfs to [{prog_cfg['destination']}]")
    logger.debug("connecting to db...")
    db = pymysql.connect(
        host = prog_cfg["db"]["host"],
        port = prog_cfg["db"]["port"],
        user = prog_cfg["db"]["user"],
        passwd = prog_cfg["db"]["passwd"],
        db = prog_cfg["db"]["db"],
    )
    cur = db.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT * FROM image_virtual_paths"
    logger.debug(f"executing sql: {sql}...")
    cur.execute(sql)
    logger.debug("finished executing sql")

    if prog_cfg["rebuild"]:
        for item in prog_cfg['destination'].walk():
            item.rmdir_p() if item.isdir() else item.remove()

    for row in cur:
        with prog_cfg["piwigo_root"]:
            src_path = Path(row["physical_path"]).abspath()
            logger.debug(f"source path: {src_path}")
            if not src_path.exists():
                handle_broken_link(src_path, prog_cfg["allow_broken_links"])

        with prog_cfg['destination']:
            virt_path = Path(row["virtual_path"]).abspath()
            logger.debug(f"virt path: {virt_path}")

        virt_path.dirname().makedirs_p()
        if ! virt_path.exists():
            src_path.symlink(virt_path)

    if prog_cfg["monitor"]:
        monitor(prog_cfg)

def monitor(prog_cfg):
    logger.info(f"Monitoring {prog_cfg['db']['db']}.image_virtual_paths for changes")
    global stream
    stream = BinLogStreamReader(
        connection_settings = {
            "host": prog_cfg["db"]["host"],
            "port": prog_cfg["db"]["port"],
            "user": prog_cfg["db"]["user"],
            "passwd": prog_cfg["db"]["passwd"]
        },
        server_id = 1,
        only_schemas = [prog_cfg["db"]["db"]],
        only_tables = ["image_virtual_paths"],
        only_events = [WriteRowsEvent, DeleteRowsEvent],
        blocking = True,
        resume_stream = True
    )

    for event in stream:
        logger.debug(event.dump())

        if type(event) is WriteRowsEvent:
            logger.debug("Received a write event...processing...")
            for row in event.rows:
                with prog_cfg["piwigo_root"]:
                    src_path = Path(row["values"]["physical_path"]).abspath()
                    logger.debug(f"source path: {src_path}")
                    if not src_path.exists():
                        handle_broken_link(src_path, prog_cfg["allow_broken_links"])

                with prog_cfg["destination"]:
                    virt_path = Path(row["values"]["virtual_path"]).abspath()
                    logger.debug(f"virt path: {virt_path}")

                virt_path.dirname().makedirs_p()
                if ! virt_path.exists():
                    src_path.symlink(virt_path)

        if type(event) is DeleteRowsEvent:
            logger.debug("Received a delete event...processing...")
            for row in event.rows:
                with prog_cfg["destination"]:
                    virt_path = Path(row["values"]["virtual_path"]).abspath()
                    logger.debug(f"virt path: {virt_path}")

                removePath(virt_path, prog_cfg)

def removePath(target, prog_cfg):
    logger.debug(f"removing [{target}]")
    if not prog_cfg["remove_empty_dirs"]:
        target.remove_p() if not target.isdir() else target.rmdir()
    else:
        parent_dir = target.parent
        target.remove_p() if not target.isdir() else target.rmdir()
        logger.debug(f"considering [{parent_dir}] for removal...")
        logger.debug(f"is path the root destination path? {parent_dir.samefile(prog_cfg['destination'])}")
        logger.debug(f"number of children in path: {len(parent_dir.listdir())}")
        if (not parent_dir.samefile(prog_cfg["destination"])) and len(parent_dir.listdir()) == 0:
            logger.debug(f"removing empty parent")
            removePath(parent_dir, prog_cfg)

def handleTerm(sig, frame):
    logger.info(f"\nHandling {signal.Signals(sig).name}...")
    stream.close()
    logger.debug(frame)
    sys.exit(1)

signal.signal(signal.SIGTERM, handleTerm)
signal.signal(signal.SIGINT, handleTerm)
signal.signal(signal.SIGQUIT, handleTerm)

def handle_broken_link(src_path, allow):
    msg = f"source path does not exist: [{src_path}]"
    logger.warn(msg) if allow else logger.error(msg)
    if not allow:
        sys.exit(1)

@click.command()
@click.option(
    "-db", "--database-config",
    help="json file specifying database connection parameters for piwigo database",
    type=click.File('r'),
    required=True,
)
@click.option(
    "-r", "--piwigo-root",
    help="path to the root of the piwigo installation",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "-d", "--destination-path",
    help="path to the where the virtual linked file structure will be maintained",
    type=click.Path(exists=True),
    required=True,
)
@click.option(
    "--allow-broken-links", is_flag=True,
    help="don't error when program attempts to create a symlink to a non-existent target. Useful for testing"
)
@click.option(
    "--rebuild", is_flag=True,
    help="remove everything from destination path before initializing."
)
@click.option(
    "--monitor", is_flag=True,
    help="run program as daemon and keep virtual file structure in sync with Piwigo db."
)
@click.option(
    "--remove-empty-dirs", is_flag=True,
    help="remove directories that become empty due to deletions synced from Piwigo db."
)
@click_log.simple_verbosity_option(logger)
def entry(
    database_config,
    piwigo_root,
    destination_path,
    allow_broken_links,
    rebuild,
    monitor,
    remove_empty_dirs
):
    """Command used to manage syncing of piwigo virtual albums with a symlinked file structure."""
    logger.debug(database_config)
    logger.debug(f"piwigo root path: {piwigo_root}")
    cfg = {
        "db": json.load(database_config),
        "piwigo_root": Path(piwigo_root),
        "destination": Path(destination_path),
        "allow_broken_links": allow_broken_links,
        "rebuild": rebuild,
        "monitor": monitor,
        "remove_empty_dirs": remove_empty_dirs
    }

    main(cfg)
