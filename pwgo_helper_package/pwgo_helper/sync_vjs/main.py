"""entry point module for the piwigo-videojs sync command"""
import time, json, asyncio
from urllib.parse import urljoin

import click
import requests
from bs4 import BeautifulSoup
from pid import PidFile

from ..config import Configuration as ProgramConfig
from .config import Configuration as SyncConfig
from ..asyncio import get_task

def _login() -> requests.Session:
    prg_cfg = ProgramConfig.get()
    logger = prg_cfg.get_logger(__name__)
    sync_cfg = SyncConfig.get()

    login_data = {
        "method": "pwg.session.login",
        "username": sync_cfg.user,
        "password": sync_cfg.password
    }

    session = requests.Session()
    logger.info("Login: ...")
    login_params = { "format": "json" }
    login_url = urljoin(prg_cfg.base_url, sync_cfg.service_path)
    try:
        login_response = session.post(login_url, params=login_params, data=login_data)
        login_response.raise_for_status()
        resp_json = json.loads(login_response.text)
        if not "result" in resp_json or not resp_json["result"]:
            logger.fatal(resp_json["message"])
            raise RuntimeError("Login Failed!")
    except (requests.exceptions.HTTPError, RuntimeError):
        logger.fatal("Login error")
        raise
    logger.info("Login: OK")
    logger.debug(login_response.text)

    return session

def _sync_single(session: requests.Session):
    prg_cfg = ProgramConfig.get()
    logger = prg_cfg.get_logger(__name__)
    sync_cfg = SyncConfig.get()

    sync_data = {
        "metadata": "1" if sync_cfg.sync_metadata else "0",
        "poster": "1" if sync_cfg.create_thumbnail else "0",
        "postersec": "4",
        "output": "jpg",
        "thumb": "0",
        "simulate": "1" if prg_cfg.dry_run else "0",
        "subcats_included": "1",
        "submit": "Submit"
    }
    if sync_cfg.sync_album_id:
        sync_data["cat_id"] = sync_cfg.sync_album_id

    logger.debug("executing sync request with config:\n%s", sync_data)

    time_start = time.time()
    sync_url = urljoin(prg_cfg.base_url, sync_cfg.admin_path)
    sync_params = { "page": "plugin", "section": "piwigo-videojs/admin/admin.php", "tab": "sync" }
    sync_response = session.post(sync_url, params=sync_params, data=sync_data)
    sync_response.raise_for_status()
    if sync_response.status_code == 504:
        logger.warning("Received status code 504. It appears the sync operation is taking longer than normal.")
    elif not sync_response.ok:
        logger.error(sync_response.text)
        raise RuntimeError("error while syncing videos")
    time_end = time.time()

    return sync_response, (time_end - time_start)

async def sync():
    """execute the sync operation"""
    prg_cfg = ProgramConfig.get()
    logger = prg_cfg.get_logger(__name__)
    session = _login()
    try:
        response, duration = _sync_single(session)
        logger.info("Status: %s, Duration: %s", response.status_code, duration)
        parsed_response = BeautifulSoup(response.text, "html.parser")
        for item in parsed_response.select("li[class^=update_summary]"):
            logger.info(item.text)
    finally:
        session.close()

@click.command("sync-vjs")
@click.option(
    "-u", "--user",
    help="Admin user to use to login and execute synchronization",
    required=True,
)
@click.option(
    "-p", "--password", help="User password",
    required=True, hide_input=True
)
@click.option(
    "--sync-album-id", help="Id of an album to synchronize (default: sync all)",
    default=None, type=click.INT
)
@click.option(
    "--sync-metadata/--no-sync-metadata", is_flag=True, default=True,
    help="Sync file metadata to the Piwigo database"
)
@click.option(
    "--create-thumbnail/--no-create-thumbnail", is_flag=True, default=True,
    help="Generate a thumbnail image"
)
@click.option(
    "--process-existing/--no-process-existing", is_flag=True, default=False,
    help="Peform sync for all video files--not just new files"
)
def sync_entry(**kwargs):
    with PidFile("pwgo-sync-vjs"):
        logger = ProgramConfig.get().get_logger(__name__)
        SyncConfig.initialize(**kwargs)
        logger.info("Begin videojs sync.")
        loop = asyncio.get_event_loop()
        loop.set_task_factory(get_task)
        loop.run_until_complete(sync())
