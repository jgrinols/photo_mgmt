"""entry point module for the piwigo sync command"""
import time, json, asyncio
from urllib.parse import urljoin

import click
import requests
from bs4 import BeautifulSoup
from pid import PidFile

from ..config import Configuration as ProgramConfig
from .config import Configuration as SyncConfig
from ..asyncio import get_task
from ..db_connection_pool import DbConnectionPool

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
    login_url = urljoin(sync_cfg.base_url, sync_cfg.service_path)
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
        "sync": "dirs" if sync_cfg.directories_only else "files",
        "display_info": "1",
        "add_to_caddie": "1" if sync_cfg.add_to_caddie else "0",
        "privacy_level": sync_cfg.privacy_level,
        "simulate": "1" if prg_cfg.dry_run else "0",
        "subcats-included": "1",
        "submit": "Submit"
    }
    if sync_cfg.sync_album_id:
        sync_data["cat"] = sync_cfg.sync_album_id
    if not sync_cfg.skip_metadata:
        sync_data["sync_meta"] = "on"

    logger.debug("executing sync request with config:\n%s", sync_data)

    time_start = time.time()
    sync_url = urljoin(sync_cfg.base_url, sync_cfg.admin_path)
    sync_params = { "page": "site_update", "site": "1" }
    sync_response = session.post(sync_url, params=sync_params, data=sync_data)
    sync_response.raise_for_status()
    if sync_response.status_code == 504:
        logger.warning("Received status code 504. It appears the sync operation is taking longer than normal.")
    elif not sync_response.ok:
        logger.error(sync_response.text)
        raise RuntimeError("error while syncing photos")
    time_end = time.time()

    return sync_response, (time_end - time_start)

def _get_pwg_token(session: requests.Session) -> str:
    prg_cfg = ProgramConfig.get()
    logger = prg_cfg.get_logger(__name__)
    sync_cfg = SyncConfig.get()

    logger.info("retrieving pwg token")
    token_url = urljoin(sync_cfg.base_url, sync_cfg.admin_path)
    token_params = { "page": "batch_manager" }
    token_response = session.get(token_url, params=token_params)

    parsed_response = BeautifulSoup(token_response.text, "html.parser")
    token_input = parsed_response.find("input", { "name": "pwg_token" })
    if token_input:
        return token_input.get("value")
    else:
        raise RuntimeError("error retrieving pwg token")

def _compute_missing_hashes(session: requests.Session, token: str) -> int:
    prg_cfg = ProgramConfig.get()
    logger = prg_cfg.get_logger(__name__)
    sync_cfg = SyncConfig.get()
    logger.info("adding missing photo md5 hashes...")
    hashes_added = 0

    compute_md5_params = { "format": "json", "method": "pwg.images.setMd5sum" }
    compute_md5_data = {
        "block_size": str(sync_cfg.md5_block_size),
        "pwg_token": token
    }
    compute_md5_url = urljoin(sync_cfg.base_url, sync_cfg.service_path)
    try:
        compute_md5_response = session.post(compute_md5_url, params=compute_md5_params, data=compute_md5_data)
        compute_md5_response.raise_for_status()

        if not compute_md5_response.ok:
            logger.error(compute_md5_response.text)
            raise RuntimeError("error while calculating missing md5 hashes")
    except (requests.exceptions.HTTPError, RuntimeError):
        logger.error("Calculate MD5 request failed")
        raise

    logger.debug("compute md5 response: %s", compute_md5_response.text)
    md5_response_data = json.loads(compute_md5_response.text)
    if "result" in md5_response_data and md5_response_data["result"]:
        logger.info("added hashes for %s photos", md5_response_data["result"]["nb_added"])
        logger.info("%s remaining photos with no hash", md5_response_data["result"]["nb_no_md5sum"])
        hashes_added += md5_response_data["result"]["nb_added"]
        if int(md5_response_data["result"]["nb_no_md5sum"]):
            hashes_added += _compute_missing_hashes(session, token)

    return hashes_added

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

        if not prg_cfg.dry_run and SyncConfig.get().add_missing_md5:
            async with DbConnectionPool.initialize(**prg_cfg.db_config) as db_pool:
                async with db_pool.acquire_dict_cursor(db=prg_cfg.pwgo_db_name) as (cur,_):
                    sql = """
                        SELECT COUNT(*) AS missing_cnt
                        FROM images
                        WHERE md5sum IS NULL
                    """
                    logger.info("checking if there are any images with missing md5 hashes")
                    await cur.execute(sql)
                    result = await cur.fetchone()

            if result["missing_cnt"]:
                token = _get_pwg_token(session)
                hashes_added = _compute_missing_hashes(session, token)
                logger.info("added %s missing hashes", hashes_added)
    finally:
        session.close()

@click.command("sync")
@click.option(
    "-b", "--base-url",
    help="url specifiying the web root of the Piwigo installation", required=True,
)
@click.option(
    "-u", "--user",
    help="Admin user to use to login and execute synchronization",
    required=True,
)
@click.option(
    "-p", "--password", help="admin password",
    required=True, hide_input=True
)
@click.option(
    "--sync-album-id", help="Id of an album to synchronize (default: sync all)",
    default=None, type=click.INT
)
@click.option(
    "--skip-metadata", is_flag=True,
    help="Don't sync file metadata to Piwigo"
)
@click.option(
    "--directories-only", is_flag=True,
    help="should the sync be limited to only adding new directory structure"
)
@click.option(
    "--add-to-caddie", is_flag=True,
    help="should synced files be added to the caddie"
)
@click.option(
    "--add-missing-md5", is_flag=True,
    help="should md5 hashes be computed for any photos where they're missing"
)
@click.option(
    "--file-access-level", type=click.Choice(["All", "Contacts", "Friends", "Family", "Admins"]),
    help="who should be able to see the imported media",
    required=True, default="Admins"
)
@click.option(
    "--md5-block-size", type=click.INT, help="number of hashes to compute per request",
    required=True, default=1
)
def sync_entry(**kwargs):
    with PidFile("pwgo-sync"):
        logger = ProgramConfig.get().get_logger(__name__)
        SyncConfig.initialize(**kwargs)
        logger.info("Begin physical album sync.")
        loop = asyncio.get_event_loop()
        loop.set_task_factory(get_task)
        loop.run_until_complete(sync())
