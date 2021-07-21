"""entry point module for the piwigo sync command"""
import time, json
from urllib.parse import urljoin

import click
import requests
from bs4 import BeautifulSoup

from ..config import Configuration as ProgramConfig
from .config import Configuration as SyncConfig

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
    login_response = session.post(login_url, params=login_params, data=login_data)
    logger.debug(login_response.text)
    resp_json = json.loads(login_response.text)
    if not "result" in resp_json or not resp_json["result"]:
        logger.fatal(resp_json["message"])
        raise RuntimeError("Login Failed!")
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
    sync_url = urljoin(sync_cfg.base_url, sync_cfg.sync_path)
    sync_params = { "page": "site_update", "site": "1" }
    sync_response = session.post(sync_url, params=sync_params, data=sync_data)
    time_end = time.time()
    session.close()
    logger.info("Connection closed")

    return sync_response, (time_end - time_start)

def _compute_missing_hashes(session: requests.Session) -> requests.Response:
    prg_cfg = ProgramConfig.get()
    logger = prg_cfg.get_logger(__name__)
    sync_cfg = SyncConfig.get()
    logger.info("adding any missing photo md5 hashes...")

    compute_md5_params = { "format": "json", "method": "pwg.images.setMd5sum" }
    compute_md5_data = { "block_size": "1" }
    compute_md5_url = urljoin(sync_cfg.base_url, sync_cfg.service_path)
    compute_md5_response = session.post(compute_md5_url, params=compute_md5_params, data=compute_md5_data)

    return compute_md5_response

def sync():
    """execute the sync operation"""
    logger = ProgramConfig.get().get_logger(__name__)
    session = _login()
    response, duration = _sync_single(session)
    logger.info("Status: %s, Duration: %s", response.status_code, duration)
    if response.status_code == 504:
        logger.warning("Received status code 504. It appears the sync operation is taking longer than normal.")
    elif not response.ok:
        logger.error(response.text)
        raise RuntimeError("error while syncing photos")
    else:
        parsed_response = BeautifulSoup(response.text, "html.parser")
        for item in parsed_response.select("li[class^=update_summary]"):
            logger.info(item.text)

        if not ProgramConfig.get().dry_run and SyncConfig.get().add_missing_md5:
            logger.info("calculating any missing photo md5 hashes")
            md5_response = _compute_missing_hashes(session)
            if not md5_response.ok:
                logger.error(md5_response.text)
                raise RuntimeError("error while calculating missing md5 hashes")
            else:
                logger.debug("compute md5 response: %s", md5_response.text)
                md5_response_data = json.loads(md5_response.text)
                if "result" in md5_response and md5_response_data["result"]:
                    logger.info("added hashes for %s photos", md5_response_data["result"]["nb_added"])

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
def sync_entry(**kwargs):
    logger = ProgramConfig.get().get_logger(__name__)
    SyncConfig.initialize(**kwargs)
    logger.info("Begin physical album sync.")
    sync()
