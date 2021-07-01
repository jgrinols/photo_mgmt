import sys, time, logging, json
import click
import click_log
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

class PiwigoSynchronizer(object):
    def __init__(self, cfg):
        self.cfg = cfg
        
    def sync(self):
        time_start = time.time()
        response, duration = self.sync_single()
        logger.info(f"Status: {response.status_code}, Duration: {duration}")
        if response.status_code == 504:
            logger.warn("Received status code 504. It appears the sync operation is taking longer than normal.")
        else:
            parsed_response = BeautifulSoup(response.text, "html.parser")
            for item in parsed_response.select("li[class^=update_summary]"):
                print(item.text)
        
    def sync_single(self):
        login_data = {}
        login_data["method"] = "pwg.session.login"
        login_data["username"] = self.cfg["user"]
        login_data["password"] = self.cfg["password"].readline().rstrip()

        session = requests.Session()
        logger.info("Login: ...")
        login_response = session.post(self.cfg["base_url"] + "/ws.php?format=json", data=login_data)
        logger.debug(login_response.text)
        resp_json = json.loads(login_response.text)
        if not "result" in resp_json or not resp_json["result"]:
            logger.fatal(resp_json["message"])
            raise Exception("Login Failed!")
        logger.info(f"Login: OK")
        logger.debug(login_response.text)
        
        sync_data = {}
        sync_data["sync"] = "files"
        sync_data["display_info"] = 1
        sync_data["add_to_caddie"] = 0
        sync_data["privacy_level"] = 8
        if self.cfg["category_id"]:
            sync_data["cat"] = self.cfg["category_id"]
        if not self.cfg["skip_metadata"]:
            sync_data["sync_meta"] = "on"
        sync_data["simulate"] = int(self.cfg["dry_run"])
        sync_data["subcats-included"] = 1
        sync_data["submit"] = "Submit"

        logger.debug(f"executing sync request with config:\n{sync_data}")

        time_start = time.time()
        sync_response = session.post(self.cfg["base_url"] + "/admin.php?page=site_update&site=1", data=sync_data)
        time_end = time.time()
        session.close()
        logger.debug(sync_response.text)
        logger.info("Connection closed")  
        
        return sync_response, (time_end - time_start)


@click.command()
@click.option(
    "-b", "--piwigo-base-url",
    help="url specifiying the web root of the Piwigo installation",
    required=True,
)
@click.option(
    "-u", "--user",
    help="Admin user to use to login and execute synchronization",
    required=True,
)
@click.option(
    "-p", "--password",
    help="File containing admin password (default: stdin)",
    type=click.File('r'),
    default=sys.stdin,
    metavar="<password>",
)
@click.option(
    "--sync-album-id",
    help="Id of an album to synchronize (default: sync all)",
    default=None,
    type=click.INT
)
@click.option(
    "--skip-metadata", is_flag=True,
    help="Don't sync file metadata to Piwigo"
)
@click.option(
    "--dry-run", is_flag=True,
    help="Only show results of a hypothetical synchronization"
)
@click_log.simple_verbosity_option(logger)
def entry(
    piwigo_base_url,
    user,
    password,
    sync_album_id,
    skip_metadata,
    dry_run
):
    logger.info("Begin physical album sync.")

    cfg = {
        "base_url": piwigo_base_url,
        "user": user,
        "password": password,
        "category_id": sync_album_id,
        "skip_metadata": skip_metadata,
        "dry_run": dry_run
    }

    logger.debug(f"Configuration:\n{cfg}")

    PiwigoSynchronizer(cfg).sync()
