"""entry module for the icloud-dl command"""
from __future__ import print_function
import os, sys, time, json, subprocess, itertools, asyncio
from datetime import datetime, timedelta

import click
from pyicloud.exceptions import PyiCloudAPIResponseException
from pid import PidFile
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from ..config import Configuration as ProgramConfig
from .config import Configuration as ICDLConfig
from ..db_connection_pool import DbConnectionPool
from .authentication import authenticate
from . import download
from .string_helpers import truncate_middle
from .autodelete import autodelete_photos
from .paths import local_download_path
from .counter import Counter
from ..asyncio import get_task

logger = ProgramConfig.get().get_logger(__name__)

@click.command("icdownload")
@click.option(
    "-d", "--directory",
    help="Local directory that should be used for download",
    type=click.Path(exists=True),
    required=True
)
@click.option(
    "-u", "--username",
    help="icloud username", required=True
)
@click.option(
    "-p", "--password",
    help="icloud password", required=True, hide_input=True
)
@click.option(
    "--cookie-directory",
    help="Directory to store cookies for authentication "
    "(default: ~/.pyicloud)",
    type=click.Path(file_okay=False), default="~/.pyicloud"
)
@click.option(
    "--size",
    help="Image size to download (default: original)",
    type=click.Choice(["original", "medium", "thumb"]),
    default="original",
)
@click.option(
    "--recent",
    help="Number of recent photos to download (default: download all photos)",
    type=click.IntRange(0),
)
@click.option(
    "--until-found",
    help="Download most recently added photos until we find x number of "
    "previously downloaded consecutive photos (default: download all photos)",
    type=click.IntRange(0),
)
@click.option(
    "-a", "--album",
    help="Album to download (default: All Photos)",
    metavar="<album>",
    default="All Photos",
)
@click.option(
    "-l", "--list-albums",
    help="Lists the available albums",
    is_flag=True,
)
@click.option(
    "--skip-videos",
    help="Don't download any videos (default: Download all photos and videos)",
    is_flag=True,
)
@click.option(
    "--force-size",
    help="Only download the requested size "
    + "(default: download original if size is not available)",
    is_flag=True,
)
@click.option(
    "--convert-heic",
    help="Auto-convert heic images to jpeg (default: retain heic format)",
    is_flag=True,
)
@click.option(
    "--convert-mov",
    help="Auto-convert mov (quicktime) files to mp4 (default: retain mov format)",
    is_flag=True
)
@click.option(
    "--auto-delete",
    help='Scans the "Recently Deleted" folder and deletes any files found in there. '
    + "(If you restore the photo in iCloud, it will be downloaded again.)",
    is_flag=True,
)
@click.option(
    "--only-print-filenames",
    help="Only prints the filenames of all files that will be downloaded "
    "(not including files that are already downloaded.)"
    + "(Does not download or delete any files.)",
    is_flag=True,
)
@click.option(
    "--folder-structure",
    help="Folder structure (default: {:%Y/%m}). "
    "If set to 'none' all photos will just be placed into the download directory",
    default="{:%Y/%m}",
)
@click.option(
    "--auth-msg-db", required=True,
    help="name of database that contains auth message table"
)
@click.option(
    "--auth-msg-tbl", required=True,
    help="name of table that receives auth message"
)
@click.option(
    "--mfa-timeout", type=int, default=30,
    help="number of seconds to wait for mfa code before raising error"
)
@click.option(
    "--auth-phone-digits",
    help="the last two digits of the phone number that will forward the auth code",
    required=True
)
@click.option(
    "--tracking-db", default="icloudpd",
    help="name of the database used for tracking downloads"
)
@click.option(
    "--lookback-days", type=int,
    default=365*5
)
def main(**kwargs):
    with PidFile("pwgo-icdownload"):
        ICDLConfig.initialize(**kwargs)
        loop = asyncio.get_event_loop()
        loop.set_task_factory(get_task)
        loop.run_until_complete(ICDownloader().run())

class ICDownloader:
    """the main driver of the icloud downloader"""
    def __init__(self) -> None:
        self.icdl_cfg = ICDLConfig.get()
        self.prg_cfg = ProgramConfig.get()
        self.prev_ids = []
        self.icloud = None

    async def run(self):
        """initiated execution of the download process"""
        start_time = datetime.now()
        end_time = None
        def _photos_exception_handler(ex, retries):
            """Handles session errors in the PhotoAlbum photos iterator"""
            if "Invalid global session" in str(ex):
                if retries > self.icdl_cfg.max_retries:
                    logger.fatal("iCloud re-authentication failed! Please try again later.")
                    raise ex
                logger.error("Session error, re-authenticating...")
                if retries > 1:
                    # If the first reauthentication attempt failed,
                    # start waiting a few seconds before retrying in case
                    # there are some issues with the Apple servers
                    time.sleep(self.icdl_cfg.wait_seconds * retries)
                self.icloud.authenticate()

        asyncio.current_task().set_name("run-downloader")
        logger.info("authenticating to icloud")
        self.icloud = await authenticate(client_id=os.environ.get("CLIENT_ID"))

        logger.debug("initializing database connection pool...")
        async with DbConnectionPool.initialize(**self.prg_cfg.db_config) as db_pool:
            self.prev_ids = await self._get_previous_ids(db_pool)

            # Default album is "All Photos", so this is the same as
            # calling `icloud.photos.all`.
            # After 6 or 7 runs within 1h Apple blocks the API for some time. In that
            # case exit.
            try:
                photos = self.icloud.photos.albums[self.icdl_cfg.album]
            except PyiCloudAPIResponseException:
                # For later: come up with a nicer message to the user. For now take the
                # exception text
                logger.exception("error accessing icloud photo album %s", self.icdl_cfg.album)
                raise

            photos.exception_handler = _photos_exception_handler
            photos_cnt = len(photos)

            if self.icdl_cfg.list_albums:
                albums_dict = self.icloud.photos.albums
                albums = albums_dict.values()  # pragma: no cover
                album_titles = [str(a) for a in albums]
                print(*album_titles, sep="\n")
                sys.exit(0)

            logger.debug(
                "Looking up all photos%s from album %s...",
                "" if self.icdl_cfg.skip_videos else " and videos",
                self.icdl_cfg.album)

            if self.icdl_cfg.recent is not None:
                logger.debug("filtering photos set to %s most recent",self.icdl_cfg.recent)
                photos_cnt = self.icdl_cfg.recent
                photos = itertools.islice(photos, self.icdl_cfg.recent)

            if self.icdl_cfg.until_found is not None:
                # ensure photos iterator doesn't have a known length
                photos_cnt = "???"
                photos = (p for p in photos)

            directory = os.path.normpath(self.icdl_cfg.directory)

            plural_suffix = "" if photos_cnt == 1 else "s"
            video_suffix = ""
            photos_count_str = "the first" if photos_cnt == 1 else photos_cnt
            if not self.icdl_cfg.skip_videos:
                video_suffix = " or video" if photos_cnt == 1 else " and videos"
            logger.info("Downloading %s %s photo%s%s to %s ...",
                photos_count_str,
                self.icdl_cfg.size,
                plural_suffix,
                video_suffix,
                directory,
            )

            photos_enumerator = photos
            photos_iterator = iter(photos_enumerator)
            consecutive_files_found = Counter(0)

            dl_counter = 0
            while True:
                try:
                    if self._should_break(consecutive_files_found):
                        logger.info("Found %s consecutive previously downloaded photos. Exiting", self.icdl_cfg.until_found)
                        break
                    item = next(photos_iterator)
                    if await self._download_photo(consecutive_files_found, item, db_pool):
                        dl_counter += 1
                except StopIteration:
                    break
            end_time = datetime.now()

            if not self.icdl_cfg.only_print_filenames:
                logger.info("All photos have been downloaded!")

                deletions = []
                if self.icdl_cfg.auto_delete:
                    logger.info("deleting photos marked as deleted in icloud")
                    deletions = autodelete_photos(self.icloud, self.icdl_cfg.folder_structure, directory)
                    logger.info("deleted %s photos", len(deletions))
                    end_time = datetime.now()

                if "SLACK_LOG_API_TOKEN" in os.environ and "SLACK_LOG_CHANNEL" in os.environ:
                    msg_blks = [{ "type": "section", "text": {
                        "type": "mrkdwn", "text": f"*ICloud Download Results for {self.icdl_cfg.username}*"
                    }}]
                    msg_lines = [
                        f"Execution began at {start_time.strftime('%H:%M:%S')} and ended at {datetime.now().strftime('%H:%M:%S')} ({(end_time-start_time).total_seconds()} seconds)",
                        f"Downloaded {dl_counter} media items"
                    ]
                    if deletions:
                        msg_lines.append(f"Deleted {len(deletions)} media items")
                    newline = "\n"
                    msg_attach = [{"color": "#007a5a", "blocks": [{
                            "type": "section", "text": { "type": "mrkdwn", "text": f"{newline.join(msg_lines)}"}
                        }]
                    }]
                    try:
                        client = WebClient(token=os.environ["SLACK_LOG_API_TOKEN"])
                        client.chat_postMessage(channel=os.environ["SLACK_LOG_CHANNEL"], blocks=msg_blks, attachments=msg_attach)
                    except SlackApiError as err:
                        logger.warning("Error posting results to slack: %s", err.response["error"])

    async def _get_previous_ids(self, db_pool):
        logger.debug("getting list of record ids for previously downloaded media items")
        async with db_pool.acquire_dict_cursor(db=self.icdl_cfg.tracking_db) as (cur,_):
            get_ids_sql = """
                SELECT MasterRecordId
                FROM download_log
                WHERE MediaCreatedDateTime >= %s
                ORDER BY MediaCreatedDateTime DESC;
            """
            lookback_date = datetime.today() - timedelta(days=self.icdl_cfg.lookback_days)
            logger.debug("using %s as lookback cutoff date", lookback_date.strftime("%Y-%m-%d"))
            await cur.execute(get_ids_sql, (lookback_date))
            prev_ids = await cur.fetchall()
            if not prev_ids:
                prev_ids = []
        logger.debug("pulled %s previously downloaded ids", len(prev_ids))
        return prev_ids

    async def _download_photo(self, counter, photo, db_pool) -> bool:
        """internal function for actually downloading the photos"""
        logger.info("processing item %s with id %s", photo.filename, photo.id)

        inserted_tracking_id = None
        if self.icdl_cfg.skip_videos and photo.item_type != "image":
            logger.debug("Skipping %s, only downloading photos.", photo.filename)
            return False

        if photo.item_type != "image" and photo.item_type != "movie":
            logger.debug("Skipping %s, only downloading photos and videos. (Item type was: %s)"
                , photo.filename, photo.item_type)
            return False

        try:
            created_date = photo.created.astimezone()
        except (ValueError, OSError):
            logger.warning("Could not convert photo created date to local timezone (%s)", photo.created)
            created_date = photo.created

        try:
            if self.icdl_cfg.folder_structure.lower() == "none":
                date_path = ""
            else:
                date_path = self.icdl_cfg.folder_structure.format(created_date)
        except ValueError:  # pragma: no cover
            # This error only seems to happen in Python 2
            logger.warning("Photo created date was not valid (%s)", photo.created)
            # e.g. ValueError: year=5 is before 1900
            # (https://github.com/icloud-photos-downloader/icloud_photos_downloader/issues/122)
            # Just use the Unix epoch
            created_date = datetime.datetime.fromtimestamp(0)
            date_path = self.icdl_cfg.folder_structure.format(created_date)

        download_dir = os.path.normpath(os.path.join(self.icdl_cfg.directory, date_path))
        logger.debug("setting download directory to %s", download_dir)
        download_size = self.icdl_cfg.size

        try:
            versions = photo.versions
        except KeyError as ex:
            logger.exception("KeyError: %s attribute was not found in the photo fields!", ex)
            with open('icloudpd-photo-error.json', 'w') as outfile:
                # pylint: disable=protected-access
                json.dump({
                    "master_record": photo._master_record,
                    "asset_record": photo._asset_record
                }, outfile)
                # pylint: enable=protected-access
            logger.error("icloudpd has saved the photo record to: ./icloudpd-photo-error.json")
            return False

        if self.icdl_cfg.size not in versions and self.icdl_cfg.size != "original":
            if self.icdl_cfg.force_size:
                filename = photo.filename.encode("utf-8").decode("ascii", "ignore")
                logger.warning("%s size does not exist for %s. Skipping...", self.icdl_cfg.size, filename)
                return False
            download_size = "original"

        download_path = local_download_path(photo, download_size, download_dir)
        logger.debug("setting download path as %s", download_path)

        id_exists = photo.id in [r["MasterRecordId"] for r in self.prev_ids]
        file_exists = os.path.isfile(download_path)
        truncated_path = truncate_middle(download_path, 96)

        if file_exists and not id_exists:
            err_msg = f"Error: file {photo.filename} exists, but is not tracked in db"
            logger.error(err_msg)
            raise RuntimeError(err_msg)
        if id_exists:
            msg = f"skipped: {truncated_path} exists in tracking db"
            logger.info(msg)
            counter.increment()
            logger.debug("have found %s consecutive existing media items", counter.value())
        if not id_exists:
            if counter.value():
                logger.debug("found non-existing media item...resetting existing item counter")
            counter.reset()
            logger.debug("media item qualifies for download")
            if not self.icdl_cfg.only_print_filenames:
                logger.info("Downloading %s", truncated_path)

                try:
                    download_result = download.download_media(
                        self.icloud, photo, download_path, download_size
                    )

                    if download_result:
                        logger.debug("successfully downloaded %s", photo.filename)
                        pre, _ = os.path.splitext(download_path)
                        if self.icdl_cfg.convert_heic and photo.filename.lower().endswith(".heic"):
                            logger.debug("attempting to covert heic file to jpeg")
                            try:
                                new_dl_path = pre + ".JPG"
                                subprocess.run(['convert', download_path, new_dl_path], check=True)
                                os.remove(download_path)
                                download_path = new_dl_path
                                logger.debug("successfully converted file to: %s", download_path)
                            except subprocess.CalledProcessError as err:
                                err_msg = f"CalledProcessError({err.returncode}): error converting {photo.filename} to JPG"
                                logger.exception(err_msg)
                                os.remove(download_path)
                                if os.path.exists(new_dl_path):
                                    os.remove(new_dl_path)
                                raise
                            except OSError:
                                logger.exception("OSError: error deleting file %s", download_path)

                        if self.icdl_cfg.convert_mov and photo.filename.lower().endswith("mov"):
                            logger.debug("attempting to covert mov file to mp4")
                            new_dl_path = pre + ".MP4"
                            cmd = [
                                "ffmpeg", "-i", download_path, "-vcodec", "libx264", "-tune", "film",
                                "-movflags", "use_metadata_tags+faststart", new_dl_path
                            ]
                            try:
                                subprocess.run(cmd, check=True)
                                os.remove(download_path)
                                download_path = new_dl_path
                                logger.debug("successfully converted file to: %s", download_path)
                            except subprocess.CalledProcessError as err:
                                err_msg = f"CalledProcessError({err.returncode}): error converting {photo.filename} to MP4"
                                logger.exception(err_msg)
                                os.remove(download_path)
                                if os.path.exists(new_dl_path):
                                    os.remove(new_dl_path)
                                raise
                            except OSError:
                                logger.exception("OSError: error deleting file %s", download_path)

                        download.set_utime(download_path, created_date)

                        self.prev_ids.append({ "MasterRecordId": photo.id })
                        logger.debug("inserting downloaded file into tracking db")
                        insert_tracking_sql = """
                            INSERT INTO download_log (
                                MasterRecordId,
                                Account,
                                MediaType,
                                OriginalFilename,
                                SavedFilePath,
                                MediaCreatedDateTime)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """

                        async with db_pool.acquire_dict_cursor(db=self.icdl_cfg.tracking_db) as (cur,conn):
                            await cur.execute(insert_tracking_sql, (
                                    photo.id,
                                    self.icdl_cfg.username,
                                    photo.item_type,
                                    photo.filename,
                                    download_path,
                                    created_date
                                ))
                            await conn.commit()
                            await cur.execute("SELECT LAST_INSERT_ID() AS id;")
                            rec = await cur.fetchone()
                            # make sure we don't still have a reference to a previously inserted id
                            inserted_tracking_id = rec["id"]

                except Exception:
                    if os.path.exists(download_path):
                        os.remove(download_path)
                    if inserted_tracking_id:
                        async with db_pool.acquire_dict_cursor(db=self.icdl_cfg.tracking_db) as (cur,conn):
                            await cur.execute("DELETE FROM download_log WHERE id = %s", inserted_tracking_id)
                    logger.exception("error occurred during download process")
                    raise
        return True

    def _should_break(self, counter):
        """Exit if until_found condition is reached"""
        return self.icdl_cfg.until_found is not None and counter.value() >= self.icdl_cfg.until_found
