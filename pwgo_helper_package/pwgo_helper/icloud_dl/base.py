"""entry module for the icloud-dl command"""
from __future__ import print_function
import os, sys, time, datetime, json, subprocess, itertools

import click
import pymysql
import pymysql.cursors
from pyicloud_ipd.exceptions import PyiCloudAPIResponseError

from ..config import Configuration as ProgramConfig
from .config import Configuration as ICDLConfig
from .authentication import authenticate
from . import download
from .string_helpers import truncate_middle
from .autodelete import autodelete_photos
from .paths import local_download_path
from .counter import Counter

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
    help="Auto-convert heic images to jpeg "
    + "(default: retain heic format)",
    is_flag=True,
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
    "--auth-msg-db", default="Syslog",
    help="name of database that contains auth message table"
)
@click.option(
    "--auth-msg-tbl", default="SystemEvents",
    help="name of table that receives auth message"
)
@click.option(
    "--auth-msg-tag", default="msg_queue",
    help="tag that is applied to auth messages"
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
def main(**kwargs):
    """Download all iCloud photos to a local directory"""
    prg_cfg = ProgramConfig.get()
    logger = prg_cfg.get_logger(__name__)
    ICDLConfig.initialize(**kwargs)
    icdl_cfg = ICDLConfig.get()

    icloud = authenticate(client_id=os.environ.get("CLIENT_ID"))

    # Default album is "All Photos", so this is the same as
    # calling `icloud.photos.all`.
    # After 6 or 7 runs within 1h Apple blocks the API for some time. In that
    # case exit.
    try:
        photos = icloud.photos.albums[icdl_cfg.album]
    except PyiCloudAPIResponseError:
        # For later: come up with a nicer message to the user. For now take the
        # exception text
        logger.exception("error accessing icloud photo album %s", icdl_cfg.album)
        raise

    if icdl_cfg.list_albums:
        albums_dict = icloud.photos.albums
        albums = albums_dict.values()  # pragma: no cover
        album_titles = [str(a) for a in albums]
        print(*album_titles, sep="\n")
        sys.exit(0)

    directory = os.path.normpath(icdl_cfg.directory)

    con = pymysql.connect(
        host = prg_cfg.db_config["host"],
        user = prg_cfg.db_config["user"],
        passwd = prg_cfg.db_config["passwd"],
        db = icdl_cfg.tracking_db,
        cursorclass = pymysql.cursors.DictCursor)
    limit_str = ""
    if not icdl_cfg.recent is None:
        limit_str = "LIMIT {0}".format(icdl_cfg.recent * 2)
    get_ids_sql = f"""
        SELECT MasterRecordId
        FROM download_log
        WHERE Account = %s
        ORDER BY MediaCreatedDateTime DESC
        {limit_str};
    """

    try:
        with con.cursor() as cur:
            cur.execute(get_ids_sql, icdl_cfg.username)
            prev_ids = cur.fetchall()

    finally:
        con.close()

    logger.debug(
        "Looking up all photos%s from album %s...",
        "" if icdl_cfg.skip_videos else " and videos",
        icdl_cfg.album)

    def photos_exception_handler(ex, retries):
        """Handles session errors in the PhotoAlbum photos iterator"""
        if "Invalid global session" in str(ex):
            if retries > icdl_cfg.max_retries:
                logger.fatal("iCloud re-authentication failed! Please try again later.")
                raise ex
            logger.error("Session error, re-authenticating...")
            if retries > 1:
                # If the first reauthentication attempt failed,
                # start waiting a few seconds before retrying in case
                # there are some issues with the Apple servers
                time.sleep(icdl_cfg.wait_seconds * retries)
            icloud.authenticate()

    photos.exception_handler = photos_exception_handler

    photos_count = len(photos)

    # Optional: Only download the x most recent photos.
    if icdl_cfg.recent is not None:
        photos_count = icdl_cfg.recent
        photos = itertools.islice(photos, icdl_cfg.recent)

    if icdl_cfg.until_found is not None:
        photos_count = "???"
        # ensure photos iterator doesn't have a known length
        photos = (p for p in photos)

    plural_suffix = "" if photos_count == 1 else "s"
    video_suffix = ""
    photos_count_str = "the first" if photos_count == 1 else photos_count
    if not icdl_cfg.skip_videos:
        video_suffix = " or video" if photos_count == 1 else " and videos"
    logger.info("Downloading %s %s photo%s%s to %s ...",
        photos_count_str,
        icdl_cfg.size,
        plural_suffix,
        video_suffix,
        directory,
    )

    photos_enumerator = photos

    def download_photo(counter, photo):
        """internal function for actually downloading the photos"""
        item_result = {
            "item": photo.filename,
            "id": photo.id,
            "downloaded": False,
            "disposition_items": []
        }
        if icdl_cfg.skip_videos and photo.item_type != "image":
            logger.debug("Skipping %s, only downloading photos.", photo.filename)
            item_result["disposition_items"].append("skipped: not photo")
            return item_result

        if photo.item_type != "image" and photo.item_type != "movie":
            logger.debug("Skipping %s, only downloading photos and videos. (Item type was: %s)"
                , photo.filename, photo.item_type)
            item_result["disposition_items"].append("skipped: not photo or video")
            return item_result
        try:
            created_date = photo.created.astimezone()
        except (ValueError, OSError):
            logger.warning("Could not convert photo created date to local timezone (%s)", photo.created)
            created_date = photo.created

        try:
            if icdl_cfg.folder_structure.lower() == "none":
                date_path = ""
            else:
                date_path = icdl_cfg.folder_structure.format(created_date)
        except ValueError:  # pragma: no cover
            # This error only seems to happen in Python 2
            logger.warning("Photo created date was not valid (%s)", photo.created)
            # e.g. ValueError: year=5 is before 1900
            # (https://github.com/icloud-photos-downloader/icloud_photos_downloader/issues/122)
            # Just use the Unix epoch
            created_date = datetime.datetime.fromtimestamp(0)
            date_path = icdl_cfg.folder_structure.format(created_date)

        download_dir = os.path.normpath(os.path.join(directory, date_path))
        download_size = icdl_cfg.size

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
            item_result["disposition_items"] \
                .append(f"KeyError: {ex} attribute was not found in the photo fields!")
            return item_result

        if icdl_cfg.size not in versions and icdl_cfg.size != "original":
            if icdl_cfg.force_size:
                filename = photo.filename.encode("utf-8").decode("ascii", "ignore")
                logger.warning("%s size does not exist for %s. Skipping...", icdl_cfg.size, filename)
                item_result["disposition_items"] \
                    .append(f"skipped: {icdl_cfg.size} size does not exist for {filename}.")
                return item_result
            download_size = "original"

        download_path = local_download_path(photo, download_size, download_dir)

        id_exists = photo.id in [r["MasterRecordId"] for r in prev_ids]
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
            item_result["disposition_items"] \
                .append(msg)
        if not id_exists:
            counter.reset()
            if icdl_cfg.only_print_filenames:
                logger.info(download_path)
                item_result["disposition_items"] \
                    .append("simulated: not executing any downloads")
            else:
                logger.info("Downloading %s", truncated_path)

                download_result = download.download_media(
                    icloud, photo, download_path, download_size
                )
                item_result["downloaded"] = download_result

                if download_result:
                    if icdl_cfg.convert_heic and photo.filename.lower().endswith(".heic"):
                        try:
                            pre, _ = os.path.splitext(download_path)
                            new_dl_path = pre + ".JPG"
                            subprocess.run(['convert', download_path, new_dl_path], check=True)
                            os.remove(download_path)
                            download_path = new_dl_path
                            item_result["disposition_items"] \
                                .append(f"converted: {photo.filename} to JPG")
                        except subprocess.CalledProcessError as err:
                            err_msg = f"CalledProcessError({err.returncode}): error converting {photo.filename} to JPG"
                            logger.exception(err_msg)
                            os.remove(download_path)
                            raise
                        except OSError as err:
                            logger.exception("OSError: error deleting file %s", download_path)
                            item_result["disposition_items"] \
                                .append(f"warning: failed to delete {download_path} after conversion")

                    download.set_utime(download_path, created_date)
                    item_result["disposition_items"] \
                        .append(f"set_utime: utime set to {created_date}")

                    prev_ids.append({ "MasterRecordId": photo.id })
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

                    try:
                        with con.cursor() as cur:
                            cur.execute(insert_tracking_sql, (
                                photo.id,
                                icdl_cfg.username,
                                photo.item_type,
                                photo.filename,
                                download_path,
                                created_date
                            ))
                            con.commit()
                            item_result["disposition_items"] \
                                .append("tracked: inserted record into tracking db.")
                    except pymysql.Error as err:
                        logger.exception("Error inserting tracking record")
                        con.close()
                        os.remove(download_path)
                        raise

        return item_result

    consecutive_files_found = Counter(0)

    def should_break(counter):
        """Exit if until_found condition is reached"""
        return icdl_cfg.until_found is not None and counter.value() >= icdl_cfg.until_found

    download_results = []
    photos_iterator = iter(photos_enumerator)
    while True:
        try:
            if should_break(consecutive_files_found):
                logger.info("Found %s consecutive previously downloaded photos. Exiting", icdl_cfg.until_found)
                break
            item = next(photos_iterator)
            download_results.append(download_photo(consecutive_files_found, item))
        except StopIteration:
            break

    deletions = []
    if not icdl_cfg.only_print_filenames:
        con.close()
        logger.info("All photos have been downloaded!")

        if icdl_cfg.auto_delete:
            deletions = autodelete_photos(icloud, icdl_cfg.folder_structure, directory)

    logger.debug("""download processing results:
        %s
    """, json.dumps(download_results))
    logger.debug("""deletions:
        %s""", deletions)

    downloads = list(filter(lambda item: item["downloaded"], download_results))
    exec_summary = { "downloads": len(downloads), "deletions": len(deletions) }

    print(json.dumps(exec_summary))
