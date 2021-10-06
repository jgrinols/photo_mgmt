"""Handles file downloads with retries and error handling"""
import os, socket, time
from tzlocal import get_localzone
from requests.exceptions import ConnectionError  # pylint: disable=redefined-builtin
from pyicloud.exceptions import PyiCloudAPIResponseException

from .config import Configuration as ICDLConfig
from ..config import Configuration as ProgramConfig

def update_mtime(photo, download_path):
    """Set the modification time of the downloaded file to the photo creation date"""
    if photo.created:
        created_date = None
        try:
            created_date = photo.created.astimezone(
                get_localzone())
        except (ValueError, OSError):
            # We already show the timezone conversion error in base.py,
            # when generating the download directory.
            # So just return silently without touching the mtime.
            return
        set_utime(download_path, created_date)

def set_utime(download_path, created_date):
    """Set date & time of the file"""
    ctime = time.mktime(created_date.timetuple())
    os.utime(download_path, (ctime, ctime))

def download_media(icloud, photo, download_path, size):
    """Download the photo to path, with retries and error handling"""
    icdl_cfg = ICDLConfig.get()
    logger = ProgramConfig.get().get_logger(__name__)
    # get back the directory for the file to be downloaded and create it if not there already
    download_dir = os.path.dirname(download_path)

    if not os.path.exists(download_dir):
        try:
            os.makedirs(download_dir)
        except OSError:  # pragma: no cover
            pass         # pragma: no cover

    for retries in range(icdl_cfg.max_retries):
        try:
            photo_response = photo.download(size)
            if photo_response:
                temp_download_path = download_path + ".part"
                with open(temp_download_path, "wb") as file_obj:
                    for chunk in photo_response.iter_content(chunk_size=1024):
                        if chunk:
                            file_obj.write(chunk)
                os.rename(temp_download_path, download_path)
                update_mtime(photo, download_path)
                return True

            logger.error("Could not find URL to download %s for size %s!", photo.filename, icdl_cfg.size)
            break

        except (ConnectionError, socket.timeout, PyiCloudAPIResponseException) as ex:
            if "Invalid global session" in str(ex):
                logger.error("Session error, re-authenticating...")
                if retries > 0:
                    # If the first reauthentication attempt failed,
                    # start waiting a few seconds before retrying in case
                    # there are some issues with the Apple servers
                    time.sleep(icdl_cfg.wait_seconds)

                icloud.authenticate()
            else:
                # you end up here when p.e. throttleing by Apple happens
                wait_time = (retries + 1) * icdl_cfg.wait_seconds
                logger.error("Error downloading %s, retrying after %s seconds...", photo.filename, wait_time)
                time.sleep(wait_time)

        except IOError:
            logger.error(
                """IOError while writing file to %s! "
                "You might have run out of disk space, or the file "
                "might be too large for your OS. "
                "Skipping this file...""", download_path
            )
            break
    else:
        logger.error("Could not download %s! Please try again later.", photo.filename)

    return False
