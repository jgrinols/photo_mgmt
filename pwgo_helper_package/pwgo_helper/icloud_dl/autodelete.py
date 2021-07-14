"""
Delete any files found in "Recently Deleted"
"""
import os
from .paths import local_download_path

from ..config import Configuration as ProgramConfig

def autodelete_photos(icloud, folder_structure, directory):
    """
    Scans the "Recently Deleted" folder and deletes any matching files
    from the download directory.
    (I.e. If you delete a photo on your phone, it's also deleted on your computer.)
    """
    logger = ProgramConfig.get().create_logger(__name__)
    logger.info("Deleting any files found in 'Recently Deleted'...")

    recently_deleted = icloud.photos.albums["Recently Deleted"]

    removed_items = []

    for media in recently_deleted:
        created_date = media.created
        date_path = folder_structure.format(created_date)
        download_dir = os.path.join(directory, date_path)

        for size in [None, "original", "medium", "thumb"]:
            path = os.path.normpath(
                local_download_path(
                    media, size, download_dir))
            if os.path.exists(path):
                logger.info("Deleting %s!", path)
                os.remove(path)
                removed_items.append(path)

    return removed_items
