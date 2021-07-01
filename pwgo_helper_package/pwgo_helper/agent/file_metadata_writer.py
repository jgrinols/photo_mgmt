"""Container module for FileMetadataWriter"""
from pyexiv2 import ImageData

from .pwgo_image import PiwigoImage

class FileMetadataWriter():
    """Synchronizes Piwigo image metadata from the database into exif/iptc fields in the physical file"""
    def __init__(self, img: PiwigoImage):
        self.image = img
        self._img_data = None
        self._img_file = None

    def __enter__(self):
        self._img_file = self.image.open_file(mode='r+')
        self._img_data = ImageData(self._img_file.read())
        self._img_file.seek(0)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._img_data.close()
        self._img_file.close()

    def write(self) -> None:
        """Writes image metadata from Piwigo database to file
        usage:
        with FileMetadataWriter(pwgo_img) as writer:
            writer.write()"""
        self._img_data.modify_iptc(self.image.metadata.get_iptc_dict())
        self._img_file.write(self._img_data.get_bytes())
