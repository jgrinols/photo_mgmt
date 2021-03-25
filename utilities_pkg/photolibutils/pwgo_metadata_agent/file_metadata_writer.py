"""Container module for FileMetadataWriter"""
from contextlib import contextmanager

from pyexiv2 import ImageData

from photolibutils.pwgo_metadata_agent.pwgo_image import PiwigoImage

class FileMetadataWriter():
    """Synchronizes Piwigo image metadata from the database into exif/iptc fields in the physical file"""
    def __init__(self, img: PiwigoImage):
        self.image = img
        self._img_data = None

    @contextmanager
    def get_file_image_data(self) -> ImageData:
        """Opens the image file and returns ImageData context manager"""
        try:
            with self.image.open_file(mode='r+') as img_file:
                img_data = ImageData(img_file.read())
                img_file.seek(0)
                yield (img_data, img_file)

        finally:
            if img_data:
                img_data.close()

    def write(self) -> None:
        """Writes image metadata from Piwigo database to file"""
        with self.get_file_image_data() as file_img_data:
            file_img_data[0].modify_iptc(self.image.metadata.get_iptc_dict())
            file_img_data[1].write(file_img_data[0].get_bytes())
