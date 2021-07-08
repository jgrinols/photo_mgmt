"""Container module for FileMetadataWriter"""
from contextlib import ExitStack

from pyexiv2 import ImageData

from .pwgo_image import PiwigoImage
from ..config import Configuration as ProgramConfig

class FileMetadataWriter():
    """Synchronizes Piwigo image metadata from the database into exif/iptc fields in the physical file"""
    def __init__(self, img: PiwigoImage):
        self._logger = ProgramConfig.get().create_logger(__name__)
        self.image = img
        self._exit_stack = None
        self._img_data = None
        self._img_file = None

    def __enter__(self):
        self._exit_stack = ExitStack()
        self._exit_stack.__enter__()
        self._img_file = self._exit_stack.enter_context(self.image.open_file(mode='r+'))
        self._img_data = self._exit_stack.enter_context(ImageData(self._img_file.read()))
        self._img_file.seek(0)
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._exit_stack.__exit__(exc_type, exc_value, exc_traceback)

    def write(self) -> None:
        """Writes image metadata from Piwigo database to file
        usage:
        with FileMetadataWriter(pwgo_img) as writer:
            writer.write()"""
        self._logger.debug("writing metadata to file")
        self._img_data.modify_iptc(self.image.metadata.get_iptc_dict())
        # pylint: disable=no-member
        self._img_file.write(self._img_data.get_bytes())
