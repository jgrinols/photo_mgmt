"""container module for TestFileMetadataWriter"""
from io import BufferedIOBase
from unittest.mock import MagicMock, patch

from fs.mountfs import MountFS
from pyexiv2 import ImageData

from ...agent.file_metadata_writer import FileMetadataWriter
from ...agent.pwgo_image import PiwigoImage, PiwigoImageMetadata

class TestFileMetadataWriter:
    """tests for the FileMetadataWriter class"""

    @patch("pwgo_helper.agent.file_metadata_writer.ImageData")
    @patch("pwgo_helper.agent.utilities.get_pwgo_fs")
    def test_basic(self, mck_get_fs, mck_img_data):
        """basic functional test"""
        mck_fs = MagicMock(spec=MountFS)
        mck_fs.openbin.return_value = MagicMock(spec=BufferedIOBase)
        mck_get_fs.return_value = mck_fs
        mck_img_data_inst = MagicMock(spec=ImageData, name="foo")
        mck_img_data.return_value.__enter__.return_value = mck_img_data_inst
        img_params = {
            "id": 1,
            "file": "test_file.JPG",
            "path": "/test_file.JPG",
            "metadata": PiwigoImageMetadata({
                "name": "test_file.JPG",
                "comment": "this is a test image!",
                "author": "john doe",
                "date_creation": "2020-01-01 00:00:00",
                "tags": ["tag1","tag2"]
            })
        }
        img = PiwigoImage(**img_params)

        with FileMetadataWriter(img) as writer:
            writer.write()

        mck_img_data_inst.modify_iptc.assert_called_once_with({
            "Iptc.Application2.ObjectName": img.metadata.name,
            "Iptc.Application2.Caption": img.metadata.comment,
            "Iptc.Application2.Byline": img.metadata.author,
            "Iptc.Application2.Keywords": img.metadata.tags
        })
