"""container module for TestFileMetadataWriter"""
from unittest.mock import MagicMock, patch

from ...pwgo_metadata_agent.file_metadata_writer import FileMetadataWriter
from ...pwgo_metadata_agent.pwgo_image import PiwigoImage, PiwigoImageMetadata

class TestFileMetadataWriter:
    """tests for the FileMetadataWriter class"""
    @patch("photolibutils.pwgo_metadata_agent.file_metadata_writer.ImageData")
    def test_basic(self, mck_img_data, test_db):
        """basic functional test"""
        img = MagicMock(spec=PiwigoImage)
        mck_mdata = {
            "name": "test_img.jpg",
            "comment": "this is a test image!",
            "author": "john doe",
            "date_creation": "2020-01-01 00:00:00",
            "tags": ["tag1","tag2"]
        }
        img.metadata = PiwigoImageMetadata(mck_mdata)

        with FileMetadataWriter(img) as writer:
            writer.write()

        mck_img_data.return_value.modify_iptc.assert_called_once_with({
            "Iptc.Application2.ObjectName": mck_mdata["name"],
            "Iptc.Application2.Caption": mck_mdata["comment"],
            "Iptc.Application2.Byline": mck_mdata["author"],
            "Iptc.Application2.Keywords": mck_mdata["tags"]
        })
