"""container for TestPiwigoFS"""
from uuid import uuid4

from pytest import fixture
import fs

from ....pwgo_metadata_agent import utilities

class TestPiwigoFS:
    """Tests for utility functions relating to mapping of piwigo paths to """
    @fixture
    def _mock_host_galleries_fs(self):
        mock_fs_root = '/mass/piwigo/media'
        mock_fs = fs.open_fs('mem://')
        mock_fs.makedirs(mock_fs_root)
        yield mock_fs.opendir(mock_fs_root)
        mock_fs.close()

    @fixture
    def _mock_pwgo_fs(self, _mock_host_galleries_fs):
        mock_pwgo_fs = utilities.get_pwgo_fs(_mock_host_galleries_fs)
        yield mock_pwgo_fs
        mock_pwgo_fs.close()

    def test_simple(self, _mock_host_galleries_fs, _mock_pwgo_fs):
        """basic functional test of utilities.map_pwgo_path"""
        test_dir = "/sub/2021/01"
        test_file = f"{test_dir}/foo.JPG"
        test_file_content = str(uuid4())
        #first create sub directories and test file on mock host fs
        _mock_host_galleries_fs.makedirs(test_dir)
        _mock_host_galleries_fs.writetext(test_file, test_file_content)
        #get a relative path into the virtual pwgo fs like something we would
        #get from the images table in the piwigo db
        mapped_file_path = utilities.map_pwgo_path(f"./galleries{test_file}")
        #verify that we are able to retrieve test file contents through
        #the piwigo virtual fs
        assert _mock_pwgo_fs.readtext(mapped_file_path) == test_file_content
