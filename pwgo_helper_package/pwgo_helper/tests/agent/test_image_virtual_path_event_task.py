"""container module for TestImageVirtualPathEventTask"""
import tempfile, os.path, json
from unittest.mock import patch

from path import Path
import pytest

from ...agent.config import Configuration as AgentConfig
from ...config import Configuration as ProgramConfig
from ...agent.image_virtual_path_event_task import ImageVirtualPathEventTask
from .conftest import TestDbResult

class TestImageVirtualPathEventTask:
    """Tests for the TestImageVirtualPathEventTask class"""
    @patch.object(AgentConfig, "get")
    def test_remove_path_recursive(self, mck_get_acfg, mocker):
        """tests the basic functioning of the _remove_path class method"""
        mck_get_acfg.return_value = AgentConfig()
        mck_get_acfg.return_value.virtualfs_remove_empty_dirs = True
        spy_remove = mocker.spy(ImageVirtualPathEventTask, "_remove_path")
        with tempfile.TemporaryDirectory() as tmp_dir:
            mck_get_acfg.return_value.virtualfs_root = tmp_dir
            tmp_dir_path = Path(tmp_dir)
            lvl1_path = tmp_dir_path.joinpath("lvl1")
            lvl2_path = lvl1_path.joinpath("lvl2")
            lvl2_path.makedirs_p()

            tmp_file = tempfile.NamedTemporaryFile(dir=lvl2_path)
            tmp_file_path = Path(tmp_file.name)
            ImageVirtualPathEventTask._remove_path(tmp_file_path)

            assert spy_remove.call_count == 3
            assert not tmp_file_path.exists()
            assert not lvl2_path.exists()
            assert not lvl1_path.exists()
            assert tmp_dir_path.exists()

    @patch.object(AgentConfig, "get")
    def test_remove_path_nonrecursive(self, mck_get_acfg, mocker):
        """tests the basic functioning of the _remove_path class method"""
        mck_get_acfg.return_value = AgentConfig()
        mck_get_acfg.return_value.virtualfs_remove_empty_dirs = False
        spy_remove = mocker.spy(ImageVirtualPathEventTask, "_remove_path")
        with tempfile.TemporaryDirectory() as tmp_dir:
            mck_get_acfg.return_value.virtualfs_root = tmp_dir
            tmp_dir_path = Path(tmp_dir)
            lvl1_path = tmp_dir_path.joinpath("lvl1")
            lvl2_path = lvl1_path.joinpath("lvl2")
            lvl2_path.makedirs_p()

            tmp_file = tempfile.NamedTemporaryFile(dir=lvl2_path)
            tmp_file_path = Path(tmp_file.name)
            ImageVirtualPathEventTask._remove_path(tmp_file_path)

            assert spy_remove.call_count == 1
            assert not tmp_file_path.exists()
            assert lvl2_path.exists()
            assert lvl1_path.exists()
            assert tmp_dir_path.exists()

    @pytest.mark.asyncio
    @patch.object(AgentConfig, "get")
    async def test_rebuild_fs(self, m_get_acfg, test_db: TestDbResult):
        """tests the proper functioning of the rebuild functionality"""
        m_get_acfg.return_value = AgentConfig()
        m_get_acfg.return_value.virtualfs_remove_empty_dirs = True
        m_get_acfg.return_value.virtualfs_allow_broken_links = True
        m_get_acfg.return_value.piwigo_galleries_host_path = "/tmp"
        pcfg_params = {
            "db_conn_json": json.dumps(test_db.db_host),
            "pwgo_db_name": test_db.piwigo_db,
            "msg_db_name": test_db.messaging_db,
            "rek_db_name": test_db.rekognition_db,
            "dry_run": False
        }
        pcfg = ProgramConfig.initialize(**pcfg_params)
        with tempfile.TemporaryDirectory() as tmp_dir:
            m_get_acfg.return_value.virtualfs_root = tmp_dir
            tmp_dir_path = Path(tmp_dir)
            lvl1_path = tmp_dir_path.joinpath("lvl1")
            lvl2_path = lvl1_path.joinpath("lvl2")
            lvl2_path.makedirs_p()
            tmp_file1 = tempfile.NamedTemporaryFile(dir=tmp_dir_path)
            tmp_file1_path = Path(tmp_file1.name)

            tmp_file2 = tempfile.NamedTemporaryFile(dir=lvl2_path)
            tmp_file2_path = Path(tmp_file2.name)

            await ImageVirtualPathEventTask.rebuild_virtualfs()

            assert not tmp_file1_path.exists()
            assert not tmp_file2_path.exists()
            assert not lvl2_path.exists()
            assert not lvl1_path.exists()

            async with test_db.db_connection_pool.acquire_dict_cursor(db=pcfg.pwgo_db_name) as (cur,_):
                sql = """
                    SELECT virtual_path FROM image_virtual_paths
                """
                await cur.execute(sql)
                # cd into virtual fs root dir to properly resolve relative paths from db
                with tmp_dir_path:
                    for row in await cur.fetchall():
                        # need to use lexists so broken links are counted
                        assert os.path.lexists(row["virtual_path"])
