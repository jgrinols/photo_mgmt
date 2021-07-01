"""container module for TestConstants"""
# pylint: disable=protected-access,unused-argument
import pytest

from ...agent.config import Configuration

class TestConstants:
    """Constant tests"""

    @pytest.mark.asyncio
    async def test_set_face_index_categories(self, test_db, db_cfg):
        """tests the proper functioning of the set_face_index_categories method.
        runs the method and confirms the correct categories are initialized in the Constants"""
        cfg = Configuration.get()
        cfg.pwgo_db_config = db_cfg
        assert cfg.face_idx_albs == []
        await cfg._set_face_index_categories()
        assert cfg.face_idx_albs == [129,130,131,132,133]
