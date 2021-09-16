"""container module for TestConstants"""
# pylint: disable=protected-access,unused-argument
import json

import pytest

from ...agent.config import Configuration as AgentConfig
from ...config import Configuration as ProgramConfig
from .conftest import TestDbResult

class TestConstants:
    """Constant tests"""

    @pytest.mark.asyncio
    async def test_set_face_index_categories(self, test_db: TestDbResult):
        """tests the proper functioning of the set_face_index_categories method.
        runs the method and confirms the correct categories are initialized in the Constants"""
        acfg = AgentConfig.get()
        pcfg_params = {
            "db_conn_json": json.dumps(test_db.db_host),
            "pwgo_db_name": test_db.piwigo_db,
            "msg_db_name": test_db.messaging_db,
            "rek_db_name": test_db.rekognition_db
        }
        ProgramConfig.initialize(**pcfg_params)
        assert acfg.face_idx_albs == []
        await acfg._set_face_index_categories()
        assert acfg.face_idx_albs == [129,130,131,132,133]
