"""container module for TestConstants"""
# pylint: disable=protected-access,unused-argument
import pytest

from ...pwgo_metadata_agent.constants import Constants

class TestConstants:
    """Constant tests"""

    @pytest.mark.asyncio
    async def test_set_face_index_categories(self, test_db):
        """tests the proper functioning of the set_face_index_categories method.
        runs the method and confirms the correct categories are initialized in the Constants"""
        assert Constants.FACE_IDX_ALBS == []
        await Constants._set_face_index_categories()
        assert Constants.FACE_IDX_ALBS == [129,130,131,132,133]
