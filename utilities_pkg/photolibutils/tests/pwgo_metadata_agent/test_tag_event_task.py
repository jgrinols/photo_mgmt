"""container module for TestTagEventTask"""
import asyncio
from unittest.mock import patch

import pytest

from ...pwgo_metadata_agent.database_event_row import TagEventRow
from ...pwgo_metadata_agent.event_task import EventTask
from ...pwgo_metadata_agent.tag_event_task import TagEventTask

class TestTagEventTask:
    """tests for the TagEventTask class"""
    @pytest.mark.asyncio
    async def test_simple(self):
        """basic functional test of TagEventTask class"""
        evt_row1 = TagEventRow(tag_id=1,
            table_name="tags",
            table_primary_key=[1],
            operation="INSERT")
        tag_event_handler = await EventTask.get_event_task(evt_row1)
        assert isinstance(tag_event_handler, TagEventTask)
        assert len(TagEventTask.get_pending_tasks()) == 1
        assert tag_event_handler.status == "INIT"
        with patch.object(TagEventTask, "_get_action") as mck_act:
            mck_act.return_value = (asyncio.sleep, [1])
            tag_event_handler.schedule_start()
            assert tag_event_handler.status == "EXEC_QUEUED"
            await asyncio.sleep(0)
            assert tag_event_handler.status == "EXEC"
            await tag_event_handler
            assert tag_event_handler.status == "DONE"
