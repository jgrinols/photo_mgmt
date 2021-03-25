"""container module for TestEventDispatcher"""
import logging,asyncio,json
from unittest import mock
from unittest.mock import PropertyMock,patch,MagicMock

import click_log
import pytest
from pymysqlreplication.row_event import WriteRowsEvent

from photolibutils.pwgo_metadata_agent.constants import Constants
from photolibutils.pwgo_metadata_agent.event_dispatcher import EventDispatcher
import photolibutils.pwgo_metadata_agent.image_tag_event_task as image_tag_event_task

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

class TestEventDispatcher:
    """EventDispatcher tests"""

    @staticmethod
    def _build_evt_msg(**kwargs):
        msg = {}
        msg[kwargs["id_nm"]] = kwargs["id_val"]
        msg["table_name"] = kwargs["t_nm"]
        msg["table_primary_key"] = kwargs["pk_vals"]
        msg["operation"] = kwargs["oper"]
        return msg

    @staticmethod
    def _build_evt_row(**kwargs):
        row = {"values": {}}
        row["values"]["message_type"] = kwargs["m_type"]
        row["values"]["message"] = json.dumps(TestEventDispatcher._build_evt_msg(**kwargs))
        return row

    @staticmethod
    @pytest.fixture
    def mck_rows():
        """returns a list of stub event rows"""
        return [
            TestEventDispatcher._build_evt_row(m_type="IMG_METADATA",id_nm="image_id",id_val=1
                ,t_nm="image_tag",pk_vals=[1,1],oper="INSERT")
        ]

    @pytest.mark.asyncio
    @patch('photolibutils.pwgo_metadata_agent.image_tag_event_task.ImageTagEventTask')
    @patch('pymysqlreplication.row_event.WriteRowsEvent',spec=WriteRowsEvent)
    async def test_queue_workers(self, mck_evt_cls, mck_img_tag_cls, mck_rows):
        type(mck_evt_cls.return_value).rows = PropertyMock(return_value=mck_rows)
        mck_img_tag_cls.return_value = "hi"
        mck_evt = mck_evt_cls()
        mck_img_tag_task = image_tag_event_task.ImageTagEventTask(1, delay=1)
        queue = asyncio.Queue()
        worker_cnt = 3
        msg = json.loads(mck_evt.rows[0]["values"]["message"])
        image_id = msg["image_id"]

        dispatcher = await EventDispatcher.create(queue, worker_cnt)
        assert len(dispatcher.workers) == worker_cnt

        await queue.put(mck_evt)
        await queue.join()

        mck_img_tag_cls.assert_called_once_with(image_id, delay=Constants.IMG_TAG_WAIT_SECS)

        await dispatcher
