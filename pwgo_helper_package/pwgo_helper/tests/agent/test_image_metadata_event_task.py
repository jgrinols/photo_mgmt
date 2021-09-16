"""container module for TestImageTagEventTask"""
import asyncio
from unittest.mock import patch,MagicMock

import pytest

from ...agent.database_event_row import ImageEventRow
from ...agent.event_task import EventTask, EventTaskStatus
from ...agent.image_metadata_event_task import ImageMetadataEventTask
from ...agent.image_metadata_event_task import AutoTagger
from ...agent.file_metadata_writer import FileMetadataWriter
from ...agent.pwgo_image import PiwigoImage
from ...agent.config import Configuration as AgentConfig

class TestImageMetadataEventTask:
    """Tests for the ImageTagEventTask class"""
    @pytest.mark.asyncio
    @patch.object(FileMetadataWriter,"__exit__")
    @patch.object(PiwigoImage,"create")
    @patch.object(FileMetadataWriter,"__enter__")
    @patch.object(AutoTagger,"create")
    async def test_reset_delay(self, mck_atag_create, *_):
        """testing behvior when a subsequent img tag event is handled during wait period"""
        #pylint: disable=protected-access
        evt_row1 = ImageEventRow(image_id=1,
            table_name="image_tag",
            table_primary_key=[1,1],
            operation="INSERT")
        tag_event_handler = await EventTask.get_event_task(evt_row1)
        assert isinstance(tag_event_handler, ImageMetadataEventTask)
        assert len(ImageMetadataEventTask.get_pending_tasks()) == 1

        with patch.object(ImageMetadataEventTask, "_reset_delay", wraps=tag_event_handler._reset_delay) as mck_reset:
            evt_row2 = ImageEventRow(image_id=1,
                table_name="image_tag",
                table_primary_key=[1,2],
                operation="INSERT")
            tag_event_handler.schedule_start()
            await asyncio.sleep(0)
            tag_event_handler2 = await EventTask.get_event_task(evt_row2)
            mck_reset.assert_called_once()
            assert tag_event_handler is tag_event_handler2
            await tag_event_handler
            mck_atag_create.return_value.__aenter__.return_value.add_implicit_tags.assert_awaited_once()

    @pytest.mark.asyncio
    @patch.object(FileMetadataWriter,"__exit__")
    @patch.object(PiwigoImage,"create")
    @patch.object(FileMetadataWriter,"__enter__")
    @patch.object(AutoTagger,"create")
    async def test_reset_after_task_start(self, mck_atag_create, *_):
        """test that attempting to reset the delay timer after the work task
        has started raises an error"""
        #pylint: disable=protected-access
        mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
        evt_row1 = ImageEventRow(image_id=1,
            table_name="image_tag",
            table_primary_key=[1,1],
            operation="INSERT")
        tag_event_handler = await EventTask.get_event_task(evt_row1)
        tag_event_handler.schedule_start()
        await asyncio.sleep(AgentConfig.get().img_tag_wait_secs + 1)
        with pytest.raises(RuntimeError, match="cannot reset delay for task with status DONE"):
            tag_event_handler._reset_delay(delay=1)

    @pytest.mark.asyncio
    @patch.object(FileMetadataWriter,"__exit__")
    @patch.object(PiwigoImage,"create")
    @patch.object(FileMetadataWriter,"__enter__")
    @patch.object(AutoTagger,"create")
    async def test_cancel_after_task_start(self, mck_atag_create, mck_enter, *_):
        """test that attempting to cancel after the work task
        has started raises an error"""
        mck_enter.return_value = MagicMock(spec=FileMetadataWriter)
        mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
        evt_row1 = ImageEventRow(image_id=1,
            table_name="image_tag",
            table_primary_key=[1,1],
            operation="INSERT")
        tag_event_handler = await EventTask.get_event_task(evt_row1)
        tag_event_handler.schedule_start()
        await asyncio.sleep(AgentConfig.get().img_tag_wait_secs + 1)
        with pytest.raises(RuntimeError, match="cannot cancel task in state DONE"):
            tag_event_handler.cancel()

    @pytest.mark.asyncio
    @patch.object(FileMetadataWriter,"__exit__")
    @patch.object(PiwigoImage,"create")
    @patch.object(FileMetadataWriter,"__enter__")
    @patch.object(AutoTagger,"create")
    async def test_awaitable(self, mck_atag_create, mck_enter, *_):
        """tests the basic functionality of the ImageTagEventTask awaitable method
        and it's status progression"""
        mck_enter.return_value = MagicMock(spec=FileMetadataWriter)
        mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
        evt_row1 = ImageEventRow(image_id=1,
            table_name="image_tag",
            table_primary_key=[1,1],
            operation="INSERT")
        tag_event_handler = await EventTask.get_event_task(evt_row1)
        assert tag_event_handler.status == EventTaskStatus.INITIALIZED
        tag_event_handler.schedule_start()
        await asyncio.sleep(0)
        assert tag_event_handler.status == EventTaskStatus.WAITING
        await asyncio.sleep(AgentConfig.get().img_tag_wait_secs + 1)
        res = await tag_event_handler
        assert tag_event_handler.status == EventTaskStatus.DONE
        mck_atag_create.return_value.__aenter__.return_value.add_implicit_tags.assert_awaited_once()
        assert res

    @pytest.mark.asyncio
    @patch.object(FileMetadataWriter,"__exit__")
    @patch.object(PiwigoImage,"create")
    @patch.object(FileMetadataWriter,"__enter__")
    @patch.object(AutoTagger,"create")
    async def test_callback(self, mck_atag_create, mck_enter, *_):
        """tests the functioning of the event task callbacks"""
        mck_enter.return_value = MagicMock(spec=FileMetadataWriter)
        mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
        evt_row1 = ImageEventRow(image_id=1,
            table_name="image_tag",
            table_primary_key=[1,1],
            operation="INSERT")
        tag_event_handler = await EventTask.get_event_task(evt_row1)
        assert tag_event_handler.status == EventTaskStatus.INITIALIZED
        tag_event_handler.schedule_start()
        mck_callback = MagicMock()
        tag_event_handler.attach_callback(mck_callback)
        mck_callback.assert_not_called()
        res = await tag_event_handler
        mck_callback.assert_called_once()
        assert isinstance(mck_callback.call_args[0][0],ImageMetadataEventTask)
        assert res

    @pytest.mark.asyncio
    @patch.object(FileMetadataWriter,"__exit__")
    @patch.object(PiwigoImage,"create")
    @patch.object(FileMetadataWriter,"__enter__")
    @patch.object(AutoTagger,"create")
    async def test_add_remove_tag(self, mck_atag_create, mck_enter, *_):
        """tests that adding a tag then removing the same tag does not cause the tagging
        handler function to be called"""
        mck_enter.return_value = MagicMock(spec=FileMetadataWriter)
        mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
        evt_row1 = ImageEventRow(image_id=1,
            table_name="image_tag",
            table_primary_key=[1,1],
            operation="INSERT")
        evt_row2 = ImageEventRow(image_id=1,
            table_name="image_tag",
            table_primary_key=[1,1],
            operation="DELETE")
        tag_event_handler1 = await EventTask.get_event_task(evt_row1)
        assert tag_event_handler1.status == EventTaskStatus.INITIALIZED
        tag_event_handler1.schedule_start()
        await asyncio.sleep(0)
        tag_event_handler2 = await EventTask.get_event_task(evt_row2)
        assert tag_event_handler2 is tag_event_handler1
        res = await tag_event_handler1
        assert res
        mck_atag_create.return_value.__aenter__.return_value.add_implicit_tags.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_image_cat_basic(self):
        """tests basic functioning when handling a single image category insert"""
        with patch.object(AutoTagger,"create") as mck_atag_create:
            mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
            evt_row1 = ImageEventRow(image_id=1,
                table_name="image_category",
                table_primary_key=[1,125],
                operation="INSERT")
            mdata_event_handler = await EventTask.get_event_task(evt_row1)
            assert mdata_event_handler.status == EventTaskStatus.INITIALIZED
            mdata_event_handler.schedule_start()
            await asyncio.sleep(0)
            res = await mdata_event_handler
            assert res
            mck_atag_create.return_value.__aenter__.return_value.add_implicit_tags.assert_not_awaited()
            mck_atag_create.return_value.__aenter__.return_value.autotag_image.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_image_cat_add_remove(self):
        """tests proper functioning when handling an insert and delete of a category"""
        with patch.object(AutoTagger,"create") as mck_atag_create:
            mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
            evt_row1 = ImageEventRow(image_id=1,
                table_name="image_category",
                table_primary_key=[1,125],
                operation="INSERT")
            evt_row2 = ImageEventRow(image_id=1,
                table_name="image_category",
                table_primary_key=[1,125],
                operation="DELETE")
            mdata_event_handler = await EventTask.get_event_task(evt_row1)
            assert mdata_event_handler.status == EventTaskStatus.INITIALIZED
            mdata_event_handler.schedule_start()
            await asyncio.sleep(0)
            await EventTask.get_event_task(evt_row2)
            await mdata_event_handler
            mck_atag_create.return_value.__aenter__.return_value.autotag_image.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_image_cat_non_autotag(self):
        """tests proper functioning when adding a category that doesn't need any handling"""
        with patch.object(AutoTagger,"create") as mck_atag_create:
            mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
            evt_row1 = ImageEventRow(image_id=1,
                table_name="image_category",
                table_primary_key=[1,1],
                operation="INSERT")
            mdata_event_handler = await EventTask.get_event_task(evt_row1)
            assert mdata_event_handler.status == EventTaskStatus.CANCELLED
            await mdata_event_handler
            mck_atag_create.return_value.__aenter__.return_value.autotag_image.assert_not_awaited()

    @pytest.mark.asyncio
    @patch.object(FileMetadataWriter,"__exit__")
    @patch.object(PiwigoImage,"create")
    @patch.object(FileMetadataWriter,"__enter__")
    @patch.object(AutoTagger,"create")
    async def test_img_tag_and_cat(self, mck_atag_create, mck_enter, *_):
        """tests proper functioning when handling both an image tag and category event"""
        mck_enter.return_value = MagicMock(spec=FileMetadataWriter)
        mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
        evt_row1 = ImageEventRow(image_id=1,
            table_name="image_tag",
            table_primary_key=[1,1],
            operation="INSERT")
        evt_row2 = ImageEventRow(image_id=1,
            table_name="image_category",
            table_primary_key=[1,125],
            operation="INSERT")
        mdata_event_handler = await EventTask.get_event_task(evt_row1)
        assert mdata_event_handler.status == EventTaskStatus.INITIALIZED
        mdata_event_handler.schedule_start()
        await asyncio.sleep(0)
        await EventTask.get_event_task(evt_row2)
        res = await mdata_event_handler
        assert res
        mck_atag_create.return_value.__aenter__.return_value.add_implicit_tags.assert_awaited_once()
        mck_atag_create.return_value.__aenter__.return_value.autotag_image.assert_awaited_once()

    @pytest.mark.asyncio
    @patch.object(FileMetadataWriter,"__exit__")
    @patch.object(PiwigoImage,"create")
    @patch.object(AutoTagger,"create")
    @patch.object(FileMetadataWriter,"__enter__")
    async def test_img_mdata(self, mck_enter, *_):
        """tests basic functioning of metadata handling"""
        mck_enter.return_value = MagicMock(spec=FileMetadataWriter)
        evt_row1 = ImageEventRow(image_id=1,
            table_name="images",
            table_primary_key=[1],
            operation="UPDATE",
            before={
                "name": "before_nm",
                "comment": "before_comment",
                "author": "before_author",
                "date_creation": "before_date_creation"
            },
            after={
                "name": "after_nm",
                "comment": "after_comment",
                "author": "after_author",
                "date_creation": "after_date_creation"
            })
        mdata_event_handler = await EventTask.get_event_task(evt_row1)
        assert mdata_event_handler.status == EventTaskStatus.INITIALIZED
        mdata_event_handler.schedule_start()
        await asyncio.sleep(0)
        res = await mdata_event_handler
        assert res
        mck_enter.return_value.write.assert_called_once()
