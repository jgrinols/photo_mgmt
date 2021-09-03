"""container module for TestEventDispatcher"""
# pylint: disable=protected-access
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from ...agent.event_dispatcher import EventDispatcher
from ...agent.config import Configuration
from ...agent.autotagger import AutoTagger
from ...agent.aggregate_results_error import AggregateResultsError
from ...agent.image_metadata_event_task import ImageMetadataEventTask

class TestEventDispatcher:
    """EventDispatcher tests"""

    @pytest.mark.asyncio
    async def test_create(self):
        """test the creation of an event dispatcher. verifies correct initialization
        and startup"""
        dispatcher = await EventDispatcher.create(5)
        assert len(dispatcher.workers) == 5
        assert dispatcher.workers[0].get_name() == "worker-0"
        await dispatcher.stop()

        _ = dispatcher.get_results()

    @pytest.mark.asyncio
    async def test_queue_event(self):
        """tests that a queued event is picked up by a worker"""
        dispatcher = await EventDispatcher.create(5)
        img_id = 1
        mck_evt = { "values": { "message_type": "IMG_METADATA", "message": f'''{{
            "image_id": {img_id}, "table_name": "image_category", "table_primary_key": [{img_id},1], "operation": "INSERT"
        }}''' }}
        dispatcher.process_event = AsyncMock()

        try:
            await dispatcher.queue_event(mck_evt)

        finally:
            await dispatcher.stop()
            _ = dispatcher.get_results()

        dispatcher.process_event.assert_awaited_once()
        assert dispatcher.process_event.await_args.args[0].image_id == img_id

    @pytest.mark.asyncio
    async def test_saturate_workers(self):
        """tests proper functioning when we queue up more task than we have workers"""
        dispatcher = await EventDispatcher.create(3)
        mck_evts = [
            { "values": { "message_type": "IMG_METADATA", "message": '''{
                "image_id": 1, "table_name": "image_category", "table_primary_key": [1,1], "operation": "INSERT"
            }''' }},
            { "values": { "message_type": "IMG_METADATA", "message": '''{
                "image_id": 1, "table_name": "image_category", "table_primary_key": [1,2], "operation": "INSERT"
            }''' }},
            { "values": { "message_type": "IMG_METADATA", "message": '''{
                "image_id": 1, "table_name": "image_category", "table_primary_key": [2,1], "operation": "INSERT"
            }''' }},
            { "values": { "message_type": "IMG_METADATA", "message": '''{
                "image_id": 1, "table_name": "image_category", "table_primary_key": [3,1], "operation": "INSERT"
            }''' }}
        ]
        async def mck_process_evt(_raw_evt):
            await asyncio.sleep(1)
        dispatcher.process_event = AsyncMock(wraps=mck_process_evt)

        try:
            for evt in mck_evts:
                await dispatcher.queue_event(evt)

        finally:
            await dispatcher.stop()
            _ = dispatcher.get_results()

        assert dispatcher.process_event.await_count == len(mck_evts)

    @pytest.mark.asyncio
    async def test_force_stop(self):
        """tests the proper handling of the force stop flag. workers should finish their
        current task but not clear the queue before ending."""
        worker_cnt = 3
        dispatcher = await EventDispatcher.create(worker_cnt)
        mck_evts = [
            { "values": { "message_type": "IMG_METADATA", "message": '''{
                "image_id": 1, "table_name": "image_category", "table_primary_key": [1,1], "operation": "INSERT"
            }''' }},
            { "values": { "message_type": "IMG_METADATA", "message": '''{
                "image_id": 1, "table_name": "image_category", "table_primary_key": [1,2], "operation": "INSERT"
            }''' }},
            { "values": { "message_type": "IMG_METADATA", "message": '''{
                "image_id": 1, "table_name": "image_category", "table_primary_key": [2,1], "operation": "INSERT"
            }''' }},
            { "values": { "message_type": "IMG_METADATA", "message": '''{
                "image_id": 1, "table_name": "image_category", "table_primary_key": [3,1], "operation": "INSERT"
            }''' }}
        ]
        async def mck_process_evt(_raw_evt):
            await asyncio.sleep(1)
        dispatcher.process_event = AsyncMock(wraps=mck_process_evt)

        try:
            for evt in mck_evts:
                await dispatcher.queue_event(evt)

        finally:
            await dispatcher.stop(force=True)
            _ = dispatcher.get_results()

        assert dispatcher.process_event.await_count == worker_cnt

    @pytest.mark.asyncio
    @patch('pwgo_helper.agent.event_dispatcher.AgentConfig')
    async def test_delay_disp(self, m_cfg):
        """tests the proper functioning of the mechanism which forces
         new events to wait for dispatching under certain scenarios"""
        sync_complete = False
        async def sync_face_index():
            nonlocal sync_complete
            await asyncio.sleep(5)
            sync_complete = True

        real_schedule_start = ImageMetadataEventTask.schedule_start
        def schedule_start(img_task_self):
            nonlocal sync_complete
            assert sync_complete
            return real_schedule_start(img_task_self)

        async def handle_events(_):
            await asyncio.sleep(.1)

        m_cfg.get.return_value = MagicMock(spec=Configuration)
        m_cfg.get.return_value.face_idx_albs = [999]
        m_cfg.get.return_value.img_tag_wait_secs = 1

        img1_id = 524
        img2_id = 612
        mck_face_idx_evt = { "values": { "message_type": "IMG_METADATA", "message": f'''{{
            "image_id": {img1_id}, "table_name": "image_category", "table_primary_key": [{img1_id},999], "operation": "INSERT"
        }}''' }}
        mck_next_evt = { "values": { "message_type": "IMG_METADATA", "message": f'''{{
            "image_id": {img2_id}, "table_name": "image_category", "table_primary_key": [{img2_id},1], "operation": "INSERT"
        }}''' }}

        with patch.object(AutoTagger, "sync_face_index", sync_face_index):
            with patch.object(ImageMetadataEventTask, "schedule_start", schedule_start):
                with patch.object(ImageMetadataEventTask, "_handle_events", handle_events):
                    try:
                        dispatcher = await EventDispatcher.create(10)
                        await dispatcher.queue_event(mck_face_idx_evt)
                        await asyncio.sleep(1)
                        await dispatcher.queue_event(mck_next_evt)
                    finally:
                        await dispatcher.stop()
                        _ = dispatcher.get_results()

    @pytest.mark.asyncio
    @patch('pwgo_helper.agent.event_dispatcher.AutoTagger')
    async def test_non_autotag(self, _mck_atag):
        """verify that adding an image to a category that isn't the autotag
        category does not cause autotagging to kick off"""
        img_id = 250
        mck_evt = { "values": { "message_type": "IMG_METADATA", "message": f'''{{
            "image_id": {img_id}, "table_name": "image_category", "table_primary_key": [{img_id},119], "operation": "INSERT"
        }}''' }}

        with patch.object(AutoTagger,"create") as mck_atag_create:
            mck_atag_create.return_value.__aenter__.return_value = MagicMock(spec=AutoTagger)
            try:
                dispatcher = await EventDispatcher.create(10)
                await dispatcher.queue_event(mck_evt)
                await asyncio.sleep(3)
            finally:
                await dispatcher.stop()
                _ = dispatcher.get_results()

            mck_atag_create.return_value.__aenter__.return_value.autotag_image.assert_not_awaited()

    @pytest.mark.asyncio
    @patch('pwgo_helper.agent.event_dispatcher.AutoTagger')
    async def test_worker_respawn(self, _mck_at):
        """verifies that a new worker is spawned if an existing worker
        encounters an exception but we're still below the error limit"""
        worker_cnt = 3
        img_id = 654421
        mck_evt = { "values": { "message_type": "IMG_METADATA", "message": f'''{{
            "image_id": {img_id}, "table_name": "image_category", "table_primary_key": [{img_id},{Configuration.get().auto_tag_alb}], "operation": "INSERT"
        }}''' }}
        with patch('pwgo_helper.agent.image_metadata_event_task.AutoTagger.create') as mck_at_create:
            mck_at_create.side_effect = RuntimeError("mock error")
            try:
                dispatcher = await EventDispatcher.create(worker_cnt, error_limit=3)
                await dispatcher.queue_event(mck_evt)
                await asyncio.sleep(3)
            finally:
                await dispatcher.stop()
                with pytest.raises(AggregateResultsError):
                    _ = dispatcher.get_results()

            assert len(dispatcher.workers) == worker_cnt + 1

    @pytest.mark.asyncio
    async def test_error_limit(self, mocker):
        """verifies that the dispatcher keeps running until error limit is reached"""
        worker_cnt = 3
        error_limit = 2
        dispatcher = await EventDispatcher.create(worker_cnt, error_limit=error_limit)
        spy_stop = mocker.spy(dispatcher, "stop")
        mck_evt_1 = { "values": { "message_type": "IMG_METADATA", "message": f'''{{
            "image_id": 6545, "table_name": "image_category", "table_primary_key": [6545,{Configuration.get().auto_tag_alb}], "operation": "INSERT"
        }}''' }}
        mck_evt_2 = { "values": { "message_type": "IMG_METADATA", "message": f'''{{
            "image_id": 854251, "table_name": "image_category", "table_primary_key": [854251,{Configuration.get().auto_tag_alb}], "operation": "INSERT"
        }}''' }}
        with patch('pwgo_helper.agent.image_metadata_event_task.AutoTagger.create') as mck_at_create:
            mck_at_create.side_effect = [RuntimeError("mock error 1"), RuntimeError("mock error 2")]
            try:
                await dispatcher.queue_event(mck_evt_1)
                await asyncio.sleep(3)
                spy_stop.assert_not_awaited()
                await dispatcher.queue_event(mck_evt_2)
                await asyncio.sleep(3)
                spy_stop.assert_awaited_once_with(force=True)

            finally:
                await dispatcher.stop()
                with pytest.raises(AggregateResultsError):
                    _ = dispatcher.get_results()

            assert spy_stop.await_count == 2
