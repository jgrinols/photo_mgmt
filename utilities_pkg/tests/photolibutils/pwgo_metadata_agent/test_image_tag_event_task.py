"""container module for TestImageTagEventTask"""
import unittest.mock, asyncio
from time import perf_counter

import pytest

from photolibutils.pwgo_metadata_agent.image_tag_event_task import ImageTagEventTask

class TestImageTagEventTask:
    """Tests for the ImageTagEventTask class"""
    handle_func = 'photolibutils.pwgo_metadata_agent.image_tag_event_task.ImageTagEventTask._exec_autotagger'
    @pytest.mark.asyncio
    async def test_reset_delay(self):
        """testing resetting the delay before handling task has begun"""
        mock_delay = 3
        init_delay = 3
        reset_delay = 10
        async def mock_handle_func(_task_self):
            await asyncio.sleep(mock_delay)

        with unittest.mock.patch(TestImageTagEventTask.handle_func, mock_handle_func):
            tag_event_handler = ImageTagEventTask(1, delay=init_delay)
            tag_event_handler.add_tagging_event(1, "INSERT")
            await asyncio.sleep(0)
            tag_event_handler.reset_delay(reset_delay)
            t_start = perf_counter()
            await asyncio.sleep(0)
            await tag_event_handler
            t_end = perf_counter()
            assert t_end - t_start > mock_delay + reset_delay
            assert t_end - t_start < mock_delay + reset_delay + init_delay

    @pytest.mark.asyncio
    async def test_reset_after_task_start(self):
        """test that attempting to reset the delay timer after the work task
        has started raises an error"""
        async def mock_handle_func(_task_self):
            await asyncio.sleep(3)

        with unittest.mock.patch(TestImageTagEventTask.handle_func, mock_handle_func):
            tag_event_handler = ImageTagEventTask(1, delay=1)
            tag_event_handler.add_tagging_event(1, "INSERT")
            await asyncio.sleep(2)
            with pytest.raises(RuntimeError, match="attempted to reset delay after execution has begun"):
                tag_event_handler.reset_delay(1)

    @pytest.mark.asyncio
    async def test_cancel_after_task_start(self):
        """test that attempting to reset the delay timer after the work task
        has started raises an error"""
        async def mock_handle_func(_task_self):
            await asyncio.sleep(3)

        with unittest.mock.patch(TestImageTagEventTask.handle_func, mock_handle_func):
            tag_event_handler = ImageTagEventTask(1, delay=1)
            tag_event_handler.add_tagging_event(1, "INSERT")
            await asyncio.sleep(2)
            with pytest.raises(RuntimeError, match="attempted to cancel image tag event task after execution has begun"):
                tag_event_handler.cancel()

    @pytest.mark.asyncio
    async def test_awaitable(self):
        """tests the basic functionality of the ImageTagEventTask awaitable method
        and it's status progression"""
        async def mock_handle_func(_task_self):
            await asyncio.sleep(3)

        with unittest.mock.patch(TestImageTagEventTask.handle_func, mock_handle_func):
            tag_event_handler = ImageTagEventTask(1, delay=1)
            tag_event_handler.add_tagging_event(1, "INSERT")
            assert tag_event_handler.status == "WAIT"
            await asyncio.sleep(2)
            assert tag_event_handler.status == "EXEC"
            res = await tag_event_handler
            assert tag_event_handler.status == "DONE"
            assert res

    @pytest.mark.asyncio
    async def test_callback(self):
        """tests the basic functionality of the ImageTagEventTask awaitable method
        and it's status progression"""
        async def mock_handle_func(_task_self):
            await asyncio.sleep(3)

        def callback(tag_handler):
            assert isinstance(tag_handler, ImageTagEventTask)
            tag_handler.callback_check = True

        with unittest.mock.patch(TestImageTagEventTask.handle_func, mock_handle_func):
            tag_event_handler = ImageTagEventTask(1, delay=1)
            tag_event_handler.add_tagging_event(1, "INSERT")
            tag_event_handler.attach_callback(callback)
            assert not hasattr(tag_event_handler, "callback_check")
            res = await tag_event_handler
            assert res
            assert hasattr(tag_event_handler, "callback_check")

    @pytest.mark.asyncio
    async def test_add_remove_tag(self):
        """tests that adding a tag then removing the same tag does not cause the tagging
        handler function to be called"""
        mock_delay = 3
        init_delay = 1
        async def mock_handle_func(_task_self):
            _task_self.handled = True
            await asyncio.sleep(mock_delay)

        with unittest.mock.patch(TestImageTagEventTask.handle_func, mock_handle_func):
            tag_event_handler = ImageTagEventTask(1, delay=init_delay)
            tag_event_handler.add_tagging_event(1, "INSERT")
            tag_event_handler.add_tagging_event(1, "DELETE")
            res = await tag_event_handler
            assert res
            assert not hasattr(tag_event_handler, "handled")
