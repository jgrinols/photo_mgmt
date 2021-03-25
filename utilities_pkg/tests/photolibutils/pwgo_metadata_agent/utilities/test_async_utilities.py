"""wrapper module for TestAsyncUtilities"""
import asyncio

import pytest

from photolibutils.pwgo_metadata_agent import utilities

class TestAsyncUtilities:
    """Tests for async helper functions in the utilities module"""

    @pytest.mark.asyncio
    async def test_delay_coroutine_simple(self):
        """basic test of cancellation behavior of delay_coroutine"""
        async def _mock_coro(msg, **_kwargs):
            await asyncio.sleep(1)
            return msg

        gen = utilities.delayed_task_generator(_mock_coro, "Hello", delay=5)
        sleep_task = next(gen)
        await sleep_task
        mock_coro_task = next(gen)
        res = await mock_coro_task

        assert res == "Hello"

    @pytest.mark.asyncio
    async def test_delay_coroutine_cancel(self):
        """basic test of cancellation behavior of delay_coroutine"""
        async def _mock_coro(msg, **_kwargs):
            await asyncio.sleep(1)
            return msg

        gen = utilities.delayed_task_generator(_mock_coro, "Hello", delay=5)
        sleep_task = next(gen)
        sleep_task.cancel()
        with pytest.raises(StopIteration):
            next(gen)
