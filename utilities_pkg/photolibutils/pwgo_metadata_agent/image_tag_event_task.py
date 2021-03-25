"""wrapper module for ImageTagEventTask"""
from __future__ import annotations
import logging,asyncio,inspect

import click_log
from py_linq import Enumerable

from photolibutils.pwgo_metadata_agent.autotagger import AutoTagger

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

class ImageTagEventTask:
    """Manages the handling of any routines that should run after an image
    is tagged. Allows for a delay in executing these routines so that multiple
    tags on an image in close succession can be batched."""
    def __init__(self, image_id, delay=0):
        self.image_id = image_id
        self._current_sleep_fut = asyncio.ensure_future(asyncio.sleep(delay))
        self._current_sleep_fut.add_done_callback(self._attach_handler_function)
        self.status = "WAIT"
        self._tag_task = None
        self._std_delay = delay
        self._included_tags = {}
        self._callbacks = []

    def __await__(self):
        sleep_fut = self._current_sleep_fut
        yield from sleep_fut.__await__()
        if sleep_fut is not self._current_sleep_fut:
            self.__await__()

        if not self._current_sleep_fut.cancelled():
            while not self._tag_task:
                asyncio.sleep(0).__await__()
            result = yield from self._tag_task.__await__()
            for func in self._callbacks:
                func(self)
            return result

    def mutable(self):
        """returns True if the task is open for new tag events, False if not"""
        return self.status == "WAIT"

    def cancel(self):
        """Cancels a the waiting image tag event task"""
        if self.status != "WAIT":
            raise RuntimeError("attempted to cancel image tag event task after execution has begun")

        self._current_sleep_fut.remove_done_callback(self._attach_handler_function)
        self._current_sleep_fut.cancel()
        self.status = "CANCELLED"

    def reset_delay(self, delay=0):
        """Resets the amount of time the task is to wait before proceeding to given number of seconds"""
        if self.status != "WAIT":
            raise RuntimeError("attempted to reset delay after execution has begun")

        self._current_sleep_fut.remove_done_callback(self._attach_handler_function)
        self._current_sleep_fut.cancel()
        self._current_sleep_fut = asyncio.ensure_future(asyncio.sleep(delay))
        self._current_sleep_fut.add_done_callback(self._attach_handler_function)

    def add_tagging_event(self, tag_id, oper):
        """appends the given tag id to list of tag events for this handler."""
        if self.status != "WAIT":
            raise RuntimeError("attempted to process a new tagging event after execution has begun")

        self.reset_delay(self._std_delay)
        if tag_id not in self._included_tags:
            self._included_tags[tag_id] = 0
        if oper == "INSERT":
            increment = 1
        elif oper == "DELETE":
            increment = -1
        else:
            raise ValueError("unrecognized tag event operation")

        self._included_tags[tag_id] += increment

    def attach_callback(self, func):
        """Attaches a callback function to be called upon task completion.
        Will be called with the task object as the first argument."""
        if not callable(func):
            raise ValueError("attempted to attach a non callable callback")
        if inspect.iscoroutinefunction(func):
            raise ValueError("callback must not be a coroutine")

        self._callbacks.append(func)

    def _attach_handler_function(self, _fut):
        self._tag_task = asyncio.create_task(self._handle_tag_event())
        self.status = "EXEC_QUEUED"

    async def _handle_tag_event(self):
        self.status = "EXEC"
        if Enumerable(self._included_tags.values()).any(lambda x: x > 0):
            await self._exec_autotagger()
        self.status = "DONE"
        return True

    async def _exec_autotagger(self):
        async with AutoTagger.create(self.image_id) as tagger:
            await tagger.add_implicit_tags()
