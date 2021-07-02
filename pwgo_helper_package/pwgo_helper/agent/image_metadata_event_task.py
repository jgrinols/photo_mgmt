"""wrapper module for ImageTagEventTask"""
from __future__ import annotations
import asyncio

from py_linq import Enumerable

from ..config import Configuration as ProgramConfig
from .config import Configuration as AgentConfig
from .event_task import EventTask
from .autotagger import AutoTagger
from .pwgo_image import PiwigoImage
from .file_metadata_writer import FileMetadataWriter
from .database_event_row import ImageEventRow

class ImageMetadataEventTask(EventTask):
    """Manages the handling of any routines that should run after an image
    is tagged. Allows for a delay in executing these routines so that multiple
    tags on an image in close succession can be batched."""
    _pending_tasks: list[ImageMetadataEventTask] = []

    def __init__(self, image_id, **kwargs):
        super().__init__()
        self.image_id = image_id
        self._current_sleep_fut = None
        self._action_task = None
        if "delay" in kwargs:
            self._std_delay = kwargs["delay"]
        else:
            self._std_delay = AgentConfig.get().img_tag_wait_secs
        self._included_tags = {}
        self._included_cats = {}
        self._write_metadata = False

    @classmethod
    def get_pending_tasks(cls) -> list[ImageMetadataEventTask]:
        """list of outstanding image tag event tasks"""
        return cls._pending_tasks

    @classmethod
    def get_handled_tables(cls) -> list[str]:
        """list of tables handled by ImageMetadataEventTask"""
        return ["image_tag","image_category","images"]

    @classmethod
    def resolve_event_task(cls, evt: ImageEventRow) -> asyncio.Future:
        # pylint: disable=protected-access
        result_fut: asyncio.Future = None
        existing_task = next((t for t in cls._pending_tasks if t.image_id == evt.image_id), None)
        if existing_task:
            if existing_task.is_waiting():
                existing_task.add_event(evt)

                result_fut = asyncio.Future()
                result_fut.set_result(existing_task)
            else:
                async def wait_for_current_task():
                    await existing_task
                    new_task = ImageMetadataEventTask(evt.image_id)
                    new_task.add_event(evt)
                    return new_task

                result_fut = asyncio.ensure_future(wait_for_current_task())

        else:
            new_task = ImageMetadataEventTask(evt.image_id)
            new_task.add_event(evt)
            result_fut = asyncio.Future()
            result_fut.set_result(new_task)

        return result_fut

    def schedule_start(self):
        """schedules execution of the image tag event handling task"""
        if not self.is_scheduled():
            self._current_sleep_fut = asyncio.ensure_future(asyncio.sleep(self._std_delay))
            self._current_sleep_fut.add_done_callback(self._schedule_action_task)
            self.status = "WAIT"

    async def _execute_task(self):
        sleep_fut = self._current_sleep_fut
        await sleep_fut
        if sleep_fut is not self._current_sleep_fut:
            await self._execute_task()

        if not self._current_sleep_fut.cancelled():
            while not self._action_task:
                await asyncio.sleep(0)
            return await self._action_task

    def is_waiting(self) -> bool:
        """returns True if the task is open for new tag events, False if not"""
        return self.status in ["INIT","WAIT"]

    def cancel(self):
        """Cancels a the waiting image tag event task"""
        if not self.is_waiting():
            raise RuntimeError("attempted to cancel image tag event task after execution has begun")

        self._current_sleep_fut.remove_done_callback(self._schedule_action_task)
        self._current_sleep_fut.cancel()
        self.status = "CANCELLED"

    def _reset_delay(self, **kwargs):
        """Resets the amount of time the task is to wait before proceeding to given number of seconds"""
        if not self.is_waiting():
            raise RuntimeError("attempted to reset delay after execution has begun")

        if "delay" in kwargs:
            delay = kwargs["delay"]
        else:
            delay = AgentConfig.get().img_tag_wait_secs

        # theoretically this could be called before _execute_task...in that case noop
        if self._current_sleep_fut:
            self._current_sleep_fut.remove_done_callback(self._schedule_action_task)
            self._current_sleep_fut.cancel()
            self._current_sleep_fut = asyncio.ensure_future(asyncio.sleep(delay))
            self._current_sleep_fut.add_done_callback(self._schedule_action_task)

    def add_event(self, evt: ImageEventRow):
        """adds an event to the image metadata event task"""
        if evt.table_name == "image_tag":
            self._add_tagging_event(evt.table_primary_key[1], evt.db_event_type)
        elif evt.table_name == "image_category":
            self._add_category_event(evt.table_primary_key[1], evt.db_event_type)
        elif evt.table_name == "images":
            self._add_image_event()
        else:
            raise RuntimeError(f"No event handler for table {evt.table_name}")

    def _add_tagging_event(self, tag_id, oper):
        if not self.is_waiting():
            raise RuntimeError("attempted to process a new tagging event after execution has begun")

        self._reset_delay(delay=self._std_delay)
        if tag_id not in self._included_tags:
            self._included_tags[tag_id] = 0
        if oper == "INSERT":
            increment = 1
        elif oper == "DELETE":
            increment = -1
        else:
            raise ValueError("unrecognized tag event operation")

        self._included_tags[tag_id] += increment

    def _add_category_event(self, category_id, oper):
        if not self.is_waiting():
            raise RuntimeError("attempted to process a new category event after execution has begun")

        # resetting the delay even if we're not handling this category id
        # thinking is that it still indicates ongoing activity on this image
        # so might as well delay
        self._reset_delay(delay=self._std_delay)

        if category_id == AgentConfig.get().auto_tag_alb:
            # since we're only handling the autotag category, this implementation
            # could be simplified, but not worth changing because it will make it
            # simpler to handle more complex category logic in the future if the need arises.
            if category_id not in self._included_cats:
                self._included_cats[category_id] = 0
            if oper == "INSERT":
                increment = 1
            elif oper == "DELETE":
                increment = -1
            else:
                raise ValueError("unrecognized category event operation")

            self._included_cats[category_id] += increment

    def _add_image_event(self):
        if not self.is_waiting():
            raise RuntimeError("attempted to process a new image event after execution has begun")

        self._write_metadata = True

    def _schedule_action_task(self, _fut):
        self._action_task = asyncio.create_task(self._handle_events())
        self.status = "EXEC_QUEUED"

    async def _handle_events(self):
        loop = asyncio.get_event_loop()
        handle_tags = Enumerable(self._included_tags.values()).any(lambda x: x > 0)
        handle_cats = Enumerable(self._included_cats.values()).any(lambda x: x > 0)
        if handle_tags or handle_cats:
            async with AutoTagger.create(self.image_id) as tagger:
                if handle_tags:
                    self.status = "EXEC_TAG_HANDLER"
                    await tagger.add_implicit_tags()
                if handle_cats:
                    self.status = "EXEC_CAT_HANDLER"
                    await tagger.autotag_image()
        if self._write_metadata:
            self.status = "EXEC_MDATA_HANDLER"
            pwgo_img = PiwigoImage.create(self.image_id, load_metadata=True)
            if not ProgramConfig.get().dry_run:
                with FileMetadataWriter(pwgo_img) as writer:
                    await loop.run_in_executor(None,writer.write)
        self.status = "DONE"
        return True
