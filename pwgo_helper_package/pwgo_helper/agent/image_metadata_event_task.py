"""wrapper module for ImageTagEventTask"""
from __future__ import annotations
import asyncio

from py_linq import Enumerable

from ..config import Configuration as ProgramConfig
from .config import Configuration as AgentConfig
from .event_task import EventTask, EventTaskStatus
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
        self._sleep_futures = []
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
        logger = ProgramConfig.get().get_logger(__name__)
        # initialize result to dummy future
        result_fut: asyncio.Future = asyncio.Future()
        inner_result_fut = asyncio.Future()
        inner_result_fut.set_result(True)
        result_fut.set_result(inner_result_fut)
        existing_task = next((t for t in cls._pending_tasks if t.image_id == evt.image_id), None)
        if existing_task:
            logger.debug("found existing task for image %s", evt.image_id)
            if existing_task.is_waiting():
                if evt.table_name == "images" and evt.db_event_type == "DELETE":
                    existing_task.cancel()
                    logger.debug("cancelling existing task for deleted image %s", evt.image_id)
                else:
                    logger.debug("adding event to existing task for image %s", evt.image_id)
                    existing_task.add_event(evt)

                result_fut = asyncio.Future()
                result_fut.set_result(existing_task)
            else:
                if evt.table_name == "images" and evt.db_event_type == "DELETE":
                    # just keep the dummy result future in this case
                    logger.debug("existing task for delete image %s has begun executing. new task will not be created."
                        , evt.image_id)
                else:
                    logger.debug("existing task has begun executing. waiting and creating new task for image %s"
                        , evt.image_id)
                    async def wait_for_current_task():
                        await existing_task
                        new_task = ImageMetadataEventTask(evt.image_id)
                        new_task.add_event(evt)
                        return new_task

                    result_fut = asyncio.ensure_future(wait_for_current_task())

        else:
            if evt.table_name == "images" and evt.db_event_type == "DELETE":
                logger.debug("no existing task found for deleted image %s. no action required.", evt.image_id)
                # just keep the dummy result future in this case
            else:
                logger.debug("no exising task found for image %s. creating new task.", evt.image_id)
                new_task = ImageMetadataEventTask(evt.image_id)
                # if the event on this new task is a no-op (like a non autotag category)
                # then we make sure to keep the dummy future so we don't create a
                # pointless task that waits and then does nothing
                keep_task = new_task.add_event(evt)
                result_fut = asyncio.Future()
                result_fut.set_result(new_task)
                if not keep_task:
                    new_task.cancel()

        return result_fut

    def schedule_start(self) -> bool:
        """schedules execution of the image tag event handling task"""
        if self.is_cancelled():
            raise RuntimeError("attempted to schedule a cancelled metadata event task")

        if not self.is_scheduled():
            sleep_fut = asyncio.ensure_future(asyncio.sleep(self._std_delay))
            sleep_fut.add_done_callback(self._schedule_action_task)
            self._sleep_futures.append(sleep_fut)
            self.status = EventTaskStatus.WAITING
            return True

        return False

    async def _execute_task(self):
        if self._sleep_futures:
            await self._sleep_futures[-1]
            # we may have appended a new sleep future...
            if not self._sleep_futures[-1].done():
                await self._execute_task()
                return

        if not self.status == EventTaskStatus.CANCELLED:
            while not self._action_task:
                await asyncio.sleep(0)
            return await self._action_task

    def cancel(self):
        """Cancels a the waiting image metadata event task"""
        if not self.is_waiting():
            raise RuntimeError(f"cannot cancel task in state {self.status}")

        if self._sleep_futures:
            self._sleep_futures[-1].remove_done_callback(self._schedule_action_task)
        self.status = EventTaskStatus.CANCELLED
        ImageMetadataEventTask._pending_tasks.remove(self)

    def _reset_delay(self, **kwargs):
        """Resets the amount of time the task is to wait before proceeding to given number of seconds"""
        if not self.is_waiting():
            raise RuntimeError(f"cannot reset delay for task with status {self.status}")

        if "delay" in kwargs:
            delay = kwargs["delay"]
        else:
            delay = AgentConfig.get().img_tag_wait_secs

        # theoretically this could be called before _execute_task...in that case noop
        if self._sleep_futures:
            self._sleep_futures[-1].remove_done_callback(self._schedule_action_task)
            self._sleep_futures.append(asyncio.ensure_future(asyncio.sleep(delay)))
            self._sleep_futures[-1].add_done_callback(self._schedule_action_task)

    def add_event(self, evt: ImageEventRow) -> bool:
        """adds an event to the image metadata event task"""
        if not self.is_waiting():
            raise RuntimeError(f"cannot add a new event to task in state {self.status}")

        self._reset_delay(delay=self._std_delay)

        if evt.table_name == "image_tag":
            return self._add_tagging_event(evt.table_primary_key[1], evt.db_event_type)
        elif evt.table_name == "image_category":
            return self._add_category_event(evt.table_primary_key[1], evt.db_event_type)
        elif evt.table_name == "images":
            return self._add_image_event()
        else:
            raise RuntimeError(f"No event handler for table {evt.table_name}")

    def _add_tagging_event(self, tag_id, oper) -> bool:
        if tag_id not in self._included_tags:
            self._included_tags[tag_id] = 0
        if oper == "INSERT":
            increment = 1
        elif oper == "DELETE":
            increment = -1
        else:
            raise ValueError("unrecognized tag event operation")

        self._write_metadata = True
        self._included_tags[tag_id] += increment

        return True

    def _add_category_event(self, category_id, oper) -> bool:
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

            return True
        return False

    def _add_image_event(self) -> bool:
        self._write_metadata = True
        return True

    def _schedule_action_task(self, _fut):
        self._action_task = asyncio.create_task(self._handle_events())
        self._action_task.set_name("exec_task")
        self.status = EventTaskStatus.EXEC_QUEUED

    async def _handle_events(self):
        self.status = EventTaskStatus.EXEC
        loop = asyncio.get_event_loop()
        handle_tags = Enumerable(self._included_tags.values()).any(lambda x: x > 0)
        handle_cats = Enumerable(self._included_cats.values()).any(lambda x: x > 0)
        if handle_tags or handle_cats:
            async with AutoTagger.create(self.image_id) as tagger:
                if handle_tags:
                    await tagger.add_implicit_tags()
                if handle_cats:
                    await tagger.autotag_image()
        if self._write_metadata:
            pwgo_img = await PiwigoImage.create(self.image_id, load_metadata=True)
            if not ProgramConfig.get().dry_run:
                with FileMetadataWriter(pwgo_img) as writer:
                    await loop.run_in_executor(None,writer.write)
        self.status = EventTaskStatus.DONE
        return True
