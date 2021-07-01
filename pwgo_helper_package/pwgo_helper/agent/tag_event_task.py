"""wrapper module for TagEventTask"""
from __future__ import annotations

import asyncio

from .autotagger import AutoTagger
from .event_task import EventTask
from .database_event_row import TagEventRow

class TagEventTask(EventTask):
    """Manages the handling of new or deleted tags in the database"""
    _pending_tasks: list[EventTask] = []

    def __init__(self, tag_id):
        super().__init__()
        self.tag_id = tag_id
        self.status = "INIT"
        self._tag_task = None

    @classmethod
    def get_pending_tasks(cls) -> list[TagEventTask]:
        """list of outstanding tag event tasks"""
        return cls._pending_tasks

    @classmethod
    def get_handled_tables(cls) -> list[str]:
        """list of tables handled by ImageVirtualPathEventTask"""
        return ["tags"]

    @classmethod
    def resolve_event_task(cls, evt: TagEventRow) -> asyncio.Future:
        """this class doesn't require any complex resolution logic so we
        just create a new instance and set it as a result on a Future"""
        result_fut = asyncio.Future()
        result_fut.set_result(TagEventTask(evt.tag_id))
        return result_fut

    def schedule_start(self):
        """schedules execution of the tag event handler on the event loop"""
        self._tag_task = asyncio.create_task(self._handle_tag_event())
        self.status = "EXEC_QUEUED"

    async def _handle_tag_event(self):
        self.status = "EXEC"
        action = self._get_action()
        await action[0](*action[1])
        self.status = "DONE"

    def _get_action(self):
        return (AutoTagger.process_new_tag, [self.tag_id])

    async def _execute_task(self):
        res = await self._tag_task
        return res
