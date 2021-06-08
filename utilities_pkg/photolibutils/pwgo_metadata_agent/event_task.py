'''container module for EventTask'''
from __future__ import annotations

import inspect,asyncio
from abc import ABC,abstractmethod,abstractclassmethod
from typing import Dict

from .database_event_row import DatabaseEventRow

class EventTask(ABC):
    """Base class for event handling tasks"""
    _tbl_task_map: Dict[str,type] = {}

    @abstractclassmethod
    def get_pending_tasks(cls) -> list[EventTask]:
        """this will be a list of outstanding tasks of the implementing type"""

    @classmethod
    def register_table_task_type(cls, tbl: str, task_cls: type):
        """registers the event task factory for the given table"""
        cls._tbl_task_map[tbl] = task_cls

    @classmethod
    async def get_event_task(cls, evt: DatabaseEventRow) -> EventTask:
        """returns an event task instance for the given event table.
        this may be a new or an existing instance depending on the event"""
        resolved_task_cls = cls._tbl_task_map[evt.table_name]
        return await resolved_task_cls.resolve_event_task(evt)

    @abstractclassmethod
    def resolve_event_task(cls, evt: DatabaseEventRow) -> asyncio.Future:
        """gets concrete task instance"""

    def __init__(self):
        self.status = "INIT"
        self._callbacks = []
        self.__class__.get_pending_tasks().append(self)

    def __await__(self):
        exec_coro = self._execute_task()
        try:
            result = yield from exec_coro.__await__()
            for func in self._callbacks:
                func(self)
            return result

        finally:
            self.__class__.get_pending_tasks().remove(self)

    @abstractmethod
    def schedule_start(self):
        """schedule the execution of the task on the event loop"""

    @abstractmethod
    async def _execute_task(self):
        """execute the event handling task"""

    def attach_callback(self, func):
        """Attaches a callback function to be called upon task completion.
        Will be called with the task object as the first argument."""
        if not callable(func):
            raise ValueError("attempted to attach a non callable callback")
        if inspect.iscoroutinefunction(func):
            raise ValueError("callback must not be a coroutine")

        self._callbacks.append(func)
