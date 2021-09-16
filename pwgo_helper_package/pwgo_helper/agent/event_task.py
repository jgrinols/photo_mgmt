'''container module for EventTask'''
from __future__ import annotations

import inspect,asyncio
from abc import ABC,abstractmethod,abstractclassmethod
from typing import Dict
from enum import IntEnum

from .database_event_row import DatabaseEventRow
from ..config import Configuration as ProgramConfig

class EventTask(ABC):
    """Base class for event handling tasks"""
    _tbl_task_map: Dict[str,type] = {}

    @abstractclassmethod
    def get_pending_tasks(cls) -> list[EventTask]:
        """this will be a list of outstanding tasks of the implementing type"""

    @abstractclassmethod
    def get_handled_tables(cls) -> list[str]:
        """returns a list of table names that the given EventTask type is responsible for"""

    @staticmethod
    def get_logger(name=None):
        """gets a logger..."""
        if not name:
            name = __name__
        return ProgramConfig.get().get_logger(name)

    @classmethod
    def register_table_task_types(cls, sub_classes):
        """registers subclasses as table handlers"""
        for sub_cls in sub_classes:
            for tbl in sub_cls.get_handled_tables():
                cls._tbl_task_map[tbl] = sub_cls

    @classmethod
    async def get_event_task(cls, evt: DatabaseEventRow) -> EventTask:
        """returns an event task instance for the given event table.
        this may be a new or an existing instance depending on the event"""
        if not cls._tbl_task_map:
            # pylint: disable=import-outside-toplevel
            from .image_metadata_event_task import ImageMetadataEventTask
            from .tag_event_task import TagEventTask
            from .image_virtual_path_event_task import ImageVirtualPathEventTask
            cls.register_table_task_types([ImageMetadataEventTask,TagEventTask,ImageVirtualPathEventTask])

        if evt.table_name in cls._tbl_task_map:
            resolved_task_cls = cls._tbl_task_map[evt.table_name]
            # this await returns an awaitable
            return await resolved_task_cls.resolve_event_task(evt)

        cls.get_logger().warning("No registered handler task for table %s", evt.table_name)
        return None

    @abstractclassmethod
    def resolve_event_task(cls, evt: DatabaseEventRow) -> asyncio.Future:
        """gets concrete task instance"""

    def __init__(self):
        self._logger = EventTask.get_logger(type(self).__name__)
        self.status = EventTaskStatus.INITIALIZED
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
            if self in self.__class__.get_pending_tasks():
                self.__class__.get_pending_tasks().remove(self)

    @abstractmethod
    def schedule_start(self):
        """schedule the execution of the task on the event loop"""

    @abstractmethod
    async def _execute_task(self):
        """execute the event handling task"""

    def is_scheduled(self) -> bool:
        """returns a boolean indicating whether this task has been
        scheduled for execution"""
        return self.status > EventTaskStatus.INITIALIZED

    def is_cancelled(self) -> bool:
        """returns a boolean indicating whether this task has been cancelled"""
        return self.status == EventTaskStatus.CANCELLED

    def is_waiting(self):
        """returns a boolean indicating whether this is an active task that has not begun execution"""
        return self.status in [EventTaskStatus.INITIALIZED, EventTaskStatus.WAITING]

    def attach_callback(self, func):
        """Attaches a callback function to be called upon task completion.
        Will be called with the task object as the first argument."""
        if not callable(func):
            raise ValueError("attempted to attach a non callable callback")
        if inspect.iscoroutinefunction(func):
            raise ValueError("callback must not be a coroutine")

        self._callbacks.append(func)

class EventTaskStatus(IntEnum):
    """enumeration of valid event task states"""
    CANCELLED = -1
    INITIALIZED = 1
    WAITING = 2
    EXEC_QUEUED = 3
    EXEC = 4
    DONE = 9999

    def __str__(self) -> str:
        return self.name
