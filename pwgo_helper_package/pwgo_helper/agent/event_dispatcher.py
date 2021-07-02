"""Container module for EventDispatcher class"""
import asyncio
from asyncio.futures import Future
from asyncio.exceptions import CancelledError, InvalidStateError
from time import perf_counter

from . import strings
from ..config import Configuration as ProgramConfig
from .config import Configuration as AgentConfig
from .autotagger import AutoTagger
from .database_event_row import DatabaseEventRow
from .aggregate_results_error import AggregateResultsError

from .event_task import EventTask

class EventDispatcher():
    """Contains the logic for managing events captured from the Piwigo database."""
    @staticmethod
    def get_logger():
        """gets a logger..."""
        return ProgramConfig.get().create_logger(__name__)

    def __init__(self, worker_cnt, error_limit):
        self.logger = EventDispatcher.get_logger()
        self._evt_queue = asyncio.Queue()
        self._delay_dispatch_task = Future()
        self._delay_dispatch_task.set_result(True)
        self._error_limit = error_limit
        self._error_cnt = 0
        self._stopping_task = None
        self.workers = [None] * worker_cnt
        self.results = None
        self.state = "INIT"

    @classmethod
    async def create(cls, worker_cnt, error_limit = 0):
        """creates an EventDispatcher instance"""
        try:
            worker_cnt_val = int(worker_cnt)
            if worker_cnt_val <= 0:
                raise ValueError()
        except ValueError as exc:
            raise ValueError("worker count must be a positive integer") from exc

        evt_dispatcher = EventDispatcher(worker_cnt, error_limit)
        cls.get_logger().debug("EventDispatcher: starting")
        await evt_dispatcher.start()

        return evt_dispatcher

    async def queue_event(self, raw_evt_row):
        """adds the given event to the dispatcher's event queue"""
        if self.state != "RUNNING":
            msg = "dispatcher is not running...cannot queue event"
            if self.state == "STOPPING":
                msg = "dispatcher is stopping...cannot queue event"
            elif self.state == "STOPPED":
                msg = "dispatcher is stopped...cannot queue event"
            raise InvalidStateError(msg)

        self.logger.debug(strings.LOG_QUEUE_EVT)
        resolved_evt = DatabaseEventRow.from_json(raw_evt_row["values"]["message_type"],
            raw_evt_row["values"]["message"])
        await self._evt_queue.put(resolved_evt)
        await asyncio.sleep(0)

    async def start(self):
        """bind workers to event queue"""
        if self.state != "INIT":
            raise InvalidStateError("start can only be called on an uninitialized dispatcher")
        for worker in self.workers:
            if worker:
                raise InvalidStateError("start can only be called on an uninitialized dispatcher")
            await self._add_worker()
        self.state = "RUNNING"

    async def _add_worker(self):
        use_index = None
        for index,worker in enumerate(self.workers):
            if not worker:
                use_index = index
                break

        if use_index is None:
            use_index = len(self.workers)
            self.workers.append(None)

        worker_name = f"worker-{use_index}"
        self.logger.debug("EventDispatcher: starting worker %s", worker_name)
        worker = asyncio.create_task(self._worker())
        await asyncio.sleep(0)
        worker.set_name(worker_name)
        self.workers[use_index] = worker

    async def stop(self, force=False):
        """Cancels the queue worker tasks and cleans up any other resources"""
        self.logger.debug("EventDispatcher: stopping event dispatcher (forced: %s)", force)
        if force:
            sig = "STOP"
        else:
            sig = "CLEAR_QUEUE"

        if not self._stopping_task or force:
            # if this is a forced stop we set the STOP signal
            # even if we're already stopping just in case the
            # previous stop call was unforced--we allow the
            # stop to be escalated, but not deescalated
            for worker in [ w for w in self.workers if not w.done() ]:
                worker.signal = sig
        if not self._stopping_task:
            self._stopping_task = asyncio.create_task(self._stop())

        self.state = "STOPPING"
        results = await self._stopping_task
        self.state = "STOPPED"
        return results

    async def _stop(self):
        for worker in [ w for w in self.workers if w.worker_status == "WAITING" ]:
            self.logger.debug("EventDispatcher: cancelling worker %s", worker.get_name())
            worker.cancel()

        #return exceptions prevents an exception or cancellation from causing await to return early
        self.logger.debug("EventDispatcher: waiting for all workers to complete")
        self.results = await asyncio.gather(*self.workers, return_exceptions=True)
        self.logger.debug("EventDispatcher: all workers completed")
        return self.results

    def get_results(self):
        """gets a dictionary containing results for all workers. Raises any exception results"""
        results = []
        exceptions = []
        for worker in self.workers:
            try:
                result = worker.result()
            except CancelledError:
                result = "CANCELLED"
            #pylint: disable=broad-except
            except Exception as err:
                exceptions.append({
                    "worker_name": worker.get_name(),
                    "exception": err
                })
                continue

            results.append({
                "worker_name": worker.get_name(),
                "result": result
            })

        if exceptions:
            raise AggregateResultsError(results, exceptions, strings.LOG_WORKER_ERRORS(len(exceptions)))
        else:
            return results

    async def process_event(self, evt: DatabaseEventRow):
        '''process a single, queued event'''
        if not isinstance(evt, DatabaseEventRow):
            raise TypeError("evt must be an instance of DatabaseEvent")

        task = asyncio.tasks.current_task()
        self.logger.debug("%s entering process_event", task.get_name())

        if not self._delay_dispatch_task.done():
            self.logger.debug("%s: waiting for delay dispatch task to complete", task.get_name())
        await self._delay_dispatch_task
        self.logger.debug("%s: proceeding to process event", task.get_name())

        if evt.table_name == "image_category" and evt.db_event_type in ["INSERT","DELETE"]:
            if evt.table_primary_key[1] in AgentConfig.get().face_idx_albs:
                self.logger.debug("%s: Handling face index change--setting delay dispatch task", task.get_name())
                self._delay_dispatch_task = Future()
                await AutoTagger.sync_face_index()
                self.logger.debug("%s: Face index sync complete--setting delay dispatch task completion", task.get_name())
                self._delay_dispatch_task.set_result(True)

        evt_handler = await EventTask.get_event_task(evt)
        if evt_handler:
            self.logger.debug("%s: Scheduling event handler %s", task.get_name(), evt_handler)
            # 
            evt_handler.schedule_start()
            result = await evt_handler
            return result
        else:
            return False

    async def _worker(self):
        task = asyncio.tasks.current_task()
        task.signal = "SERVICE_QUEUE"
        task.worker_status = "INIT"
        proceed = lambda t: t.signal == "SERVICE_QUEUE" or (t.signal == "CLEAR_QUEUE" and not self._evt_queue.empty())
        while proceed(task):
            task.worker_status = "WAITING"
            evt = await self._evt_queue.get()
            try:
                task.worker_status = "DISPATCHED"
                self.logger.debug("%s: processing new event", task.get_name())
                beg = perf_counter()
                await self.process_event(evt)
                end = perf_counter()
                self._evt_queue.task_done()
                self.logger.debug("%s: processed event in %s", task.get_name(), end-beg)

            #pylint: disable=broad-except
            except Exception as error:
                self._error_cnt += 1
                self.logger.exception("%s encounter an error", task.get_name())
                # handle case where we've exceeded error limit
                if self._error_cnt >= self._error_limit:
                    asyncio.create_task(self.stop(force=True))
                    await asyncio.sleep(0)
                elif proceed(task):
                    # spawn a new worker task
                    asyncio.create_task(self._add_worker())
                    await asyncio.sleep(0)
                raise error

        task.worker_status = "KILLED"
