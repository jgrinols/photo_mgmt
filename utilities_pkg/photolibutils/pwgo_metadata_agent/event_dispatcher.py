"""Container module for EventDispatcher class"""
import logging, asyncio, json
from threading import Event
from time import perf_counter

import click_log
from pymysqlreplication.row_event import WriteRowsEvent
from py_linq import Enumerable

from photolibutils.pwgo_metadata_agent.constants import Constants
from photolibutils.pwgo_metadata_agent.autotagger import AutoTagger
from photolibutils.pwgo_metadata_agent.utilities import NoopTask
from photolibutils.pwgo_metadata_agent.image_tag_event_task import ImageTagEventTask

logger = logging.getLogger(__name__)
click_log.basic_config(logger)

class EventDispatcher():
    """Contains the logic for managing events captured from the Piwigo database."""
    def __init__(self, evt_queue, worker_cnt):
        self._evt_queue = evt_queue
        self._allow_dispatch = Event()
        self._allow_dispatch.set()
        self._worker_count = worker_cnt
        self._face_index_msg_types = set([
            ("IMG_METADATA", "image_category", "INSERT"),
            ("IMG_METADATA", "image_category", "DELETE")
        ])
        self._handle_msg_types = set(self._face_index_msg_types).union([
            ("IMG_METADATA", "images", "INSERT"),
            ("IMG_METADATA", "images", "UPDATE"),
            ("IMG_METADATA", "image_category", "INSERT"),
            ("IMG_METADATA", "image_category", "DELETE"),
            ("IMG_METADATA", "image_tag", "INSERT"),
            ("IMG_METADATA", "image_tag", "DELETE"),
            ("IMG_METADATA", "tags", "INSERT"),
            ("IMG_METADATA", "tags", "UPDATE"),
            ("IMG_METADATA", "tags", "DELETE"),
        ])
        self._message_tbl_handlers = {
            "image_category": self._handle_msg_img_category,
            "images": self._handle_msg_imgs,
            "image_tag": self._handle_msg_img_tag,
            "tags": self._handle_msg_tags
        }
        self._pending_img_tag_tasks = {}
        self.workers = []
        self._worker_wrapping_task = None

    @staticmethod
    async def create(evt_queue, worker_cnt):
        """creates an EventDispatcher instance implemented as contextmanager"""
        try:
            worker_cnt_val = int(worker_cnt)
            if worker_cnt_val <= 0:
                raise ValueError()
        except ValueError as exc:
            raise ValueError("worker count must be a positive integer") from exc

        evt_dispatcher = EventDispatcher(evt_queue, worker_cnt)
        evt_dispatcher._worker_wrapping_task = asyncio.create_task(evt_dispatcher.start())
        await asyncio.sleep(0)

        return evt_dispatcher

    async def start(self):
        """bind workers to event queue"""
        for i in range(self._worker_count):
            worker = asyncio.create_task(self._worker())
            worker.name = f"worker-{i}"
            self.workers.append(worker)
        await asyncio.gather(*self.workers)

    async def stop(self):
        """Cancels the queue worker tasks and cleans up any other resources"""
        for worker in self.workers:
            worker.cancel()
            await asyncio.sleep(0)

    async def process_event(self, evt):
        '''populates the action queue with actions that should be executed based on the
        type of event, and the table involved:
            * if any images were added or removed from face index album, we queue up face index sync
            * if any images were added to autotag album, the we queue up auto tag process:
                1) face detection and matching
                2) relevant label detection
            * when tags are added, we add any tags that are triggered by the previously added ones'''
        if not isinstance(evt, WriteRowsEvent):
            raise TypeError("evt must be an instance of RowsEvent")

        task = asyncio.tasks.current_task()
        logger.debug("%s entering process_event", task.name)
        def get_parsed_msgs(row):
            return (row["values"]["message_type"], json.loads(row["values"]["message"]))
        msgs = list(map(get_parsed_msgs, evt.rows))

        await self._handle_face_index(msgs)

        # check if dispatching is open--if not, wait
        self._allow_dispatch.wait()

        evt_tasks = []
        for msg in msgs:
            msg_type = (msg[0],msg[1]["table_name"],msg[1]["operation"])
            if msg_type in self._handle_msg_types:
                _,m_tbl,m_op = msg_type

                # then we go through row level handling
                evt_tasks.append(self._message_tbl_handlers[m_tbl](m_op, msg[1]))

        await asyncio.wait(evt_tasks, return_when=asyncio.ALL_COMPLETED)

    async def handle_autotag_image(self, img_ref):
        """wraps the handling of an individual image to be autotagged"""
        task = asyncio.tasks.current_task()
        logger.debug("%s entering handle_autotag_image", task.name)
        async with AutoTagger.create(img_ref) as tagger:
            await tagger.autotag_image()

    async def _worker(self):
        task = asyncio.tasks.current_task()
        while True:
            evt = await self._evt_queue.get()
            logger.debug("%s processing new event", task.name)
            beg = perf_counter()
            await self.process_event(evt)
            end = perf_counter()
            self._evt_queue.task_done()
            logger.debug("worker %s processed event in %s", task.name, end-beg)

    async def _handle_face_index(self, msgs):
        task = asyncio.tasks.current_task()
        logger.debug("%s: entering _handle_face_index", task.name)
        # first determine if we need to run a face index sync
        # happens if there's an image added or removed from
        # a face sync album
        msg_types = [(m[0],m[1]['table_name'],m[1]['operation']) for m in msgs]
        # check if any rows are potential face sync event types
        if Enumerable(msg_types).intersect(Enumerable(self._face_index_msg_types), lambda x: x).any():
            # now have to check if any of these candidates actually affect a face index album
            alb_ids = { m[1]['table_primary_key'][1] for m in msgs }
            if Enumerable(alb_ids).intersect(Enumerable(Constants.FACE_IDX_ALBS), lambda x: x).any():
                # don't dispatch new events while we sync face index
                self._allow_dispatch.clear()
                await AutoTagger.sync_face_index()
                # open up dispatching for waiting and new events
                self._allow_dispatch.set()
                return

        logger.debug("%s: face index sync not needed.", task.name)

    def _handle_msg_img_category(self, oper, msg):
        task = asyncio.tasks.current_task()
        logger.debug("%s entering _handle_msg_img_category", task.name)
        # queue up autotag job for any images that were added to autotag album
        if oper == "INSERT" and msg["table_primary_key"][1] == Constants.AUTO_TAG_ALB:
            img_id = msg["image_id"]
            task = asyncio.create_task(self.handle_autotag_image(img_id))
            task.image_id = img_id
            return task
        return NoopTask()

    def _handle_msg_imgs(self, oper, msg):
        task = asyncio.tasks.current_task()
        logger.debug("%s entering _handle_msg_imgs", task.name)
        logger.debug("recieved metadata update message for image %s", msg["image_id"])

    async def _handle_msg_img_tag(self, oper, msg):
        logger.debug("entering _handle_msg_img_tag")
        img_id = msg["image_id"]
        tag_id = msg["table_primary_key"][1]

        def task_done_callback(_tag_task):
            del self._pending_img_tag_tasks[img_id]

        img_tag_task = next((t for t in self._pending_img_tag_tasks if t.image_id == img_id), None)
        if img_tag_task:
            if img_tag_task.mutable():
                if oper == "INSERT":
                    img_tag_task.add_tag(tag_id)
                elif oper == "DELETE":
                    img_tag_task.remove_tag(tag_id)
                else:
                    raise ValueError(f"unrecognized operation {oper}")

                return
            else:
                await img_tag_task

        img_tag_task = ImageTagEventTask(img_id, Constants.IMG_TAG_WAIT_SECS)
        self._pending_img_tag_tasks[img_id] = img_tag_task
        img_tag_task.attach_callback(task_done_callback)
        img_tag_task.add_tagging_event(img_id, oper)

    def _handle_msg_tags(self, oper, msg):
        task = asyncio.tasks.current_task()
        logger.debug("%s entering _handle_msg_tags", task.name)
        return asyncio.create_task(AutoTagger.process_new_tag(msg["tag_id"]))

    def __await__(self):
        if not self._worker_wrapping_task:
            raise RuntimeError("awaited an initialized event dispatcher")

        if not self._worker_wrapping_task.done():
            yield from self._worker_wrapping_task
