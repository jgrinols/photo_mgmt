"""wrapper module for ImageVirtualPathEventTask"""
from __future__ import annotations
import asyncio

from path import Path

from .database_event_row import ImageEventRow
from .event_task import EventTask
from ..config import Configuration as ProgramConfig
from .config import Configuration as AgentConfig
from .db_connection_pool import DbConnectionPool
from . import strings

class ImageVirtualPathEventTask(EventTask):
    """coordinates any tasks that should run when there is a new image virtual path
    in the piwigo database"""
    _pending_tasks: list[ImageVirtualPathEventTask] = []

    def __init__(self, event):
        super().__init__()
        self.logger = ImageVirtualPathEventTask.get_logger()
        self.event = event
        self._virt_path_task = None

    @staticmethod
    def get_logger():
        """gets a logger..."""
        return ProgramConfig.get().create_logger(__name__)

    @classmethod
    def get_pending_tasks(cls) -> list[ImageVirtualPathEventTask]:
        """list of outstanding virtual path tasks"""
        return cls._pending_tasks

    @classmethod
    def get_handled_tables(cls) -> list[str]:
        """list of tables handled by ImageVirtualPathEventTask"""
        return ["image_virtual_paths"]

    @classmethod
    def resolve_event_task(cls, evt: ImageEventRow) -> asyncio.Future:
        """this class doesn't require any complex resolution logic so we
        just create a new instance and set it as a result on a Future"""
        uppercats_str = evt.db_event_data["values"]["category_uppercats"]
        uppercats = [int(c.strip()) for c in uppercats_str.split(",")]
        vfs_cat_id = AgentConfig.get().virtualfs_category_id

        result_fut = asyncio.Future()
        if not vfs_cat_id or vfs_cat_id in uppercats:
            result_fut.set_result(ImageVirtualPathEventTask(evt))
        else:
            cls.get_logger().debug("%s is not a descendent of the virtualfs root category %s. skipping..."
                , evt.db_event_data["values"]["virtual_path"], str(vfs_cat_id))
            result_fut.set_result(False)

        return result_fut

    def schedule_start(self):
        """schedules execution of the image virtual path event handler on the event loop"""
        if not self.is_scheduled():
            loop = asyncio.get_event_loop()
            self._virt_path_task = loop.run_in_executor(None, self._handle_event)
            self.status = "EXEC_QUEUED"

    def _handle_event(self):
        self.status = "EXEC"

        if self.event.db_event_type == "INSERT":
            self.logger.info("handling image virtual path insert")
            with Path(AgentConfig.get().piwigo_galleries_host_path):
                src_path = Path(self.event.db_event_data["values"]["physical_path"]).abspath()
                self.logger.debug("resolved source file to %s", src_path)
                if not src_path.exists():
                    broken_msg = "%s does not exist"
                    if AgentConfig.get().virtualfs_allow_broken_links:
                        self.logger.warning(broken_msg, src_path)
                    else:
                        raise FileNotFoundError(broken_msg % src_path)
            with Path(AgentConfig.get().virtualfs_root):
                virt_path = Path(self.event.db_event_data["values"]["virtual_path"]).abspath()
                self.logger.debug("resolved virtual path to %s", virt_path)

            if not ProgramConfig.get().dry_run and not virt_path.exists():
                virt_path.dirname().makedirs_p()
                src_path.symlink(virt_path)

        elif self.event.db_event_type == "DELETE":
            self.logger.info("handling image virtual path delete")
            with Path(AgentConfig.get().virtualfs_root):
                virt_path = Path(self.event.db_event_data["values"]["virtual_path"]).abspath()
                self.logger.debug("resolved existing virtual path to %s", virt_path)
            ImageVirtualPathEventTask._remove_path(virt_path)

        self.status = "DONE"

    async def _execute_task(self):
        res = await self._virt_path_task
        return res

    @classmethod
    def _remove_path(cls, target: Path):
        logger = cls.get_logger()
        if not AgentConfig.get().virtualfs_remove_empty_dirs:
            if not ProgramConfig.get().dry_run:
                if not target.isdir():
                    target.remove_p()
                else:
                    target.rmdir()
        else:
            parent_dir = target.parent
            if not ProgramConfig.get().dry_run:
                if not target.isdir():
                    target.remove_p()
                else:
                    target.rmdir()

            logger.debug("considering %s for removal...", parent_dir)
            is_root_dir = parent_dir.samefile(AgentConfig.get().virtualfs_root)
            logger.debug("is path the root destination path? %s", is_root_dir)
            is_empty = len(parent_dir.listdir()) == 0
            logger.debug("is path empty? %s", is_empty)
            remove_parent = not is_root_dir and is_empty
            if remove_parent:
                logger.debug("removing %s", parent_dir)
            if not ProgramConfig.get().dry_run and remove_parent:
                cls._remove_path(parent_dir)

    @classmethod
    async def rebuild_virtualfs(cls):
        """deletes everything in the virtualfs root directory and recreates
        everything from the piwigo db"""
        # todo: rebuild the image_virtual_paths table--using existing script
        # can be referenced from program config db_scripts_path
        logger = cls.get_logger()
        async with DbConnectionPool.get().acquire_dict_cursor(db=AgentConfig.get().pwgo_db_config["name"]) as (cur,_):
            vfs_root = Path(AgentConfig.get().virtualfs_root)
            logger.debug("retrieving all image virtual paths from db")
            await cur.execute("SELECT * FROM image_virtual_paths")
            v_path_rows = await cur.fetchall()

            logger.debug(strings.LOG_VFS_REBUILD_REMOVE(vfs_root))
            if not ProgramConfig.get().dry_run:
                for file in vfs_root.files():
                    file.remove()
                for directory in vfs_root.dirs():
                    directory.rmtree()

            logger.debug(strings.LOG_VFS_REBUILD_CREATE(len(v_path_rows)))
            for row in v_path_rows:
                with Path(AgentConfig.get().piwigo_galleries_host_path):
                    src_path = Path(row["physical_path"]).abspath()
                    if not src_path.exists():
                        broken_msg = "%s does not exist"
                        if AgentConfig.get().virtualfs_allow_broken_links:
                            logger.warning(broken_msg, src_path)
                        else:
                            raise FileNotFoundError(broken_msg % src_path)
                with Path(AgentConfig.get().virtualfs_root):
                    virt_path = Path(row["virtual_path"]).abspath()

                if not ProgramConfig.get().dry_run and not virt_path.exists():
                    virt_path.dirname().makedirs_p()
                    src_path.symlink(virt_path)
