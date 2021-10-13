"""container module for Configuration"""
from __future__ import annotations

from pathlib import Path

from ..db_connection_pool import DbConnectionPool
from ..config import Configuration as ProgramConfig

class Configuration():
    """Contains program constants, read-only, and configuration values"""
    instance: Configuration = None

    def __init__(self):
        self.pwgo_gallery_virt_path = Path("/config/www/gallery")
        self.event_tables = { f"{ProgramConfig.get().msg_db_name}.pwgo_message": {} }
        self.auto_tag_alb = 125
        self.auto_tag_proc_alb = 126
        self.face_idx_parent_alb = 128
        self.face_idx_albs = []
        self.min_tag_confidence = 90
        self.img_tag_wait_secs = 1
        self.stop_timeout = 10
        self.scaled_img_max_size = (1024,1024)

        # set by initialization
        self.piwigo_galleries_host_path = None
        self.rek_cfg = None
        self.image_crop_save_path = None
        self.virtualfs_root = None
        self.virtualfs_allow_broken_links = True
        self.debug = False
        self.workers = None
        self.worker_error_limit = None
        self.virtualfs_remove_empty_dirs = True
        self.initialization_args = None
        self.virtualfs_category_id = 0

    @staticmethod
    def get() -> Configuration:
        """returns the Configuration singleton"""
        if not Configuration.instance:
            def_cfg = Configuration()
            ProgramConfig.get().get_logger(__name__).warning("Agent config is not initialized. Returning default config.")
            return def_cfg
        return Configuration.instance

    @staticmethod
    async def initialize(**kwargs) -> Configuration:
        """Initialize program configuration values"""
        cfg = Configuration()
        cfg.initialization_args = kwargs
        cfg.rek_cfg = {
            "aws_access_key_id": kwargs["rek_access_key"],
            "aws_secret_access_key": kwargs["rek_secret_access_key"],
            "region_name": kwargs["rek_region"],
            "default_collection_arn": kwargs["rek_collection_arn"],
            "collection_id": kwargs["rek_collection_id"]
        }

        for key, val in kwargs.items():
            if hasattr(cfg, key):
                setattr(cfg, key, val)

        # pylint: disable=protected-access
        await cfg._set_face_index_categories()
        Configuration.instance = cfg
        return cfg

    async def _set_face_index_categories(self):
        pcfg = ProgramConfig.get()
        async with DbConnectionPool.get().acquire_dict_cursor(db=pcfg.pwgo_db_name) as (cur, _):
            sql = f"""
                SELECT c.id
                FROM {pcfg.pwgo_db_name}.categories c
                WHERE c.id_uppercat = %s
                    AND c.name NOT LIKE '%s'
            """

            await cur.execute(sql % (self.face_idx_parent_alb, ".%"))
            cats = []
            for row in await cur.fetchall():
                cats.append(row["id"])

        self.face_idx_albs = cats
