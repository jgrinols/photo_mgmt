"""container module for Configuration"""
from __future__ import annotations

from pathlib import Path

from .db_connection_pool import DbConnectionPool
from ..config import Configuration as ProgramConfig

class Configuration():
    """Contains program constants, read-only, and configuration values"""
    instance: Configuration = None

    def __init__(self):
        self.pwgo_gallery_virt_path = Path("/config/www/gallery")
        self.event_tables = { "pwgo_message": {} }
        self.auto_tag_alb = 125
        self.auto_tag_proc_alb = 126
        self.face_idx_parent_alb = 128
        self.face_idx_albs = []

        self.img_tag_wait_secs = 1

        self.msg_db = "messaging"

        self.stop_timeout = 10

        # set by initialization
        self.piwigo_galleries_host_path = None
        self.rek_db_config = None
        self.pwgo_db_config = None
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
            ProgramConfig.get().create_logger(__name__).warning("Agent config is not initialized. Returning default config.")
            return def_cfg
        return Configuration.instance

    @staticmethod
    async def initialize(**kwargs):
        """Initialize program configuration values"""
        cfg = Configuration()
        cfg.initialization_args = kwargs
        cfg.pwgo_db_config = {
            "host": kwargs["pwgo_db_host"],
            "port": kwargs["pwgo_db_port"],
            "user": kwargs["pwgo_db_user"],
            "passwd": kwargs["pwgo_db_pw"],
            "name": kwargs["pwgo_db_name"]
        }
        cfg.rek_cfg = {
            "aws_access_key_id": kwargs["rek_access_key"],
            "aws_secret_access_key": kwargs["rek_secret_access_key"],
            "region_name": kwargs["rek_region"],
            "default_collection_arn": kwargs["rek_collection_arn"],
            "collection_id": kwargs["rek_collection_id"]
        }
        # take the pwgo database params as fallbacks
        cfg.rek_db_config = cfg.pwgo_db_config.copy()
        if "rek_db_host" in kwargs:
            cfg.rek_db_config["host"] = kwargs["rek_db_host"]
        if "rek_db_port" in kwargs:
            cfg.rek_db_config["port"] = kwargs["rek_db_port"]
        if "rek_db_user" in kwargs:
            cfg.rek_db_config["user"] = kwargs["rek_db_user"]
        if "rek_db_pw" in kwargs:
            cfg.rek_db_config["passwd"] = kwargs["rek_db_pw"]
        if "rek_db_name" in kwargs:
            cfg.rek_db_config["name"] = kwargs["rek_db_name"]

        for key, val in kwargs.items():
            if hasattr(cfg, key):
                setattr(cfg, key, val)

        # pylint: disable=protected-access
        await cfg._set_face_index_categories()
        Configuration.instance = cfg

    async def _set_face_index_categories(self):
        async with DbConnectionPool.get().acquire_dict_cursor(db=self.pwgo_db_config["name"]) as (cur, _):
            sql = """
                SELECT c.id
                FROM piwigo.categories c
                WHERE c.id_uppercat = %s
                    AND c.name NOT LIKE '%s'
            """

            await cur.execute(sql % (self.face_idx_parent_alb, ".%"))
            cats = []
            for row in await cur.fetchall():
                cats.append(row["id"])

        self.face_idx_albs = cats
