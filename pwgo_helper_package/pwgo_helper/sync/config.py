"""container module for Configuration"""
from __future__ import annotations

import click

from ..config import Configuration as ProgramConfig

class Configuration():
    """Contains program constants, read-only, and configuration values"""
    instance: Configuration = None
    privacy_level_map = {
        "All": 0, "Contacts": 1, "Friends": 2, "Family": 4, "Admins": 8
    }

    def __init__(self):
        self.login_path = "ws.php"
        self.sync_path = "admin.php"

        self.initialization_args: dict = None
        self.base_url: str = None
        self.user: str = None
        self.password: str = None
        self.sync_album_id: int = None
        self.skip_metadata: bool = False
        self.privacy_level: int = 8
        self.directories_only: bool = False
        self.add_to_caddie: bool = False

    @staticmethod
    def get() -> Configuration:
        """returns the Configuration singleton"""
        if not Configuration.instance:
            def_cfg = Configuration()
            ProgramConfig.get().get_logger(__name__).warning("sync config is not initialized. Returning default config.")
            return def_cfg
        return Configuration.instance

    @staticmethod
    def initialize(**kwargs):
        """Initialize configuration values"""
        logger = ProgramConfig.get().get_logger(__name__)
        cfg = Configuration()
        cfg.initialization_args = kwargs

        for key, val in kwargs.items():
            if key == "file_access_level":
                cfg.privacy_level = Configuration.privacy_level_map[val]
            if hasattr(cfg, key):
                setattr(cfg, key, val)

        click_ctx = click.get_current_context()
        # only log init parameters if we have a click context
        # so we don't risk logging sensitive data
        for key,val in kwargs.items():
            show_val = val
            if click_ctx:
                opt = [opt for opt in click_ctx.command.params if opt.name == key]
                if opt and opt[0].hide_input:
                    show_val = "OMITTED"
            else:
                show_val = "OMITTED"
            logger.debug("initializing program config with %s=%s", key, show_val)

        Configuration.instance = cfg
