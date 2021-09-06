"""container module for icloud downloader configuration"""
from __future__ import annotations

import click

from ..config import Configuration as ProgramConfig
from . import strings

class Configuration():
    """contains configuration options and constant for icloud-dl program"""
    instance: Configuration = None

    def __init__(self) -> None:
        self.initialization_args = None
        self.auth_msg_db = "Syslog"
        self.auth_msg_tbl = "SystemEvents"
        self.auth_msg_tag = "msg_queue"
        self.auth_phone_digits = None
        self.directory = None
        self.username = None
        self.password = None
        self.cookie_directory = None
        self.size = "original"
        self.recent = None
        self.until_found = None
        self.album = "All Photos"
        self.list_albums = False
        self.skip_videos = False
        self.force_size = False
        self.convert_heic = False
        self.auto_delete = False
        self.only_print_filenames = False
        self.folder_structure = "{:%Y/%m}"
        self.tracking_db = "icloudpd"
        self.max_retries = 5
        self.wait_seconds = 5
        self.mfa_timeout = 30
        self.lookback_days = None

    @staticmethod
    def get() -> Configuration:
        """returns the Configuration singleton"""
        if not Configuration.instance:
            def_cfg = Configuration()
            ProgramConfig.get().get_logger(__name__) \
                .warning("icloud-dl config is not initialized. Returning default config.")
            return def_cfg
        return Configuration.instance

    @staticmethod
    def initialize(**kwargs):
        """Initialize program configuration values"""
        prg_cfg = ProgramConfig.get()
        cfg = Configuration()
        cfg.initialization_args = kwargs

        for key, val in kwargs.items():
            if hasattr(cfg, key):
                setattr(cfg, key, val)

        click_ctx = click.get_current_context()
        # only log init parameters if we have a click context
        # so we don't risk logging sensitive data
        if click_ctx:
            for key,val in kwargs.items():
                show_val = val
                opt = [opt for opt in click_ctx.command.params if opt.name == key]
                if opt and opt[0].hide_input:
                    show_val = "OMITTED"
                prg_cfg.get_logger(__name__).debug(strings.LOG_ICDL_OPT(key,show_val))

        if not cfg.list_albums and not cfg.directory:
            raise RuntimeError('--directory or --list-albums are required')

        Configuration.instance = cfg
