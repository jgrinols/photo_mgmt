"""the main pwgo_helper command entry point"""
import os, logging

import click
from dotenv import load_dotenv

from .config import Configuration
from .agent.metadata_agent import agent_entry
from .icloud_dl.base import main
from .sync.main import sync_entry
# pylint: disable=reimported
from . import logging as pwgo_logging

MODULE_BASE_PATH = os.path.dirname(__file__)
logging.setLoggerClass(pwgo_logging.CustomLogger)

def _load_environment(_ctx, _opt, val):
    # use the root logger since we're not initialized yet
    logger = Configuration.get().get_logger()
    if val:
        logger.info("attempting to load environment from %s", val)
        if os.path.exists(val):
            logger.info("file exists...loading environment...")
            load_dotenv(dotenv_path=val, verbose=True)
    return val

@click.group()
@click.option(
    "--env-file",help="optional env file to use to setup environment",
    type=click.Path(dir_okay=False, exists=True),
    callback=_load_environment, is_eager=True
)
@click.option(
    "--db-conn-json", help="json string representing the database server connection parameters",
    type=str, required=True, hide_input=True
)
@click.option(
    "--pwgo-db-name",help="name of the piwigo database",type=str,required=False,default="piwigo"
)
@click.option(
    "--log-level",
    help="specifies the verbosity of the log output",
    type=click.Choice(["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]),
    default="NOTSET"
)
@click.option(
    "--lib-log-level",
    help="specifies the verbosity of logging from standard library and third party modules",
    type=click.Choice(["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]),
    default="ERROR"
)
@click.option(
    "--dry-run",
    help="don't actually do anything--just pretend",
    is_flag=True
)
def pwgo_helper(
    **kwargs
):
    """the main pwgo_helper command entry point"""
    Configuration.initialize(**kwargs)
    logger = Configuration.get().get_logger(__name__)
    logger.debug("Execeuting pwgo_helper command")

def pwgo_helper_entry():
    """console script entry point"""
    pwgo_helper(auto_envvar_prefix="PWGO_HLPR")

pwgo_helper.add_command(agent_entry)
pwgo_helper.add_command(main)
pwgo_helper.add_command(sync_entry)
