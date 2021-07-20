"""the main pwgo_helper command entry point"""
import os, uuid

import click
from dotenv import load_dotenv

from .config import Configuration
from .agent.metadata_agent import agent_entry
from .icloud_dl.base import main

MODULE_BASE_PATH = os.path.dirname(__file__)

def _load_environment(_ctx, _opt, val):
    logger = Configuration.get().get_logger(str(uuid.uuid4())[0:8])
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
    "--db-host",help="hostname for piwigo db connection",type=str,required=True
)
@click.option(
    "--db-port",help="port for piwigo db connection",type=int,required=False,default=3306
)
@click.option(
    "--db-user",help="username for piwigo db connection",type=str,required=True
)
@click.option(
    "--db-pw",help="password for piwigo db connection",type=str,required=True,hide_input=True
)
@click.option(
    "--pwgo-db-name",help="name of the piwigo database",type=str,required=False,default="piwigo"
)
@click.option(
    "-v", "--verbosity",
    help="specifies the verbosity of the log output",
    type=click.Choice(["CRITICAL","ERROR","WARNING","INFO","DEBUG","NOTSET"]),
    default="NOTSET"
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
