"""the main pwgo_helper command entry point"""
import os, logging

import click
from dotenv import load_dotenv
from pkg_resources import get_distribution

from .config import Configuration
from .agent.metadata_agent import agent_entry
from .icloud_dl.base import main
from .sync.main import sync_entry
# pylint: disable=reimported
from . import logging as pwgo_logging
from .click import required_for_commands

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

@click.group(context_settings=dict(auto_envvar_prefix="PWGO_HLPR"))
@click.option(
    "--env-file",help="The path to an environment file which will be loaded before other options are resolved",
    type=click.Path(dir_okay=False, exists=True),
    callback=_load_environment, is_eager=True
)
@click.option(
    "--db-conn-json", help="json string representing the database server connection parameters",
    type=str, hide_input=True,
    cls=required_for_commands(["agent", "icdownload", "sync"])
)
@click.option(
    "--pwgo-db-name",help="name of the piwigo database",type=str,required=False,default="piwigo"
)
@click.option(
    "--msg-db-name",help="name of the messaging database",type=str,required=False,default="messaging"
)
@click.option(
    "--rek-db-name",help="name of the rekognition database",type=str,required=False,default="rekognition"
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
    "--dry-run/--no-dry-run",
    help="don't actually do anything--just pretend",
    is_flag=True
)
def pwgo_helper(**kwargs):
    """The top-level command. Accepts parameters shared by all subcommands.

    All options can be set via environment variables. The name of the environment
    variable is the prefix ``PWGO_HLPR_`` followed by the parameter name in uppercase
    and with hyphens replaced by underscores. For instance, the parameter ``--log-level``
    can be set with an environment variable named ``PWGO_HLPR_LOG_LEVEL``.

    This is also true for subcommand parameters. In this case, the environment variable name
    includes the subcommand name. The ``--worker-error-limit`` option of the agent command
    could be set by creating an environment variable ``PWGO_HLPR_AGENT_WORKER_ERROR_LIMIT``.
    """
    Configuration.initialize(**kwargs)
    logger = Configuration.get().get_logger(__name__)
    logger.debug("Execeuting pwgo_helper command")

@click.command("version")
def version():
    """outputs the pwgo-helper version"""
    print(get_distribution("pwgo_helper").version)

pwgo_helper.add_command(version)
pwgo_helper.add_command(agent_entry)
pwgo_helper.add_command(main)
pwgo_helper.add_command(sync_entry)
