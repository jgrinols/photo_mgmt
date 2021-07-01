"""the main pwgo_helper command entry point"""
import click

from .config import Configuration
from .agent.metadata_agent import agent_entry

@click.group()
@click.option(
    "-v", "--verbosity",
    help="specifies the verbosity of the log output",
    type=click.Choice(["CRITICAL","ERROR","WARNING","INFO","DEBUG"]),
    default="INFO"
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
    logger = Configuration.get().create_logger(__name__)
    logger.debug("Execeuting pwgo_helper command")

def pwgo_helper_entry():
    """console script entry point"""
    pwgo_helper(auto_envvar_prefix="PWGO_HLPR")

pwgo_helper.add_command(agent_entry)
