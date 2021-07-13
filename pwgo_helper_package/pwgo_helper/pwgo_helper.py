"""the main pwgo_helper command entry point"""
import click

from .config import Configuration
from .agent.metadata_agent import agent_entry

@click.group()
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
