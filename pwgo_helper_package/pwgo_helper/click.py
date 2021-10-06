"""custom click classes"""
from typing import List
import click

def required_for_commands(subcommands: List[str]):
    """decorator for defining a shared option that is only required
    for the given list of subcommands"""
    class Cls(click.Option):
        def __init__(self, *args, **kwargs) -> None:
            super(Cls, self).__init__(*args, **kwargs)

        def handle_parse_result(self, ctx, opts, args):
            self.required = ctx.invoked_subcommand and ctx.invoked_subcommand in subcommands
            return super().handle_parse_result(ctx, opts, args)
    return Cls
