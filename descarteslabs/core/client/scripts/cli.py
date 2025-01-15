# © 2025 EarthDaily Analytics Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from descarteslabs.config import get_settings, select_env
from ..auth.cli import cli as auth_cli
from .. import version as version_module
from .lazy_group import LazyGroup

import click

# Hack to determine where the client root is installed
MODULE_PREFIX = version_module.__package__.rsplit(".", 1)[0]


# Use a lazy group to avoid loading these large packages until they are needed
@click.group(
    cls=LazyGroup,
    lazy_subcommands={
        "catalog": f"{MODULE_PREFIX}.catalog.cli.cli",
        # "compute": f"{MODULE_PREFIX}.compute.scripts.cli",
        # "vector": f"{MODULE_PREFIX}.vector.scripts.cli",
    },
    help="Descartes Labs command-line interface",
)
@click.option(
    "--env", help="The environment to use", envvar="DESCARTESLABS_ENV", default=None
)
@click.pass_context
def cli(ctx, env):
    if env:
        select_env(env)
    ctx.obj = get_settings()


cli.add_command(auth_cli, name="auth")


@cli.command()
def version():
    """Print the version of the CLI"""
    click.echo(version_module.__version__)


@cli.command()
@click.pass_context
def env(ctx):
    """Print the version of the CLI"""
    click.echo(ctx.obj.env)
