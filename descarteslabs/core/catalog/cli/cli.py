# Copyright 2018-2024 Descartes Labs.

import click

from .products import products
from .bands import bands
from .blobs import blobs


@click.group()
def cli():
    pass


cli.add_command(products)
cli.add_command(blobs)
cli.add_command(bands)
