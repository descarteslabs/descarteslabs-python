import json

import click

from .. import BandType, Product, properties as p

from .utils import serialize


@click.group()
def bands():
    """Band management."""
    pass


@bands.command()
@click.argument("product-id", type=str)
@click.option("--type", type=click.Choice([bt.value for bt in BandType]))
@click.option("--name", type=str)
@click.option("--tags", type=str, multiple=True)
@click.option("--limit", type=int)
@click.option(
    "--output",
    type=click.Choice(("id", "short", "json")),
    default="short",
    help="Output format",
)
def list(product_id, type, name, tags, limit, output):
    """List bands."""
    product = Product.get(product_id)
    if not product:
        raise click.BadParameter(f"Product {product_id} not found")
    search = product.bands()
    if product_id is not None:
        search = search.filter(p.product_id == product_id)
    if type is not None:
        search = search.filter(p.type == type)
    if name is not None:
        search = search.filter(p.name == name)
    if tags:
        search = search.filter(p.tags.any_of(tags))
    if limit:
        search = search.limit(limit)
    if output == "json":
        click.echo(json.dumps([serialize(band) for band in search]))
    else:
        for band in search:
            if output == "id":
                click.echo(band.id)
            else:
                click.echo(band)
