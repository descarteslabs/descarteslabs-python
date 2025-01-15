# © 2025 EarthDaily Analytics Corp.

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
