import json

import click

from .. import Product, properties as p

from .utils import serialize


@click.group()
def products():
    """Product management."""
    pass


@products.command()
@click.option("--ids", type=str, multiple=True)
@click.option("--core", "is_core", flag_value=True)
@click.option("--user", "is_core", flag_value=False)
@click.option("--all", "is_core", flag_value=None, type=bool)
@click.option("--product-tier", type=click.Choice(("standard", "premium")))
@click.option("--access-id", type=str, help="Administrative users only")
@click.option("--owners", type=str, multiple=True)
@click.option("--writers", type=str, multiple=True, help="Administrative users only")
@click.option("--readers", type=str, multiple=True, help="Administrative users only")
@click.option("--tags", type=str, multiple=True)
@click.option("--limit", type=int)
@click.option(
    "--output",
    type=click.Choice(("id", "short", "json")),
    default="short",
    help="Output format",
)
def list(
    ids, is_core, product_tier, access_id, owners, writers, readers, tags, limit, output
):
    """List products."""
    search = Product.search()
    if ids:
        search = search.filter(p.id.any_of(ids))
    if is_core is not None:
        search = search.filter(p.is_core == is_core)
    if product_tier is not None:
        search = search.filter(p.product_tier == product_tier)
    if access_id:
        search = search.filter(p.access_id == access_id)
    if owners:
        search = search.filter(p.owners.any_of(owners))
    if writers:
        search = search.filter(p.writers.any_of(writers))
    if readers:
        search = search.filter(p.readers.any_of(readers))
    if tags:
        search = search.filter(p.tags.any_of(tags))
    if limit:
        search = search.limit(limit)
    if output == "json":
        click.echo(json.dumps([serialize(product) for product in search]))
    else:
        for product in search:
            if output == "id":
                click.echo(product.id)
            else:
                click.echo(product)


@products.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def add_owner(id, subject):
    """Add an owner to a product."""
    product = Product.get(id)
    if not product:
        raise click.BadParameter(f"Product {id} not found")
    if subject in product.owners:
        click.echo(f"{subject} is already an owner of {id}")
    else:
        product.owners.append(subject)
        product.save()
        click.echo(f"Added {subject} as an owner of {id}")


@products.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def remove_owner(id, subject):
    """Remove an owner from a product."""
    product = Product.get(id)
    if not product:
        raise click.BadParameter(f"Product {id} not found")
    if subject not in product.owners:
        raise click.BadParameter(f"{subject} is not an owner of {id}")
    product.owners.remove(subject)
    product.save()
    click.echo(f"Removed {subject} as an owner of {id}")


@products.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def add_writer(id, subject):
    """Add a writer to a product."""
    product = Product.get(id)
    if not product:
        raise click.BadParameter(f"Product {id} not found")
    if subject in product.writers:
        click.echo(f"{subject} is already an writer of {id}")
    else:
        product.writers.append(subject)
        product.save()
        click.echo(f"Added {subject} as an writer of {id}")


@products.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def remove_writer(id, subject):
    """Remove a writer from a product."""
    product = Product.get(id)
    if not product:
        raise click.BadParameter(f"Product {id} not found")
    if subject not in product.writers:
        raise click.BadParameter(f"{subject} is not a writer of {id}")
    product.writers.remove(subject)
    product.save()
    click.echo(f"Removed {subject} as a writer of {id}")


@products.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def add_reader(id, subject):
    """Add a reader to a product."""
    product = Product.get(id)
    if not product:
        raise click.BadParameter(f"Product {id} not found")
    if subject in product.reader:
        click.echo(f"{subject} is already a reader of {id}")
    else:
        product.readers.append(subject)
        product.save()
        click.echo(f"Added {subject} as a reader of {id}")


@products.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def remove_reader(id, subject):
    """Remove a reader from a product."""
    product = Product.get(id)
    if not product:
        raise click.BadParameter(f"Product {id} not found")
    if subject not in product.readers:
        raise click.BadParameter(f"{subject} is not a reader of {id}")
    product.readers.remove(subject)
    product.save()
    click.echo(f"Removed {subject} as a reader of {id}")
