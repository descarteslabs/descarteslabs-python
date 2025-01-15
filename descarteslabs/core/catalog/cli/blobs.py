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

from .. import Blob, StorageType, properties as p

from .utils import serialize


@click.group()
def blobs():
    """Blob management."""
    pass


@blobs.command()
@click.option("--ids", type=str, multiple=True)
@click.option("--storage-type", type=click.Choice([st.value for st in StorageType]))
@click.option("--namespace", type=str)
@click.option("--prefix", type=str, help="Name prefix")
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
    ids,
    storage_type,
    namespace,
    prefix,
    access_id,
    owners,
    writers,
    readers,
    tags,
    limit,
    output,
):
    """List blobs."""
    search = Blob.search()
    if ids:
        search = search.filter(p.id.any_of(ids))
    if storage_type is not None:
        search = search.filter(p.storage_type == storage_type)
    if namespace is not None:
        search = search.filter(p.namespace == namespace)
    if prefix is not None:
        search = search.filter(p.name.startswith(prefix))
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
        click.echo(json.dumps([serialize(blob) for blob in search]))
    else:
        for blob in search:
            if output == "id":
                click.echo(blob.id)
            else:
                click.echo(blob)


@blobs.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def add_owner(id, subject):
    """Add an owner to a blob."""
    blob = Blob.get(id)
    if not blob:
        raise click.BadParameter(f"blob {id} not found")
    if subject in blob.owners:
        click.echo(f"{subject} is already an owner of {id}")
    else:
        blob.owners.append(subject)
        blob.save()
        click.echo(f"Added {subject} as an owner of {id}")


@blobs.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def remove_owner(id, subject):
    """Remove an owner from a blob."""
    blob = Blob.get(id)
    if not blob:
        raise click.BadParameter(f"blob {id} not found")
    if subject not in blob.owners:
        raise click.BadParameter(f"{subject} is not an owner of {id}")
    blob.owners.remove(subject)
    blob.save()
    click.echo(f"Removed {subject} as an owner of {id}")


@blobs.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def add_writer(id, subject):
    """Add a writer to a blob."""
    blob = Blob.get(id)
    if not blob:
        raise click.BadParameter(f"blob {id} not found")
    if subject in blob.writers:
        click.echo(f"{subject} is already a writer of {id}")
    else:
        blob.writers.append(subject)
        blob.save()
        click.echo(f"Added {subject} as a writer of {id}")


@blobs.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def remove_writer(id, subject):
    """Remove a writer from a blob."""
    blob = Blob.get(id)
    if not blob:
        raise click.BadParameter(f"blob {id} not found")
    if subject not in blob.writers:
        raise click.BadParameter(f"{subject} is not a writer of {id}")
    blob.writers.remove(subject)
    blob.save()
    click.echo(f"Removed {subject} as a writer of {id}")


@blobs.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def add_reader(id, subject):
    """Add a reader to a blob."""
    blob = Blob.get(id)
    if not blob:
        raise click.BadParameter(f"blob {id} not found")
    if subject in blob.reader:
        click.echo(f"{subject} is already a reader of {id}")
    else:
        blob.readers.append(subject)
        blob.save()
        click.echo(f"Added {subject} as a reader of {id}")


@blobs.command()
@click.argument("id", type=str)
@click.argument("subject", type=str)
def remove_reader(id, subject):
    """Remove a reader from a blob."""
    blob = Blob.get(id)
    if not blob:
        raise click.BadParameter(f"blob {id} not found")
    if subject not in blob.readers:
        raise click.BadParameter(f"{subject} is not a reader of {id}")
    blob.readers.remove(subject)
    blob.save()
    click.echo(f"Removed {subject} as a reader of {id}")
