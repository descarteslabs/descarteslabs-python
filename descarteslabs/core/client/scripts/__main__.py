#!/usr/bin/env python
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

import sys

import click

try:
    from descarteslabs.core.client.scripts.cli import cli
except ImportError:
    # run from monorepo, somewhat unusual
    from descarteslabs.client.scripts.cli import cli


def main():
    try:
        cli()
    except Exception as e:
        click.echo(f"{e.__class__.__name__}: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
