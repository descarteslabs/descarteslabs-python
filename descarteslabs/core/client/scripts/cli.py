#!/usr/bin/env python
# Copyright 2018-2023 Descartes Labs.
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

# flake8: noqa


import argparse
from ..auth.cli import auth_handler
from ..services.raster.cli import scales, raster_handler
from ..version import __version__


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="group")

# Auth Group
auth_parser = subparsers.add_parser("auth")
auth_parser.add_argument(
    "command",
    choices=["login", "token", "name", "groups", "payload", "env", "version"],
    help="The action to take (e.g. login, etc.)",
)

# Raster Group
raster_parser = subparsers.add_parser("raster")
raster_parser.add_argument("inputs", type=str, nargs="+")
raster_parser.add_argument("-bands", nargs="+", default=None, type=str)
raster_parser.add_argument("-scales", default=None, type=scales, nargs="+")
raster_parser.add_argument("-resolution", default=240, type=float)
raster_parser.add_argument("-output_format", default="GTiff", type=str)
raster_parser.add_argument("-data_type", default="UInt16", type=str)
raster_parser.add_argument("-srs", default=None, type=str)
raster_parser.add_argument("-place", default=None, type=str)
raster_parser.add_argument("-outfile_basename", default=None, type=str)

# Version
auth_parser = subparsers.add_parser("version")


def handle(args):
    if args.group == "auth":
        auth_handler(args)
    elif args.group == "raster":
        raster_handler(args)
    elif args.group == "version":
        print(__version__)
    else:
        print("invalid command: choose from 'auth', 'raster', 'version'")


if __name__ == "__main__":
    handle(parser.parse_args())
