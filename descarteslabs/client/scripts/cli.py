#!/usr/bin/env python
# Copyright 2018-2020 Descartes Labs.
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
from descarteslabs.client.auth.cli import auth_handler
from descarteslabs.client.services.metadata.cli import metadata_handler
from descarteslabs.client.services.raster.cli import scales, raster_handler
from descarteslabs.client.services.places.cli import places_handler


parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest="group")

# Auth Group
auth_parser = subparsers.add_parser("auth")
auth_parser.add_argument(
    "command",
    choices=["login", "token", "name", "groups", "payload", "env", "version"],
    help="The action to take (e.g. login, etc.)",
)

# Metadata Group
metadata_parser = subparsers.add_parser("metadata")
metadata_parser.add_argument(
    "command", choices=["summary", "search", "keys", "get"], help="The action to take"
)
metadata_parser.add_argument("argument", nargs="?")
metadata_parser.add_argument("-url", help="The url of the service")
metadata_parser.add_argument("-place", help="A slug for a named place")
metadata_parser.add_argument(
    "-start_datetime", help="Start of valid date/time range (inclusive)"
)
metadata_parser.add_argument(
    "-end_datetime", help="End of valid date/time range (inclusive)"
)
metadata_parser.add_argument("-geom", help="Region of interest as GeoJSON or WKT")
metadata_parser.add_argument(
    "-params",
    help="JSON String of additional key/value pairs for searching properties: tile_id, cloud_fraction, etc.",
)  # NOQA
metadata_parser.add_argument(
    "-limit", default=100, help="Number of items to return (default 100)", type=int
)
metadata_parser.add_argument("-offset", help="Number of items to skip (default 0)")
metadata_parser.add_argument(
    "-bbox",
    help="Whether or not to use a bounding box filter (default: false)",
    action="store_true",
)

# Places Group
places_parser = subparsers.add_parser("places")
places_parser.add_argument(
    "command",
    choices=["find", "shape", "prefix", "placetypes"],
    help="The action to take.",
)
places_parser.add_argument("argument", nargs="?")

places_parser.add_argument("-url", help="The url of the service")
places_parser.add_argument(
    "-placetype", default=None, help="The placetype of the response"
)
places_parser.add_argument(
    "-geom",
    default="low",
    choices=["low", "medium", "high", "None"],
    help="Resolution of shape",
)
places_parser.add_argument("-output", default="geojson")

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


def handle(args):
    if args.group == "auth":
        auth_handler(args)
    elif args.group == "metadata":
        metadata_handler(args)
    elif args.group == "places":
        places_handler(args)
    elif args.group == "raster":
        raster_handler(args)
    else:
        print("invalid command group")


if __name__ == "__main__":
    handle(parser.parse_args())
