#!/usr/bin/env python
# Copyright 2018 Descartes Labs.
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

import json

from .places import Places


def places_handler(args):
    if args.geom == "None":
        args.geom = None

    places = Places()

    if args.url:
        places.url = args.url

    kwargs = {}

    if args.command == 'placetypes':
        placetypes = places.placetypes()

        print(json.dumps(placetypes, indent=2))

    if args.command == 'models':
        sources = places.sources()

        print(json.dumps(sources))

    if args.command == 'find':

        if args.placetype:
            kwargs['placetype'] = args.placetype

        find = places.find(args.argument, **kwargs)

        print(json.dumps(find, indent=2))

    if args.command == 'shape':
        kwargs['geom'] = args.geom
        shape = places.shape(args.argument, **kwargs)

        print(json.dumps(shape, indent=2))

    if args.command == 'prefix':
        if args.argument is None:
            raise RuntimeError("Please specify location in argument")

        if args.placetype is None:
            raise RuntimeError("Please specify placetype, e.g. -placetype region")

        kwargs['geom'] = args.geom
        kwargs['output'] = args.output
        kwargs['placetype'] = args.placetype

        prefix = places.prefix(args.argument, **kwargs)

        print(json.dumps(prefix, indent=2))
