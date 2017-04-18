#!/usr/bin/env python
# Copyright 2017 Descartes Labs.
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

from __future__ import print_function
import json

import descarteslabs as dl


def metadata_handler(args):
    metadata = dl.metadata

    if args.url:
        metadata.url = args.url

    kwargs = {}

    if args.command == 'sources':
        sources = metadata.sources()

        print(json.dumps(sources, indent=2))

    if args.command == 'summary':

        if args.place:
            kwargs['place'] = args.place
        if args.const_id:
            kwargs['const_id'] = args.const_id
        if args.start_time:
            kwargs['start_time'] = args.start_time
        if args.end_time:
            kwargs['end_time'] = args.end_time
        if args.geom:
            kwargs['geom'] = args.geom
        if args.params:
            kwargs['params'] = args.params
        if args.bbox:
            kwargs['bbox'] = args.bbox

        summary = metadata.summary(**kwargs)

        print(json.dumps(summary))

    if args.command == 'search':

        if args.place:
            kwargs['place'] = args.place
        if args.const_id:
            kwargs['const_id'] = args.const_id
        if args.start_time:
            kwargs['start_time'] = args.start_time
        if args.end_time:
            kwargs['end_time'] = args.end_time
        if args.geom:
            kwargs['geom'] = args.geom
        if args.params:
            kwargs['params'] = args.params
        if args.limit:
            kwargs['limit'] = args.limit
        if args.offset:
            kwargs['offset'] = args.offset
        if args.bbox:
            kwargs['bbox'] = args.bbox

        search = metadata.search(**kwargs)

        print(json.dumps(search, indent=2))

    if args.command == 'keys':

        if args.place:
            kwargs['place'] = args.place
        if args.const_id:
            kwargs['const_id'] = args.const_id
        if args.start_time:
            kwargs['start_time'] = args.start_time
        if args.end_time:
            kwargs['end_time'] = args.end_time
        if args.geom:
            kwargs['geom'] = args.geom
        if args.params:
            kwargs['params'] = args.params
        if args.limit:
            kwargs['limit'] = args.limit
        if args.offset:
            kwargs['offset'] = args.offset
        if args.bbox:
            kwargs['bbox'] = args.bbox

        keys = metadata.keys(**kwargs)

        print(' '.join(keys))

    if args.command == 'get':
        get = metadata.get(args.argument)

        print(json.dumps(get))
