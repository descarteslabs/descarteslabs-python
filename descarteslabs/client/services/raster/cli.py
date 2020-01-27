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

import argparse
import json

import six

from .raster import Raster


def scales(s):
    if s.count(",") == 1:
        mi, ma = six.moves.map(float, s.split(","))
        return [mi, ma]
    elif s.count(",") == 3:
        smi, sma, dmi, dma = six.moves.map(float, s.split(","))
        return [smi, sma, dmi, dma]
    else:
        raise argparse.ArgumentTypeError("Scales must be mi,ma or smi,sma,dmi,dma")


def raster_handler(args):
    params = {
        "inputs": args.inputs,
        "bands": args.bands,
        "scales": args.scales,
        "resolution": args.resolution,
        "data_type": args.data_type,
        "output_format": args.output_format,
        "srs": args.srs,
        "place": args.place,
        "save": True,
        "outfile_basename": args.outfile_basename,
    }
    response = Raster().raster(**params)
    print(json.dumps(response["metadata"], indent=2))
