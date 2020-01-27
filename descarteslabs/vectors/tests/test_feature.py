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

import json
import unittest

from descarteslabs.common.dotdict import DotDict
from descarteslabs.vectors import Feature

from .fixtures import POINT, POLYGON


class TestFeature(unittest.TestCase):
    def test___init__(self):
        feature = Feature(geometry=POINT, properties={})

        assert feature.id is None
        assert feature.geometry is not None
        assert feature.properties is not None

    def test__create_from_jsonapi(self):
        r = DotDict(
            id="foo", attributes=dict(geometry=POINT, properties=dict(foo="bar"))
        )
        feature = Feature._create_from_jsonapi(r)

        assert feature.id is not None
        assert feature.geometry is not None
        assert feature.properties is not None

    def test_geojson_geometries(self):
        geometries = [POLYGON, POINT]
        properties = {"temperature": 70.13, "size": "large"}

        for geometry in geometries:
            feature = Feature(geometry=geometry, properties=properties)

            assert json.dumps(feature.geojson, sort_keys=True) == json.dumps(
                {
                    "geometry": geometry,
                    "id": None,
                    "properties": properties,
                    "type": "Feature",
                },
                sort_keys=True,
            )
