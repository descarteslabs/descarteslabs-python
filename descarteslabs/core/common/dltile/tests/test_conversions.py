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

import json

import pytest
import shapely.geometry as geo

from ..conversions import normalize_polygons
from ..exceptions import InvalidShapeError


@pytest.fixture
def feature():
    return {
        "type": "Feature",
        "properties": {"id": 0},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [-67.13734, 45.13745],
                    [-66.96466, 44.8097],
                    [-68.03252, 44.3252],
                    [-67.13734, 45.13745],
                ]
            ],
        },
    }


def test_normalize_polygons_jsonfeature(feature):
    inshape = json.dumps(feature)
    expected = [geo.shape(feature["geometry"])]
    result = normalize_polygons(inshape)
    assert result == expected


def test_normalize_polygons_jsonfeaturelist(feature):
    inshape = json.dumps([feature])
    expected = [geo.shape(feature["geometry"])]
    result = normalize_polygons(inshape)
    assert result == expected


def test_normalize_polygons_geometry(feature):
    inshape = feature["geometry"]
    expected = [geo.shape(feature["geometry"])]
    result = normalize_polygons(inshape)
    assert result == expected


def test_normalize_polygons_feature(feature):
    inshape = feature
    expected = [geo.shape(feature["geometry"])]
    result = normalize_polygons(inshape)
    assert result == expected


def test_normalize_polygons_featurecollection(feature):
    inshape = {"type": "FeatureCollection", "features": [feature]}
    expected = [geo.shape(feature["geometry"])]
    result = normalize_polygons(inshape)
    assert result == expected


def test_normalize_polygons_poly(feature):
    inshape = geo.shape(feature["geometry"])
    expected = [geo.shape(feature["geometry"])]
    result = normalize_polygons(inshape)
    assert result == expected


def test_normalize_polygons_multipoly(feature):
    original_geom = feature["geometry"]
    feature["geometry"]["coordinates"] = [original_geom["coordinates"]]
    feature["geometry"]["type"] = "MultiPolygon"
    inshape = geo.shape(feature["geometry"])
    expected = [geo.shape(original_geom)]
    result = normalize_polygons(inshape)
    assert result == expected


def test_normalize_polygons_geo_interface(feature):
    class MockGeo(object):
        @property
        def __geo_interface__(self):
            return feature

    inshape = MockGeo()
    expected = [geo.shape(feature["geometry"])]
    result = normalize_polygons(inshape)
    assert result == expected


def test_normalize_polygons_badshape(feature):
    feature["geometry"]["coordinates"] = [0, 0]
    feature["geometry"]["type"] = "Point"
    inshape = geo.shape(feature["geometry"])
    with pytest.raises(InvalidShapeError):
        normalize_polygons(inshape)


def test_normalize_polygons_error(feature):
    with pytest.raises(InvalidShapeError):
        normalize_polygons([0, "a", None])
