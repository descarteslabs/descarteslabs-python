import pytest
import shapely

from .. import load_geojson, Geometry, GeometryCollection, Feature, FeatureCollection


@pytest.mark.parametrize(
    "geojson, expected",
    [
        [shapely.geometry.box(1, 2, 5, 6).__geo_interface__, Geometry],
        [
            shapely.geometry.GeometryCollection(
                [shapely.geometry.box(1, 2, 5, 6), shapely.geometry.box(1, 2, 3, 6)]
            ),
            GeometryCollection,
        ],
        [
            {
                "geometry": shapely.geometry.box(1, 2, 5, 6).__geo_interface__,
                "properties": {"foo": 1},
                "type": "Feature",
            },
            Feature,
        ],
        [
            {
                "type": "FeatureCollection",
                "features": [
                    {
                        "geometry": shapely.geometry.box(1, 2, 5, 6).__geo_interface__,
                        "properties": {"foo": 1},
                        "type": "Feature",
                    },
                    {
                        "geometry": shapely.geometry.box(0, 0, 4, 6).__geo_interface__,
                        "properties": {"foo": 2},
                        "type": "Feature",
                    },
                ],
            },
            FeatureCollection,
        ],
    ],
)
def test_load_geojson(geojson, expected):
    obj = load_geojson(geojson)
    assert isinstance(obj, expected)
