import pytest
import shapely
from .. import Feature, Geometry, Image


@pytest.mark.parametrize("properties", [{}, {"foo": 1.0}])
def test_create(properties):
    Feature(geometry=Geometry(type="Point", coordinates=[0, 0]), properties=properties)


def test_rasterize():
    feature = Feature(
        geometry=Geometry(type="Point", coordinates=[0, 0]), properties={"foo": 1.0}
    )
    assert isinstance(feature.rasterize(2), Image)
    assert isinstance(feature.rasterize("foo"), Image)
    assert isinstance(feature.rasterize("foo", default_value=5), Image)


@pytest.mark.parametrize(
    "shape",
    [
        shapely.geometry.Point(0, 0),
        shapely.geometry.LineString([(0, 0), (1, 3), (-1, 2)]),
        shapely.geometry.box(1, 2, 5, 6),
    ],
)
def test_from_geojson(shape):
    # TODO: decent test
    geojson = dict(
        geometry=shape.__geo_interface__, properties={"foo": 1}, type="Feature"
    )
    feature = Feature.from_geojson(geojson)
    assert isinstance(feature, Feature)
