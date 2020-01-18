import pytest


import shapely.geometry

from ...primitives import Int
from .. import Geometry, GeometryCollection, Image


@pytest.mark.parametrize(
    "type_, coordinates",
    [
        ("Point", [0, 0]),
        ("Point", [0.0, 0.0]),  # with floats
        ("Polygon", [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]]),  # without holes
        (
            "Polygon",
            [
                [[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]],
                [[1, 1], [3, 1], [3, 3], [1, 3], [1, 1]],
            ],
        ),  # with holes
    ],
)
def test_create(type_, coordinates):
    Geometry(type=type_, coordinates=coordinates)


def test_from_geo_interface():
    shape = shapely.geometry.Point(0, 0)
    geom = Geometry.from_geo_interface(shape)
    assert isinstance(geom, Geometry)

    with pytest.raises(
        TypeError, match="Expected an object with a `__geo_interface__` attribute"
    ):
        Geometry.from_geo_interface([])


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
    geom = Geometry.from_geojson(shape.__geo_interface__)
    assert isinstance(geom, Geometry)


def test_promote():
    shape = shapely.geometry.Point(0, 0)

    assert isinstance(Geometry._promote(shape), Geometry)


def test_rasterize():
    geom = Geometry.from_geo_interface(shapely.geometry.box(2, 3, 7, 8))
    assert isinstance(geom.rasterize(2), Image)


def test_gc_create():
    geoms = [
        Geometry(**{"type": "Point", "coordinates": [0, 0]}),
        Geometry(**{"type": "Point", "coordinates": [0.0, 0.0]}),  # with floats
        Geometry(
            **{
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 1], [0, 0]]],
            }
        ),  # without holes
        Geometry(
            **{
                "type": "Polygon",
                "coordinates": [
                    [[0, 0], [4, 0], [4, 4], [0, 4], [0, 0]],
                    [[1, 1], [3, 1], [3, 3], [1, 3], [1, 1]],
                ],
            }
        ),  # with holes
    ]
    GeometryCollection(type="GeometryCollection", geometries=geoms)


def test_gc_from_geojson():
    geoms = [
        shapely.geometry.Point(0, 0),
        shapely.geometry.LineString([(0, 0), (1, 3), (-1, 2)]),
        shapely.geometry.box(1, 2, 5, 6),
    ]
    shape = shapely.geometry.GeometryCollection(geoms)
    geom = GeometryCollection.from_geojson(shape.__geo_interface__)
    assert isinstance(geom, GeometryCollection)


def test_gc_length():
    gc = GeometryCollection.from_geojson(
        {"type": "GeometryCollection", "geometries": []}
    )
    assert isinstance(gc.length(), Int)


def test_gc_reversed():
    gc = GeometryCollection.from_geojson(
        {"type": "GeometryCollection", "geometries": []}
    )
    rev = reversed(gc)
    assert isinstance(rev, GeometryCollection)
    assert rev is not gc
