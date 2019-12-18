import pytest
import shapely

from ...containers import List
from .. import Feature, FeatureCollection, Geometry, Image


def test_create():
    FeatureCollection(
        [Feature(geometry=Geometry(type="Point", coordinates=[0, 0]), properties={})]
    )


def test_from_vector_id():
    assert isinstance(FeatureCollection.from_vector_id("foo"), FeatureCollection)


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
    feature = dict(
        geometry=shape.__geo_interface__, properties={"foo": 1}, type="Feature"
    )
    geojson = dict(features=[feature, feature], type="FeatureCollection")
    fc = FeatureCollection.from_geojson(geojson)
    assert isinstance(fc, FeatureCollection)


def test_struct_for_regressions():
    fc = FeatureCollection.from_vector_id("foo")
    assert isinstance(fc.features, List)


def test_rasterize():
    fc = FeatureCollection(
        [
            Feature(
                geometry=Geometry(type="Point", coordinates=[0, 0]),
                properties={"foo": 1.0},
            ),
            Feature(
                geometry=Geometry(type="Point", coordinates=[1, 2]),
                properties={"foo": 2.0},
            ),
        ]
    )
    assert isinstance(fc.rasterize(2), Image)
    assert isinstance(fc.rasterize("foo"), Image)
    assert isinstance(fc.rasterize("foo", default_value=5), Image)
    assert isinstance(
        fc.rasterize("foo", default_value=5, merge_algorithm="add"), Image
    )
    assert isinstance(
        fc.rasterize("foo", default_value=5, merge_algorithm="replace"), Image
    )


def test_sorted():
    fc = FeatureCollection.from_vector_id("foo")

    def key(feature):
        assert isinstance(feature, Feature)
        return feature.properties["foo"]

    sorted_ = fc.sorted(key)
    assert isinstance(sorted_, FeatureCollection)


def test_sorted_bad_args():
    fc = FeatureCollection.from_vector_id("foo")

    with pytest.raises(TypeError):
        fc.sorted()

    with pytest.raises(
        TypeError, match="Sort key function produced non-orderable type Feature"
    ):
        fc.sorted(lambda feature: feature)
