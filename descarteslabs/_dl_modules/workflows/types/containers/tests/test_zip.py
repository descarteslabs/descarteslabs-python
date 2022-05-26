import pytest

from ...core import ProxyTypeError
from ...primitives import Int, Str
from ...geospatial import ImageCollection, Image, FeatureCollection, GeometryCollection

from .. import Tuple, List, zip as wf_zip


examples = [
    List[Int]([1, 2, 3]),
    List[Str](["a", "b", "c"]),
    List[Tuple[Int, Str]]([(1, "foo"), (3, "bar")]),
    ImageCollection.from_id("foo"),
    Image.from_id("foo"),
    FeatureCollection.from_vector_id("bar"),
    GeometryCollection.from_geojson({"type": "GeometryCollection", "geometries": []}),
]


@pytest.mark.parametrize("args", [examples, ()] + [(ex,) for ex in examples])
def test_zip(args):
    zipped = wf_zip(*args)
    assert isinstance(
        zipped,
        List[Tuple[tuple(getattr(arg, "_element_type", type(arg)) for arg in args)]],
    )


def test_zip_str():
    zipped = wf_zip(Str("abcd"), List[Int]([1, 2, 3]))
    assert isinstance(zipped, List[Tuple[Str, Int]])


@pytest.mark.parametrize(
    "seqs",
    [
        [List[Int]([1, 2, 3]), [1, 2, 3]],
        [List[Int]([1, 2, 3]), Tuple[Int, Int, Int]([1, 2, 3])],
        [List[Int]([1, 2, 3]), "asdf"],
    ],
)
def test_zip_wrong_args(seqs):
    with pytest.raises(
        ProxyTypeError, match="All arguments to 'zip' must be Proxytype sequences"
    ):
        wf_zip(*seqs)
