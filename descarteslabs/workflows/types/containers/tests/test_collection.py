import pytest

from ... import (
    Any,
    Bool,
    CollectionMixin,
    Feature,
    FeatureCollection,
    Image,
    Float,
    Int,
    List,
    ProxyTypeError,
    Str,
    Tuple,
)
from ..collection import _initial_reduce_type, REDUCE_INITIAL_DEFAULT


def test_init():
    with pytest.raises(TypeError):
        CollectionMixin()


@pytest.mark.parametrize("col", [FeatureCollection([]), List[Int]([0, 1, 2])])
def test_map_identity(col):
    assert isinstance(col.map(lambda x: x), type(col))


@pytest.mark.parametrize("col", [FeatureCollection([]), List[Int]([0, 1, 2])])
def test_map_conversion(col):
    assert isinstance(col.map(lambda x: Str("")), List[Str])


@pytest.mark.parametrize("col", [FeatureCollection([]), List[Int]([0, 1, 2])])
def test_filter(col):
    filtered = col.filter(lambda x: Bool(True))

    assert isinstance(filtered, type(col))


def test_reduce():
    initial = Float(0.0)
    list_ = List[Tuple[Str, Int]]([("foo", 0), ("bar", 1), ("baz", 2)])
    reduced = list_.reduce(lambda x, y: x + y[1], initial)

    assert isinstance(reduced, type(initial))


def test_reduce_no_initial():
    list_ = List[Int]([0, 1, 2])
    reduced = list_.reduce(lambda x, y: x + y)

    assert isinstance(reduced, Int)


def test_reduce_no_promote_initial():
    list_ = List[Int]([0, 1, 2])

    with pytest.raises(ProxyTypeError):
        list_.reduce(lambda x, y: x + y, 0)


def test_reduce_wrong_return_type():
    list_ = List[Int]([0, 1, 2])

    with pytest.raises(ProxyTypeError):
        list_.reduce(lambda x, y: Str(""))


def test_collection_type():
    list_ = List[Int]([0, 1, 2])
    assert list_._element_type is list_._type_params[0]

    fc = FeatureCollection([])
    assert fc._element_type is Feature


def test__initial_reduce_type_no_initial():
    type_ = _initial_reduce_type(REDUCE_INITIAL_DEFAULT, Int)
    assert type_ is Int


def test__initial_reduce_type_with_initial():
    type_ = _initial_reduce_type(List[Int]([8, 9]), Int)
    assert type_ is List[Int]


def test__initial_reduce_type_value_no_promotion():
    with pytest.raises(ProxyTypeError):
        _initial_reduce_type(0, Int)


def test_sorted():
    col = List[Int]([0, 1, 2])

    sorted_ = col.sorted()
    assert isinstance(sorted_, type(col))

    elem_type = col._element_type

    def sorter(x):
        assert isinstance(x, elem_type)
        return Int(1)

    sorted_ = col.sorted(key=sorter)
    assert isinstance(sorted_, type(col))


def test_sorted_any():
    col = List[Any]([0, "foo", 2])

    assert isinstance(col.sorted(lambda x: x + 1), type(col))


def test_sorted_bad_key():
    col = List[Int]([0, 1, 2])

    with pytest.raises(
        TypeError, match="Sort key function produced non-orderable type Feature"
    ):
        # < operator fails
        col.sorted(key=lambda x: Feature._from_apply(""))

    with pytest.raises(
        TypeError,
        match=(
            "Sort key function produced Image, which is not orderable, "
            "since comparing Images produces Image, not Bool."
        ),
    ):
        # < operator doesn't produce Bool
        col.sorted(key=lambda x: Image.from_id("foo") + x)
