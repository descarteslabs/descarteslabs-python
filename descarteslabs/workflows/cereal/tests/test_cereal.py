import pytest

from hypothesis import given, settings, HealthCheck
import hypothesis.strategies as st

from .. import serialize_typespec, deserialize_typespec, serializable
from ...types import (
    Int,
    Bool,
    Float,
    Str,
    NoneType,
    List,
    Dict,
    Tuple,
    Struct,
    Function,
    GenericProxytype,
)


def test_serialize_unknown():
    with pytest.raises(
        ValueError, match="'int' is not in the types registry; cannot serialize it"
    ):
        serialize_typespec(int)

        class Str(object):
            pass

        with pytest.raises(ValueError, match="is not a subclass of"):
            serialize_typespec(Str)


def test_cant_serialize_generic():
    with pytest.raises(
        TypeError, match="Can only serialize concrete types, not the generic type"
    ):
        serialize_typespec(List)


@serializable(is_named_concrete_type=True)
class KnownClass(Tuple[Int, Str]):
    pass


@serializable()
class FooBar(GenericProxytype):
    pass


def test_named_concrete_type():
    typespec = serialize_typespec(KnownClass)
    assert deserialize_typespec(typespec) is KnownClass
    assert deserialize_typespec(typespec) is not Tuple[Int, Str]

    typespec = serialize_typespec(FooBar[Int, 1])
    assert deserialize_typespec(typespec) is FooBar[Int, 1]
    assert deserialize_typespec(typespec) is not FooBar


def test_serializable_helpful_error():
    with pytest.raises(
        TypeError, match="On Foo, the @serializable decorator must be called"
    ):

        @serializable
        class Foo(object):
            pass


primitives_st = st.sampled_from([Int, Bool, Float, Str, NoneType, KnownClass])

strings = st.text()

proxytypes = st.deferred(
    lambda: (
        primitives_st
        | st.builds(List.__class_getitem__, proxytypes)
        | st.builds(Dict.__class_getitem__, st.tuples(primitives_st, proxytypes))
        | st.builds(
            Tuple.__class_getitem__, st.lists(proxytypes, max_size=8).map(tuple)
        )
        | st.builds(
            Struct.__class_getitem__, st.dictionaries(strings, proxytypes, max_size=8)
        )
        | st.builds(
            Function.__class_getitem__,
            st.tuples(st.dictionaries(strings, proxytypes, max_size=8), proxytypes),
        )
    )
)


@given(proxytypes)
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_roundtrip(cls):
    assert deserialize_typespec(serialize_typespec(cls)) == cls
