import pytest

from descarteslabs.common.graft import client

from ...core import ProxyTypeError
from ...primitives import Int, Str
from .. import Struct, List, Tuple, struct as struct_


def test_init_unparameterized():
    with pytest.raises(TypeError, match="Cannot instantiate a generic Struct"):
        Struct(x=1)


def test_validate_params():
    Struct[{"a": Int}]
    Struct[{"a": Tuple[Int, Str], "b": Struct[{"x": Int}]}]

    with pytest.raises(AssertionError, match="must be specified with a dictionary"):
        Struct[Int]
    with pytest.raises(AssertionError, match="Field names must be strings"):
        Struct[{1: Str}]
    with pytest.raises(TypeError, match="must be Proxytypes"):
        Struct[{"a": 2}]
        Struct[{"a": Tuple[Int, Str], "b": Struct[{"x": Int, "y": 1}]}]


def test_promote_kwargs():
    struct = Struct[{"a": Int, "b": Str}]
    promoted = struct._promote_kwargs({"a": 1, "b": "foo"})
    assert isinstance(promoted["a"], Int)
    assert isinstance(promoted["b"], Str)


def test_promote_kwargs_missing():
    struct = Struct[{"a": Int, "b": Str}]
    with pytest.raises(
        ProxyTypeError,
        match=r"Missing required keyword arguments to Struct\[{'a': Int, 'b': Str}\]: 'b'",
    ):
        struct._promote_kwargs({"a": 1})


def test_promote_kwargs_optional():
    struct = Struct[{"a": Int, "b": Str}]
    promoted = struct._promote_kwargs({"a": 1}, optional={"b"})
    assert "b" not in promoted
    with pytest.raises(TypeError, match="Optional kwargs must be given as a set"):
        struct._promote_kwargs({"a": 1}, optional=["b"])


def test_promote_kwargs_read_only():
    struct = Struct[{"a": Int, "b": Str}]

    with pytest.raises(TypeError, match="Read only kwargs must be given as a set"):
        struct._promote_kwargs({"a": 1}, read_only=["b"])

    class Foo(struct):
        _read_only = {"b"}

    with pytest.raises(TypeError, match="Read only keyword argument to"):
        Foo(a=1, b="foo")

    val = Foo(a=1)
    assert isinstance(val.b, Str)


def test_promote_kwargs_extra():
    struct = Struct[{"a": Int, "b": Str}]
    with pytest.raises(
        ProxyTypeError, match=r"Struct\[{'a': Int, 'b': Str}\] has no field 'c'"
    ):
        struct._promote_kwargs({"a": 1, "b": "foo", "c": "extra"})


def test_promote_kwargs_wrong_type():
    struct = Struct[{"a": Int, "b": Str}]
    with pytest.raises(
        ProxyTypeError,
        match=r"In field 'b' of Struct\[{'a': Int, 'b': Str}\], expected .*Str.*",
    ):
        struct._promote_kwargs({"a": 1, "b": 0})


def test_init():
    struct = Struct[{"a": Int, "b": Str}](a=1, b="foo")
    assert client.is_delayed(struct)
    assert isinstance(struct._items_cache["a"], Int)
    assert isinstance(struct._items_cache["b"], Str)


def test_attr_type():
    struct = Struct[{"a": Int, "b": Str}](a=1, b="foo")
    assert isinstance(struct._attr("a"), Int)
    assert isinstance(struct._attr("b"), Str)


def test_attr_error():
    struct = Struct[{"a": Int, "b": Str}](a=1, b="foo")
    with pytest.raises(
        AttributeError, match=r"Struct\[{'a': Int, 'b': Str}\] has no attribute 'foo'"
    ):
        struct._attr("foo")


@pytest.mark.parametrize(
    "struct",
    [
        Struct[{"a": Int, "b": Str}](a=1, b="foo"),
        Struct[{"a": Int, "b": Str}]._from_graft({"returns": None}),
    ],
)
def test_attr_cache(struct):
    assert struct._attr("a") is struct._attr("a")
    assert struct._attr("b") is struct._attr("b")
    assert struct._attr("a") is not struct._attr("b")


def test_properties():
    struct = Struct[{"a": Int, "b": List[Str]}]

    assert hasattr(struct, "a")
    assert hasattr(struct, "b")

    assert struct.a.__doc__ == "Int"
    assert struct.b.__doc__ == "List[Str]"

    instance = struct(a=1, b="foo")

    a = instance.a
    assert isinstance(a, Int)
    b = instance.b
    assert isinstance(b, List[Str])

    assert instance.a is instance.a


def test_dict_repr_to_constructor_syntax():
    struct = Struct[{"a": Int, "b": Struct[{"a": Int, "b": List[Str]}]}]
    name = struct.__name__

    sphinx_safe = struct_._dict_repr_to_constructor_syntax(name)
    assert ":" not in sphinx_safe
    assert "{" not in sphinx_safe
    assert "dict" in sphinx_safe

    reconstituted = eval(sphinx_safe)
    assert reconstituted is struct


def test_doc_on_subclass():
    struct = Struct[{"a": Int, "b": Str}]

    class Sub(struct):
        _doc = {"a": "a field", "b": "b field"}

    assert Sub.a.__doc__ == "Int: a field"
    assert Sub.b.__doc__ == "Str: b field"


def test_doc_on_subclass_wrong_field():
    struct = Struct[{"a": Int, "b": Str}]

    with pytest.raises(TypeError, match="Cannot document field 'missing'"):

        class Sub(struct):
            _doc = {"a": "a field", "missing": "b field"}
