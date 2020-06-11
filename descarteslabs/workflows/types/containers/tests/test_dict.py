import six
import operator
import pytest
import mock

from descarteslabs.common.graft import client
from descarteslabs.common.graft import interpreter

from ...core import ProxyTypeError
from ...primitives import Int, Str, Float, Bool
from .. import Tuple, List, Dict


def iter_argpairs(args):
    assert len(args) % 2 == 0
    for key, value in zip(args[::2], args[1::2]):
        yield key, value


def dict_builtin(*args, **kwargs):
    result = dict(iter_argpairs(args))
    result.update(kwargs)
    return result


def test_init_unparameterized():
    with pytest.raises(TypeError, match="Cannot instantiate a generic Dict"):
        Dict({1: 2})


@mock.patch.object(client, "apply_graft", wraps=client.apply_graft)
def test_init_fromdict(mock_apply):
    dct = Dict[Int, Float]({1: 1.1, 2: 2.2})
    apply_args, apply_kwargs = mock_apply.call_args

    assert apply_args[0] == "wf.dict.create"
    assert len(apply_kwargs) == 0
    for (key_item, value_item) in iter_argpairs(apply_args[1:]):
        assert isinstance(key_item, Int)
        assert isinstance(value_item, Float)

    assert client.is_delayed(dct)
    assert interpreter.interpret(
        dct.graft, builtins={"wf.dict.create": dict_builtin}
    )() == {1: 1.1, 2: 2.2}


@mock.patch.object(client, "apply_graft", wraps=client.apply_graft)
def test_init_fromkwargs(mock_apply):
    dct = Dict[Str, Int](a=1, b=2, c=3)
    apply_args, apply_kwargs = mock_apply.call_args

    assert apply_args == ("wf.dict.create",)
    assert six.viewkeys(apply_kwargs) == {"a", "b", "c"}
    for value in six.itervalues(apply_kwargs):
        assert isinstance(value, Int)

    assert client.is_delayed(dct)
    assert interpreter.interpret(
        dct.graft, builtins={"wf.dict.create": dict_builtin}
    )() == dict(a=1, b=2, c=3)


@mock.patch.object(client, "apply_graft", wraps=client.apply_graft)
def test_init_fromdict_andkwargs(mock_apply):
    dct = Dict[Str, Int]({"a": 0, "z": 100}, a=1, b=2, c=3)
    apply_args, apply_kwargs = mock_apply.call_args

    assert apply_args == ("wf.dict.create",)
    assert six.viewkeys(apply_kwargs) == {"a", "z", "b", "c"}
    for value in six.itervalues(apply_kwargs):
        assert isinstance(value, Int)

    assert client.is_delayed(dct)

    assert interpreter.interpret(
        dct.graft, builtins={"wf.dict.create": dict_builtin}
    )() == {"a": 1, "z": 100, "b": 2, "c": 3}


def test_init_fromproxydict():
    dct1 = Dict[Str, Int](a=1, b=2)
    dc2 = Dict[Str, Int](dct1)
    assert dc2.graft == dct1.graft


def test_init_fromproxydict_wrongtype():
    dct1 = Dict[Str, Int](a=1, b=2)
    with pytest.raises(
        ProxyTypeError,
        match=r"Cannot convert Dict\[Str, Int\] to Dict\[Int, Int\], their element types are different",
    ):
        Dict[Int, Int](dct1)


def test_init_wrongtype():
    with pytest.raises(ProxyTypeError, match="Expected a mapping"):
        Dict[Str, Int](1)
    with pytest.raises(ProxyTypeError, match="Expected a mapping"):
        Dict[Str, Int](Str("sdf"))


def test_validate_params():
    Dict[Str, Int]
    Dict[Str, List[Tuple[Str, Float]]]

    with pytest.raises(
        AssertionError, match="Both Dict key and value types must be specified"
    ):
        Dict[Str]
    with pytest.raises(TypeError, match="must be Proxytypes"):
        Dict[Str, 1]


def test_from_pairs():
    pairs = List[Tuple[Str, Int]]([("foo", 1), ("bar", 2)])
    dct = Dict[Str, Int].from_pairs(pairs)
    assert isinstance(dct, Dict)

    pairs = [("foo", 1), ("bar", 2)]
    dct = Dict[Str, Int].from_pairs(pairs)
    assert isinstance(dct, Dict)

    with pytest.raises(TypeError):
        # wrong Dict type
        Dict[Int, Int].from_pairs(pairs)


def test_getitem_type():
    dct = Dict[Str, Int](a=1, b=2)
    assert isinstance(dct["a"], Int)
    with pytest.raises(
        ProxyTypeError, match="Dict keys are of type .*Str.*, but indexed with 1"
    ):
        dct[1]


def test_getitem_roundtrip():
    src = {"a": 1, "b": 2}
    dct = Dict[Str, Int](src)

    for key, truth in six.iteritems(src):
        value = interpreter.interpret(
            dct[key].graft,
            builtins={"wf.get": operator.getitem, "wf.dict.create": dict_builtin},
        )()
        assert value == truth


def test_keys():
    dct = Dict[Str, Float](x=1.0, y=2.2)
    keys = dct.keys()
    assert isinstance(keys, List[Str])


def test_values():
    dct = Dict[Str, Float](x=1.0, y=2.2)
    values = dct.values()
    assert isinstance(values, List[Float])


def test_items():
    dct = Dict[Str, Float](x=1.0, y=2.2)
    items = dct.items()
    assert isinstance(items, List[Tuple[Str, Float]])


def test_contains():
    dct = Dict[Str, Float](x=1.0, y=2.2)
    assert isinstance(dct.contains("foo"), Bool)
    with pytest.raises(TypeError):
        dct.contains(1)


def test_length():
    dct = Dict[Str, Float](x=1.0, y=2.2)
    assert isinstance(dct.length(), Int)


def test_iter_error():
    dct = Dict[Str, Float](x=1.0, y=2.2)
    with pytest.raises(TypeError, match=r"Consider \.keys\(\)\.map\(\.\.\.\)"):
        iter(dct)
