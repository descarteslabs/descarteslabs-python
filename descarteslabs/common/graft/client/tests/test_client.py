import functools

import pytest

from .. import client
from ... import syntax


def consistent_guid(value=0):
    "Decorator that ensures a consistent graft GUID at the start of a function"

    def deco(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            with client.consistent_guid(value):
                return func(*args, **kwargs)

        return wrapped

    return deco


def drop_keys(dct, *keys):
    res = dct.copy()
    for key in keys:
        res.pop(key)
    return res


def test_keyref_graft():
    assert client.keyref_graft("some-key") == {"returns": "some-key"}


@pytest.mark.parametrize("key", (42,) + syntax.RESERVED_WORDS)
def test_bad_keyref_key(key):
    with pytest.raises(ValueError):
        client.keyref_graft(key)


class TestFunctionGraft(object):
    def test_value_graft(self):
        result = {"x": 1, "res": ["add", "x", "y"], "returns": "res"}
        func = client.function_graft(result, "y")
        assert syntax.is_graft(func) and client.is_function_graft(func)
        assert func == dict(result, parameters=["y"])

    @consistent_guid(0)
    def test_higher_order(self):
        result = {"parameters": ["y"], "res": ["add", "x", "y"], "returns": "res"}

        func = client.function_graft(result, "x")
        assert syntax.is_graft(func) and client.is_function_graft(func)
        assert func == {"parameters": ["x"], "0": result, "returns": "0"}

        foo = client.keyref_graft("foo")
        even_higher = client.apply_graft(func, foo)
        creed = client.function_graft(even_higher, foo)

        assert creed == {
            "parameters": ["foo"],
            "1": func,
            "2": ["1", "foo"],
            "returns": "2",
        }

    @consistent_guid(100)
    def test_outer_scope_value_graft(self):
        result = {"1": 1, "2": ["add", "1", "foo"], "returns": "2"}

        func = client.function_graft(result, "foo", first_guid="2")
        assert func == {
            "1": 1,
            "100": dict(drop_keys(result, "1"), parameters=["foo"]),
            "returns": "100",
        }

    @consistent_guid(100)
    def test_outer_scope_higher_order(self):
        result = {
            "1": 1,
            "2": ["sub", "outer_p", "1"],
            # the above values actually supposed to be in
            # the outer scope of a function that takes "outer_p"
            "parameters": ["inner_p"],
            "3": ["add", "2", "inner_p"],
            "returns": "3",
        }

        func = client.function_graft(result, "outer_p", first_guid="3")
        assert func == {
            "parameters": ["outer_p"],
            "1": 1,
            "2": ["sub", "outer_p", "1"],
            "100": drop_keys(result, "1", "2"),
            "returns": "100",
        }


class TestIsolateKeys(object):
    @consistent_guid(0)
    def test_value_graft(self):
        initial = {"1": 1, "2": 2, "3": ["add", "1", "2"], "returns": "3"}

        isolated = client.isolate_keys(initial)
        assert isolated == {"0": initial, "1": ["0"], "returns": "1"}

        extra = client.apply_graft("sub", isolated, 5)
        assert extra == dict(
            isolated, **{"2": 5, "3": ["sub", "1", "2"], "returns": "3"}
        )

    def test_function_graft_unwrapped(self):
        initial = {"parameters": ["x"], "2": 2, "3": ["add", "x", "2"], "returns": "3"}

        isolated = client.isolate_keys(initial)
        assert isolated is initial

    @consistent_guid(0)
    def test_function_graft_wrapped(self):
        initial = {"parameters": ["x"], "2": 2, "3": ["add", "x", "2"], "returns": "3"}

        isolated = client.isolate_keys(initial, wrap_function=True)
        assert isolated == {"0": initial, "returns": "0"}


class TestParametrize(object):
    @consistent_guid(0)
    def test_non_guid_params(self):
        initial = {"2": 2, "3": ["add", "x", "y", "2"], "returns": "3"}

        parametrized = client.parametrize(initial, x=0, y=100, z="foo")
        assert parametrized == {
            "0": initial,
            "1": ["0"],
            "x": 0,
            "y": 100,
            "z": "foo",
            "returns": "1",
        }

    @consistent_guid(0)
    def test_guid_params(self):
        initial = {"2": 2, "3": ["add", "100", "101", "2"], "returns": "3"}

        parametrized = client.parametrize(initial, **{"100": 0, "101": 100, "z": "foo"})
        assert parametrized == {
            "2": {
                "0": initial,
                "1": ["0"],
                "100": 0,
                "101": 100,
                "z": "foo",
                "returns": "1",
            },
            "3": ["2"],
            "returns": "3",
        }
