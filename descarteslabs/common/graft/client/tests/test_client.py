import pytest

from .. import client
from ... import syntax


def reset_guid_counter(value=0):
    client.GUID_COUNTER = value


def drop_keys(dct, *keys):
    res = dct.copy()
    for key in keys:
        res.pop(key)
    return res


def test_keyref_graft():
    assert client.keyref_graft("some-key") == {"returns": "some-key"}


def test_keyref_type_error():
    with pytest.raises(TypeError):
        client.keyref_graft(42)


def test_keyref_value_error():
    for keyword in client.RESERVED_WORDS:
        with pytest.raises(ValueError):
            client.keyref_graft(keyword)


class TestFunctionGraft(object):
    def test_value_graft(self):
        result = {"x": 1, "res": ["add", "x", "y"], "returns": "res"}
        func = client.function_graft(result, "y")
        assert syntax.is_graft(func) and client.is_function_graft(func)
        assert func == dict(result, parameters=["y"])

    def test_higher_order(self):
        result = {"parameters": ["y"], "res": ["add", "x", "y"], "returns": "res"}

        reset_guid_counter()
        func = client.function_graft(result, "x")
        assert syntax.is_graft(func) and client.is_function_graft(func)
        assert func == {"parameters": ["x"], "1": result, "returns": "1"}

        foo = client.keyref_graft("foo")
        even_higher = client.apply_graft(func, foo)
        creed = client.function_graft(even_higher, foo)

        assert creed == {
            "parameters": ["foo"],
            "2": func,
            "3": ["2", "foo"],
            "returns": "3",
        }

    def test_outer_scope_value_graft(self):
        result = {"1": 1, "2": ["add", "1", "foo"], "returns": "2"}

        reset_guid_counter(100)
        func = client.function_graft(result, "foo", first_guid="2")
        assert func == {
            "1": 1,
            "101": dict(drop_keys(result, "1"), parameters=["foo"]),
            "returns": "101",
        }

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

        reset_guid_counter(100)
        func = client.function_graft(result, "outer_p", first_guid="3")
        assert func == {
            "parameters": ["outer_p"],
            "1": 1,
            "2": ["sub", "outer_p", "1"],
            "101": drop_keys(result, "1", "2"),
            "returns": "101",
        }


class TestIsolateKeys(object):
    def test_value_graft(self):
        initial = {"1": 1, "2": 2, "3": ["add", "1", "2"], "returns": "3"}

        reset_guid_counter(0)
        isolated = client.isolate_keys(initial)
        assert isolated == {"1": initial, "2": ["1"], "returns": "2"}

        extra = client.apply_graft("sub", isolated, 5)
        assert extra == dict(
            isolated, **{"3": 5, "4": ["sub", "2", "3"], "returns": "4"}
        )

    def test_function_graft_unwrapped(self):
        initial = {"parameters": ["x"], "2": 2, "3": ["add", "x", "2"], "returns": "3"}

        reset_guid_counter(0)
        isolated = client.isolate_keys(initial)
        assert isolated is initial

    def test_function_graft_wrapped(self):
        initial = {"parameters": ["x"], "2": 2, "3": ["add", "x", "2"], "returns": "3"}

        reset_guid_counter(0)
        isolated = client.isolate_keys(initial, wrap_function=True)
        assert isolated == {"1": initial, "returns": "1"}


class TestParametrize(object):
    def test_non_guid_params(self):
        initial = {"2": 2, "3": ["add", "x", "y", "2"], "returns": "3"}

        reset_guid_counter(0)
        parametrized = client.parametrize(initial, x=0, y=100, z="foo")
        assert parametrized == {
            "1": initial,
            "2": ["1"],
            "x": 0,
            "y": 100,
            "z": "foo",
            "returns": "2",
        }

    def test_guid_params(self):
        initial = {"2": 2, "3": ["add", "100", "101", "2"], "returns": "3"}

        reset_guid_counter(0)
        parametrized = client.parametrize(initial, **{"100": 0, "101": 100, "z": "foo"})
        assert parametrized == {
            "3": {
                "1": initial,
                "2": ["1"],
                "100": 0,
                "101": 100,
                "z": "foo",
                "returns": "2",
            },
            "4": ["3"],
            "returns": "4",
        }
