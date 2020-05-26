import pytest
import mock
import operator

from .. import interpreter


class TestInterpreter(object):
    def test_literal(self):
        graft = {"x": 1, "returns": "x"}
        func = interpreter.interpret(graft)
        assert func() == 1

    def test_literal_str(self):
        graft = {"x": "foo", "returns": "x"}
        func = interpreter.interpret(graft)
        assert func() == "foo"

    @pytest.mark.parametrize(
        "json",
        [
            [1, 2, 3],
            [1, 2, "foo", 3, {"four": 5, "six": [7, 8]}],
            [1],
            [[1]],
            [{"y": 2}],
            [{"a": 1, "returns": "a"}],
            [{"parameters": ["x"], "a": 1, "returns": "a"}],
            ["not_function_application"],
        ],
    )
    def test_quoted_json(self, json):
        graft = {"x": [json], "returns": "x"}
        func = interpreter.interpret(graft)
        assert func() == json

    @pytest.mark.parametrize(
        "graft",
        [
            # positional only
            {"x": 1, "y": 2, "z": ["add", "x", "y"], "returns": "z"},
            # positional and named
            {"x": 1, "y": 2, "z": ["add", "x", {"b": "y"}], "returns": "z"},
            # named only
            {"x": 1, "y": 2, "z": ["add", {"a": "x", "b": "y"}], "returns": "z"},
            # positional only, with empty parameters tossed in
            {"parameters": [], "x": 1, "y": 2, "z": ["add", "x", "y"], "returns": "z"},
        ],
    )
    def test_apply(self, graft):
        func = interpreter.interpret(graft, builtins={"add": lambda a, b: a + b})
        assert func() == 3

    def test_apply_to_param(self):
        graft = {"parameters": ["x"], "y": 2, "z": ["add", "x", "y"], "returns": "z"}
        func = interpreter.interpret(graft, builtins={"add": operator.add})
        assert func(5) == func(x=5) == 7

    @pytest.mark.parametrize(
        "graft",
        [
            {"x": ["do_foo"], "returns": "x"},  # no named args
            {"x": ["do_foo", {}], "returns": "x"},  # empty named args
        ],
    )
    def test_apply_noargs(self, graft):
        func = interpreter.interpret(graft, builtins={"do_foo": lambda: "foo"})
        assert func() == "foo"

    @pytest.mark.parametrize(
        "apply_expr",
        [
            ["adder", "x"],  # apply arguments positionally
            ["adder", {"p": "x"}],  # apply arguments by name
        ],
    )
    def test_funcdef(self, apply_expr):
        graft = {
            "x": 1,
            "adder": {
                "parameters": ["p"],
                "one": 1,
                "res": ["add", "p", "one"],
                "returns": "res",
            },
            "z": apply_expr,
            "returns": "z",
        }
        func = interpreter.interpret(graft, builtins={"add": operator.add})
        assert func() == 2

    def test_funcdef_closure(self):
        graft = {
            "one": 1,
            "adder": {
                "parameters": ["p"],
                "res": ["add", "p", "one"],
                "returns": "res",
            },
            "y": 5.5,
            "z": ["adder", "y"],
            "returns": "z",
        }
        func = interpreter.interpret(graft, builtins={"add": operator.add})
        assert func() == 6.5

    def test_closure_shadows_parent(self):
        graft = {
            "shadowme": 1,
            "foo": {
                "parameters": ["x"],
                "shadowme": 2,
                "y": ["add", "x", "shadowme"],
                "returns": "y",
            },
            "two": 2,
            "three": ["add", "two", "shadowme"],
            "res": ["foo", "three"],
            "returns": "res",
        }
        func = interpreter.interpret(
            graft, builtins={"add": operator.add, "foo": lambda x: -1, "shadowme": -100}
        )
        assert func() == 5

    def test_higher_order_function(self):
        graft = {
            "two": 2,
            "factory": {
                "parameters": ["p"],
                "half_p": ["div", "p", "two"],
                "func": {
                    "parameters": ["x"],
                    "one": 1,
                    "half_p_plus_one": ["add", "one", "half_p"],
                    "x_plus_half_p_plus_one": ["add", "x", "half_p_plus_one"],
                    "returns": "x_plus_half_p_plus_one",
                },
                "returns": "func",
            },
            "ten": 10,
            "half_ten_plus_one_adder": ["factory", "ten"],
            "res1": ["half_ten_plus_one_adder", "two"],  # (10 / 2) + 1 + 2 == 8
            "half_eight_plus_one_adder": ["factory", "res1"],
            "five": 5,
            "res2": ["half_eight_plus_one_adder", "five"],  # (8 / 2) + 1 + 5 == 10
            "returns": "res2",
        }
        func = interpreter.interpret(
            graft, builtins={"add": operator.add, "div": operator.truediv}
        )
        assert func() == 10

    def test_reuse_shared_dependency(self):
        getBaseVal = mock.Mock()
        getBaseVal.return_value = 10

        graft = {
            "base_val": ["getBaseVal"],
            "five": 5,
            "low": ["sub", "base_val", "five"],
            "high": ["add", "base_val", "five"],
            "res": ["sub", "high", "low"],
            "returns": "res",
        }
        func = interpreter.interpret(
            graft,
            builtins={
                "add": operator.add,
                "sub": operator.sub,
                "getBaseVal": getBaseVal,
            },
        )
        assert func() == 10
        getBaseVal.assert_called_once()

    def test_reuse_shared_dependency_in_closure(self):
        getBaseVal = mock.Mock()
        getBaseVal.return_value = 10

        graft = {
            "base_val": ["getBaseVal"],
            "get_low": {
                "parameters": ["x"],
                "res": ["sub", "base_val", "x"],
                "returns": "res",
            },
            "get_high": {
                "parameters": ["x"],
                "res": ["add", "base_val", "x"],
                "returns": "res",
            },
            "five": 5,
            "low": ["get_low", "five"],
            "high": ["get_high", "five"],
            "res": ["sub", "high", "low"],
            "returns": "res",
        }
        func = interpreter.interpret(
            graft,
            builtins={
                "add": operator.add,
                "sub": operator.sub,
                "getBaseVal": getBaseVal,
            },
        )
        assert func() == 10
        getBaseVal.assert_called_once()


class TestErrors(object):
    def test_syntax_error_no_return(self):
        graft = {"x": 1}
        with pytest.raises(
            interpreter.exceptions.GraftSyntaxError, match="missing a 'returns' key"
        ):
            interpreter.interpret(graft)

    # def test_syntax_error_no_return_nested(self):
    #     graft = {"x": {"y": 0}, "z": ["x"], "returns": "z"}
    #     with pytest.raises(
    #         interpreter.exceptions.GraftSyntaxError, match="missing a 'returns' key"
    #     ):
    #         interpreter.interpret(graft)()

    def test_syntax_error_invalid_return(self):
        graft = {"returns": 99}
        with pytest.raises(
            interpreter.exceptions.GraftSyntaxError,
            match="Invalid value for a 'returns' key",
        ):
            interpreter.interpret(graft)

    @pytest.mark.parametrize("name", ["returns", "parameters"])
    def test_syntax_error_reserved_name(self, name):
        graft = {"x": [name], "returns": "x"}
        with pytest.raises(
            interpreter.exceptions.GraftSyntaxError,
            match=r"Not a valid expression: \[{!r}\]".format(name),
        ):
            interpreter.interpret(graft)()

    @pytest.mark.parametrize(
        "invalid_expr",
        [
            [1, 2, 3],
            {},
            {"parameters": ["y"]},
            {"y": 1},
            ["function", "a", {"param": "foo"}, "b"],
            ["function", {"param": "foo"}, "b"],
            ["function", {"param": True}],
            ["function", True],
            [0, "y"],
            [1],
            [True],
            [None],
        ],
    )
    def test_syntax_error_invalid_expr(self, invalid_expr):
        graft = {"x": invalid_expr, "returns": "x"}
        with pytest.raises(
            interpreter.exceptions.GraftSyntaxError, match="Not a valid expression"
        ):
            interpreter.interpret(graft)()

    @pytest.mark.parametrize(
        "expr",
        [
            ["foo", "doesnt_exist"],
            ["foo", {"x": "doesnt_exist"}],
            ["doesnt_exist", "one"],
            ["doesnt_exist", {"x": "one"}],
            ["doesnt_exist", "doesnt_exist"],
        ],
    )
    def test_name_error(self, expr):
        graft = {"one": 1, "y": expr, "returns": "y"}
        with pytest.raises(interpreter.exceptions.GraftNameError, match="doesnt_exist"):
            interpreter.interpret(graft, builtins={"foo": lambda x: x})()

    @pytest.mark.parametrize("apply_expr", [["func"], ["func", {}]])
    def test_missing_argument(self, apply_expr):
        graft = {
            "func": {"parameters": ["param"], "returns": "param"},
            "x": apply_expr,
            "returns": "x",
        }
        with pytest.raises(
            interpreter.exceptions.GraftTypeError,
            match="Missing required argument 'param'",
        ):
            interpreter.interpret(graft)()

    @pytest.mark.parametrize(
        ["apply_expr", "missing", "plural"],
        [
            (["func", "one"], "'param2'", False),
            (["func", {"param": "one"}], "'param2'", False),
            (["func", {"param2": "one"}], "'param'", False),
            (["func"], "'param', 'param2'", True),
        ],
    )
    def test_missing_argument_multi(self, apply_expr, missing, plural):
        graft = {
            "func": {"parameters": ["param", "param2"], "returns": "param"},
            "one": 1,
            "x": apply_expr,
            "returns": "x",
        }
        with pytest.raises(
            interpreter.exceptions.GraftTypeError,
            match="Missing required argument{} {}".format(
                "s" if plural else "", missing
            ),
        ):
            interpreter.interpret(graft)()

    @pytest.mark.parametrize(
        ["apply_expr", "msg"],
        [
            (["func", {"bad_arg": "one"}], "Unexpected named argument 'bad_arg'"),
            (
                ["func", {"bad_arg": "one", "terrible_arg": "one"}],
                "Unexpected named arguments 'bad_arg', 'terrible_arg'",
            ),
            (
                ["func", "one", "one"],
                "Too many positional arguments: expected 1, got 2",
            ),
        ],
    )
    def test_unexpected_argument(self, apply_expr, msg):
        graft = {
            "func": {"parameters": ["param"], "returns": "param"},
            "one": 1,
            "x": apply_expr,
            "returns": "x",
        }
        with pytest.raises(interpreter.exceptions.GraftTypeError, match=msg):
            interpreter.interpret(graft)()
