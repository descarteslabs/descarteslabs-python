import operator
import pytest

from descarteslabs.common.graft import client, interpreter

from ...core import ProxyTypeError
from ...primitives import Any, Int, Str, Float
from ...containers import Dict, Tuple

from .. import Function


class TestDelay(object):
    def test_delay_anyargs(self):
        result = []

        def delayable(a, b, c):
            assert isinstance(a, Any)
            assert isinstance(b, Any)
            assert isinstance(c, Any)
            res = a + b / c
            result.append(res)
            return res

        delayed = Function._delay(delayable, Int)
        assert isinstance(delayed, Int)
        assert delayed.graft == client.function_graft(result[0], "a", "b", "c")

    def test_delay_fixedargs(self):
        result = []

        def delayable(a, b):
            assert isinstance(a, Dict[Str, Int])
            assert isinstance(b, Str)
            res = a[b]
            result.append(res)
            return res

        delayed = Function._delay(delayable, Int, Dict[Str, Int], Str)
        assert isinstance(delayed, Int)
        assert delayed.graft == client.function_graft(result[0], "a", "b")

    def test_delay_wrongargs(self):
        def delayable(a):
            pass

        with pytest.raises(TypeError, match="too many positional arguments"):
            Function._delay(delayable, Int, Dict[Str, Int], Str)

        def delayable(a, b, c):
            pass

        with pytest.raises(TypeError, match="missing a required argument: 'c'"):
            Function._delay(delayable, Int, Dict[Str, Int], Str)

    def test_delay_wrongresult(self):
        def delayable(a):
            return a

        with pytest.raises(ProxyTypeError, match="Cannot promote"):
            Function._delay(delayable, Str, Int)

    def test_delay_proxify_result(self):
        def delayable(a, b):
            c = a + b
            return c, (a, b), 2, "foo"

        delayed = Function._delay(delayable, None, Int, Float)
        assert isinstance(delayed, Tuple[Float, Tuple[Int, Float], Int, Str])


class TestFunction(object):
    def test_init_unparameterized(self):
        with pytest.raises(TypeError, match="Cannot instantiate a generic Function"):
            Function("asdf")

    def test_init_kwargs(self):
        with pytest.raises(
            TypeError, match="Cannot create a Function with optional arguments"
        ):
            Function[{"foo": Int}, Int](lambda foo=1: foo)

    def test_init_str(self):
        func = Function[{}, Int]("foo")
        assert func.function == "foo"

    def test_init_callable(self):
        func = Function[{}, Int](lambda: 1)
        assert isinstance(func.function, Int)
        # ^ this is weird and should change eventually
        interpreted_func = interpreter.interpret(func.graft)
        assert interpreted_func() == 1

    def test_call(self):
        def delayable(a, b):
            assert isinstance(a, Int)
            assert isinstance(b, Str)
            return a

        func = Function[Int, Str, {}, Int](delayable)
        result = func(1, "foo")
        assert isinstance(result, Int)
        assert interpreter.interpret(result.graft)() == 1

    def test_call_kwargs(self):
        builtins = {"func": lambda a, x=1, y="foo": (a, x, y)}
        func = Function[Int, {"x": Int, "y": Str}, Tuple[Int, Int, Str]]("func")

        res1 = func(1)
        assert interpreter.interpret(res1.graft, builtins)() == (1, 1, "foo")

        res2 = func(1, x=-1)
        assert interpreter.interpret(res2.graft, builtins)() == (1, -1, "foo")

        res2 = func(1, y="bar")
        assert interpreter.interpret(res2.graft, builtins)() == (1, 1, "bar")

        res2 = func(1, x=-1, y="bar")
        assert interpreter.interpret(res2.graft, builtins)() == (1, -1, "bar")

    def test_call_wrong_positional_args(self):
        func = Function[Int, Str, {}, Str]("func")

        with pytest.raises(
            ProxyTypeError, match=r"exactly 2 positional arguments \(1 given\)"
        ):
            func(1)

        with pytest.raises(
            ProxyTypeError, match=r"exactly 2 positional arguments \(3 given\)"
        ):
            func(1, 2, 3)

        with pytest.raises(
            ProxyTypeError, match=r"exactly 1 positional argument \(2 given\)"
        ):
            Function[Int, {}, Str]("func")(1, 2)

        with pytest.raises(
            ProxyTypeError, match=r"exactly 0 positional arguments \(2 given\)"
        ):
            Function[{}, Str]("func")(1, 2)

        with pytest.raises(
            ProxyTypeError,
            match=r"exactly 1 positional argument \(2 given\). Keyword arguments must be given by name",
        ):
            Function[Int, {"x": Int}, Str]("func")(1, 2)

        with pytest.raises(ProxyTypeError, match=r"Unexpected keyword argument 'z'"):
            Function[Int, {"b": Str}, Int]("func")(1, b="sdf", z="extra")

        with pytest.raises(
            ProxyTypeError, match=r"Unexpected keyword arguments 'q', 'z'"
        ):
            Function[Int, {"b": Str}, Int]("func")(
                1, b="sdf", z="extra", q="more extra"
            )

        with pytest.raises(ProxyTypeError, match=r"Expected .*Str.* for argument 0"):
            Function[Str, {}, Str]("func")(1)

        with pytest.raises(ProxyTypeError, match=r"Expected .*Int.* for argument 'x'"):
            Function[Str, {"x": Int}, Str]("func")("foo", x="not_int")

    def test_from_callable(self):
        def py_func(a, b, c):
            return (a + b) / c

        func = Function.from_callable(py_func)
        assert isinstance(func, Function[Any, Any, Any, {}, Any])

        result = func(7, 1, 4)
        interpreted = interpreter.interpret(
            result.graft, builtins={"add": operator.add, "div": operator.truediv}
        )()
        assert interpreted == 2

    def test_returns_closure(self):
        def outer(a):
            b = a + 1

            def inner(x, y):
                return (x + y) / b

            return inner

        func = Function[Int, {}, Function[Int, Int, {}, Float]](outer)

        proxy_inner = func(4)
        assert isinstance(proxy_inner, Function[Int, Int, {}, Float])
        result = proxy_inner(6, 4)

        interpreted = interpreter.interpret(
            result.graft, builtins={"add": operator.add, "div": operator.truediv}
        )()
        assert interpreted == 2

    def test_takes_function(self):
        def main(a, b, helper):
            return helper(a) / helper(b)

        func = Function[Int, Int, Function[Int, {}, Int], {}, Float](main)
        result = func(3, 1, lambda x: x + 1)

        interpreted = interpreter.interpret(
            result.graft, builtins={"add": operator.add, "div": operator.truediv}
        )()
        assert interpreted == 2
