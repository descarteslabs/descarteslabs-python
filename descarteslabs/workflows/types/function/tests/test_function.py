import operator
import pytest
import mock

from descarteslabs.common.graft import client, interpreter

from ...core import ProxyTypeError
from ...primitives import Any, Int, Str, Float
from ...containers import Dict, Tuple
from ...identifier import parameter

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

    def test_validate_params(self):
        Function[Int, {}, Str]
        Function[Int, {"a": Any}, Str]

        with pytest.raises(
            TypeError, match="argument type parameters must be Proxytypes"
        ):
            Function[1, 2, 3]
        with pytest.raises(
            AssertionError, match="kwarg type parameters must be a dict"
        ):
            Function[Int, Str]
        with pytest.raises(
            AssertionError, match="Keyword argument names must be strings"
        ):
            Function[Int, {1: Str}, Str]
        with pytest.raises(TypeError, match="kwarg type parameters must be Proxytypes"):
            Function[Int, {"a": 1}, Str]
        with pytest.raises(
            TypeError, match="return type parameter must be a Proxytype"
        ):
            Function[Int, {}, "test"]

    def test_from_callable(self):
        def py_func(a: Int, b: Float, c: Int) -> Float:
            "my func"
            return (a + b) / c

        func = Function.from_callable(py_func)
        assert isinstance(func, Function[Int, Float, Int, {}, Float])
        assert func.__doc__ == "my func"

        result = func(7, 1.0, 4)
        interpreted = interpreter.interpret(
            result.graft,
            builtins={
                "wf.numpy.add": operator.add,
                "wf.numpy.true_divide": operator.truediv,
            },
        )()
        assert interpreted == 2

    def test_from_callable_infer_return(self):
        def py_func(a: Int, b: Float, c: Int):
            return (a + b) / c

        func = Function.from_callable(py_func)
        assert isinstance(func, Function[Int, Float, Int, {}, Float])

    def test_from_callable_proxyfunc(self):
        func = Function[Int, {}, Int]("foo")

        assert Function.from_callable(func) is func
        assert Function.from_callable(func, Int) is func

        with pytest.raises(TypeError, match="Expected a Function with parameters"):
            Function.from_callable(func, Str)

    def test_from_callable_bad_annotations(self):
        def py_func(a: Int, b: int):
            pass

        with pytest.raises(
            TypeError,
            match="For parameter 'b' to function 'py_func': type annotation must be a Proxytype",
        ):
            Function.from_callable(py_func)

        def py_func(a: Int, b: Int) -> float:
            pass

        with pytest.raises(
            TypeError,
            match="For return type of function 'py_func': type annotation must be a Proxytype",
        ):
            Function.from_callable(py_func)

        def py_func(a: Int, b) -> Int:
            pass

        with pytest.raises(
            TypeError,
            match="No type annotation given for parameter 'b' to function 'py_func'",
        ):
            Function.from_callable(py_func)

    def test_returns_closure(self):
        def outer(a):
            b = a + parameter("global", Int)  # 1; use to track closure correctness

            def inner(x, y):
                return (x + y) / b

            return inner

        func = Function[Int, {}, Function[Int, Int, {}, Float]](outer)

        global_value = 1
        first_call_arg = 4

        proxy_inner = func(4)
        assert isinstance(proxy_inner, Function[Int, Int, {}, Float])
        result_2 = proxy_inner(6, 4)
        result_3 = proxy_inner(10, 5)
        result = result_2 + result_3

        m = mock.MagicMock()
        m.__radd__.side_effect = lambda other: other + global_value

        interpreted = interpreter.interpret(
            result.graft,
            builtins={
                "wf.numpy.add": operator.add,
                "wf.numpy.true_divide": operator.truediv,
                "global": m,
            },
        )()
        assert interpreted == 5
        m.__radd__.assert_called_once_with(first_call_arg)

    def test_takes_function(self):
        def main(a, b, helper):
            return helper(a) / helper(b)

        func = Function[Int, Int, Function[Int, {}, Int], {}, Float](main)
        result = func(3, 1, lambda x: x + 1)

        interpreted = interpreter.interpret(
            result.graft,
            builtins={
                "wf.numpy.add": operator.add,
                "wf.numpy.true_divide": operator.truediv,
            },
        )()
        assert interpreted == 2

    def test_very_higher_order(self):
        ext1_value = -1
        ext2_value = -10

        def make_function(proxy=True):
            ext1 = parameter("ext1", Int) if proxy else ext1_value

            def func_a(a_1):
                x = a_1 + ext1

                def func_b(b_1, b_2):
                    y = b_1 - b_2 + (parameter("ext2", Int) if proxy else ext2_value)

                    def func_c(c_1):
                        return x + y + c_1 + ext1

                    return func_c

                return func_b

            return func_a

        func = Function[Int, {}, Function[Int, Int, {}, Function[Int, {}, Int]]](
            make_function()
        )

        def do_operation(f):
            b = f(2)
            c1 = b(1, 3)
            c2 = b(2, 4)
            result = c1(0) - c1(5) + c2(10) + c2(3)
            return result

        proxy_result = do_operation(func)
        real_result = do_operation(make_function(proxy=False))

        ext1 = mock.MagicMock()
        ext1.__radd__.side_effect = lambda other: ext1_value + other
        ext2 = mock.MagicMock()
        ext2.__radd__.side_effect = lambda other: ext2_value + other

        interpreted = interpreter.interpret(
            proxy_result.graft,
            builtins={
                "wf.numpy.add": operator.add,
                "wf.numpy.subtract": operator.sub,
                "ext1": ext1,
                "ext2": ext2,
            },
        )()

        assert interpreted == real_result
        assert len(ext1.__radd__.mock_calls) == 5  # 4 `c` calls + 1 to construct `x`
        assert len(ext2.__radd__.mock_calls) == 2  # 2 `b` calls
