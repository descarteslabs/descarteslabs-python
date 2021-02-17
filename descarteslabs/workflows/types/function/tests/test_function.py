import operator
import pytest
import mock
import inspect
import typing

from descarteslabs.common.graft import client, interpreter

from ...core import ProxyTypeError
from ...primitives import Any, Int, Str, Float, Number
from ...containers import Dict, Tuple
from ...identifier import parameter

from .. import Function


class TestDelay(object):
    @pytest.mark.parametrize("returns", [Int, None])
    def test_delay(self, returns):
        result = []

        def delayable(x, a, b):
            assert isinstance(x, Float)
            assert isinstance(a, Dict[Str, Int])
            assert isinstance(b, Str)
            res = a[b]
            result.append(res)
            return res

        delayed = Function._delay(delayable, returns, Float, b=Str, a=Dict[Str, Int])
        assert isinstance(delayed, Int)
        assert delayed.graft == client.function_graft(result[0], "x", "a", "b")

    def test_delay_wrongargs(self):
        def delayable(a):
            pass

        with pytest.raises(TypeError, match="too many positional arguments"):
            Function._delay(delayable, Int, Dict[Str, Int], Str)

        with pytest.raises(TypeError, match="unexpected keyword argument 'foo'"):
            Function._delay(delayable, Int, a=Str, foo=Int)

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

    @pytest.mark.parametrize("ret_type", [None, Str])
    def test_delay_propagate_params(self, ret_type):
        p = parameter("p", Int)

        def delayable(a):
            return a * p

        delayed = Function._delay(delayable, ret_type, Str)
        assert isinstance(delayed, Str)
        assert delayed.params == (p,)

    def test_delay_bad_params(self):
        def bad_params(a, b, **kwargs):
            pass

        with pytest.raises(TypeError, match=r"kind VAR_KEYWORD, used for \*\*kwargs"):
            Function._delay(bad_params, None)

        def has_defaults(a, b=1):
            pass

        with pytest.raises(TypeError, match="Parameter b=1 has a default value"):
            Function._delay(has_defaults, None)


class TestSubclasscheck:
    def test_identity(self):
        assert issubclass(Function[{}, Int], Function[{}, Int])
        assert issubclass(Function[Str, Int, {}, Int], Function[Str, Int, {}, Int])
        assert issubclass(
            Function[Str, Int, dict(x=Int, y=Float), Int],
            Function[Str, Int, dict(x=Int, y=Float), Int],
        )

    def test_positionality(self):
        # named args treated positionally *on the subclass only*
        assert issubclass(
            Function[dict(x=Str, y=Int), Int],
            Function[Str, Int, {}, Int],
        )
        assert issubclass(
            Function[Str, dict(y=Int), Int],
            Function[Str, Int, {}, Int],
        )

        # unnamed args are not subclasses of named args
        assert not issubclass(
            Function[Str, Int, {}, Int],
            Function[dict(x=Str, y=Int), Int],
        )
        assert not issubclass(
            Function[Str, Int, {}, Int],
            Function[Str, dict(y=Int), Int],
        )

    def test_names(self):
        # names must match
        assert not issubclass(
            Function[dict(x=Int, y=Float), Int],
            Function[dict(a=Int, b=Float), Int],
        )

        # name order matters
        assert not issubclass(
            Function[dict(x=Int, y=Float), Int],
            Function[dict(y=Float, x=Int), Int],
        )

    def test_covariant_return_type(self):
        assert issubclass(Function[{}, Int], Function[{}, Number])
        assert not issubclass(Function[{}, Number], Function[{}, Int])

    def test_contravariant_args(self):
        assert issubclass(Function[Number, {}, Int], Function[Int, {}, Int])
        assert issubclass(Function[Number, {}, Int], Function[Float, {}, Int])
        assert issubclass(
            Function[dict(x=Str, y=Number), Int], Function[Str, Float, {}, Int]
        )


class TestFunction:
    def test_init_unparameterized(self):
        with pytest.raises(TypeError, match="Cannot instantiate a generic Function"):
            Function("asdf")

    def test_init_kwargs(self):
        with pytest.raises(TypeError, match="Parameter foo=1 has a default value."):
            Function[{"foo": Int}, Int](lambda foo=1: foo)

        func = Function[{"foo": Int}, Int](lambda foo: foo)
        assert client.is_delayed(func)
        interpreted_func = interpreter.interpret(func.graft)
        assert interpreted_func(1) == 1
        assert interpreted_func(foo=1) == 1

    def test_init_str(self):
        func = Function[{}, Int]("foo")
        assert func.graft == client.keyref_graft("foo")
        assert func.params == ()

    def test_init_other_function(self):
        ftype = Function[Int, Float, {}, Int]

        f1 = ftype("bar")
        from_f1 = ftype(f1)
        assert from_f1.graft is f1.graft
        assert from_f1.params is f1.params

        # compatible subclasses also work
        f2 = Function[Number, {"x": Number}, Int]("bar")
        from_f2 = ftype(f2)
        assert from_f2.graft is f2.graft
        assert from_f2.params is f2.params

        # incompatible subclasses don't work (different return type)
        f3 = Function[Int, Float, {}, Str]("bar")
        with pytest.raises(TypeError, match="signatures are incompatible"):
            ftype(f3)

    def test_init_callable(self):
        func = Function[{}, Int](lambda: 1)
        assert client.is_delayed(func)
        interpreted_func = interpreter.interpret(func.graft)
        assert interpreted_func() == 1

    def test_init_callable_propagate_params(self):
        p = parameter("p", Int)
        func = Function[Float, {}, Float](lambda x: x + p)
        assert func.params == (p,)
        assert func(0.0).params == (p,)

        q = parameter("q", Float)
        assert func(q).params == (p, q)

    def test_has_signature(self):
        f = Function[Int, dict(x=Str, y=Float), Str]
        sig = inspect.signature(f)

        assert list(sig.parameters) == ["implicit0", "x", "y"]
        assert [p.kind for p in sig.parameters.values()] == [
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ]
        assert [p.annotation for p in sig.parameters.values()] == [Int, Str, Float]

        assert sig.return_annotation is Str

    def test_has_annotations(self):
        f = Function[Int, dict(x=Str, y=Float), Str]
        assert typing.get_type_hints(f) == {"x": Str, "y": Float, "return": Str}

    def test_call(self):
        def delayable(a, b):
            assert isinstance(a, Int)
            assert isinstance(b, Str)
            return a

        func = Function[Int, Str, {}, Int](delayable)
        result = func(1, "foo")
        assert isinstance(result, Int)
        assert interpreter.interpret(result.graft)() == 1

    @pytest.mark.parametrize(
        "call_args, call_kwargs",
        [
            ((1, 2, "x"), {}),
            ((1, 2), {"b": "x"}),
            ((1,), {"a": 2, "b": "x"}),
            ((1,), {"b": "x", "a": 2}),
        ],
    )
    def test_call_proper_binding(self, call_args, call_kwargs):
        func = Function[Int, dict(a=Int, b=Str), Int]("foo")

        builtins = {"foo": lambda *args, **kwargs: (args, kwargs)}
        res = func(*call_args, **call_kwargs)
        args, kwargs = interpreter.interpret(res.graft, builtins)()

        # positional args should always be given positionally in the graft
        assert args == (1,)
        # named args should always be given by name in the graft, and in the right order,
        # even if given positionally or in a different order in py
        assert list(kwargs.items()) == [("a", 2), ("b", "x")]

    def test_call_wrong_positional_args(self):
        with pytest.raises(TypeError, match="missing a required argument: 'implicit1'"):
            Function[Int, Str, {}, Str]("func")(1)

        with pytest.raises(TypeError, match="too many positional arguments"):
            Function[Int, Str, {}, Str]("func")(1, 2, 3)

        with pytest.raises(TypeError, match="got an unexpected keyword argument 'z'"):
            Function[Int, {"b": Str}, Int]("func")(1, b="sdf", z="extra")

        with pytest.raises(TypeError, match=r"Expected .*Str.* for argument 0"):
            Function[Str, {}, Str]("func")(1)

        with pytest.raises(TypeError, match=r"Expected .*Int.* for argument 'x'"):
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
        with pytest.raises(AssertionError, match="must be valid Python identifiers"):
            Function[Int, {"0foo": Str}, Str]
        with pytest.raises(AssertionError, match="must be valid Python identifiers"):
            Function[Int, {"return": Str}, Str]
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
        assert isinstance(func, Function[dict(a=Int, b=Float, c=Int), Float])
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
        assert isinstance(func, Function[dict(a=Int, b=Float, c=Int), Float])

    def test_from_callable_closure_propagates_names(self):
        def outer(a: Int, b: Float):
            c = Int(a / b)

            def inner(y: Str, z: Str):
                return (y + z) * c

            return inner

        func = Function.from_callable(outer)
        assert isinstance(
            func, Function[dict(a=Int, b=Float), Function[dict(y=Str, z=Str), Str]]
        )

    def test_from_callable_proxyfunc(self):
        func = Function[Int, {}, Int]("foo")
        func_named = Function[dict(x=Int), Int]("foo")

        with pytest.raises(
            TypeError, match="Cannot call `from_callable` on a concrete Function type"
        ):
            func.from_callable(func)

        assert Function.from_callable(func) is func
        assert Function.from_callable(func_named) is func_named

        assert Function.from_callable(func, Int) is func
        assert Function.from_callable(func_named, Int) is func_named

        class SubInt(Int):
            pass

        assert Function.from_callable(func, SubInt, return_type=Int) is func
        assert Function.from_callable(func_named, SubInt, return_type=Int) is func_named

        with pytest.raises(TypeError, match="Their signatures are incompatible"):
            Function.from_callable(func, Str)

        with pytest.raises(TypeError, match="Their signatures are incompatible"):
            Function.from_callable(func, return_type=Str)

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
        global_p = parameter("global", Int)
        # ^ will be 1; use to track closure correctness

        def outer(a):
            b = a + global_p

            def inner(x, y):
                return (x + y) / b

            return inner

        func = Function[Int, {}, Function[Int, Int, {}, Float]](outer)
        assert func.params == (global_p,)

        global_value = 1
        first_call_arg = 4

        proxy_inner = func(4)
        assert isinstance(proxy_inner, Function[Int, Int, {}, Float])
        assert proxy_inner.params == (global_p,)
        result_2 = proxy_inner(6, 4)
        result_3 = proxy_inner(10, 5)
        result = result_2 + result_3
        assert result.params == (global_p,)

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
        assert tuple(p._name for p in func.params) == ("ext1", "ext2")

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
