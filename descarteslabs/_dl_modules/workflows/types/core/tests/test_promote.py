import pytest

from ..promote import _promote, _resolve_lambdas
from .. import typecheck_promote, Proxytype
from ...primitives import Int, Float, Str, Bool, NoneType
from ...containers import Tuple, List


class TestPromote(object):
    def test_promote_proxytype(self):
        promoted = _promote(1, Int, None, "fake_name")
        assert isinstance(promoted, Int)

    def test_promote_union(self):
        promoted = _promote(1, (Str, Int), None, "fake_name")
        assert isinstance(promoted, Int)

    def test_proxytype_fails(self):
        with pytest.raises(
            TypeError,
            match="Argument 'baz' to function fake_name: expected Int or an object promotable to that",
        ):
            _promote("foo", Int, "baz", "fake_name")

    def test_union_fails(self):
        with pytest.raises(
            TypeError,
            match=r"Argument 0 to function fake_name: expected Int, Float or an object promotable to those",
        ):
            _promote("foo", (Int, Float), 0, "fake_name")


def test_resolve_lambdas():
    assert _resolve_lambdas((Int, lambda: Int)) == (Int, Int)
    assert _resolve_lambdas(dict(foo=Int, bar=lambda: Int)) == dict(foo=Int, bar=Int)
    assert _resolve_lambdas(Int) is Int


@typecheck_promote(Int, Tuple[Float, Str], x=List[Bool], y=NoneType)
def func(a1, a2, x=None, y=None):
    assert isinstance(a1, Int)
    assert isinstance(a2, Tuple[Float, Str])
    assert isinstance(x, List[Bool])
    assert isinstance(y, NoneType)


class TestDecorator(object):
    def test_decorator(self):
        func(42, (6.0, "foo"), x=[True, False], y=None)

    def test_decorator_rejects(self):
        with pytest.raises(TypeError, match=r"Argument 'a2' to function func\(\)"):
            func(42, (0.0, 0.0), x=[True, False], y=None)
        with pytest.raises(TypeError, match=r"Argument 'x' to function func\(\)"):
            func(42, (0.0, "foo"), x=[True, "baz"], y=None)

    def test_decorator_bad_args(self):
        with pytest.raises(
            TypeError, match="missing 1 required positional argument: 'a2'"
        ):
            func(42, x=[True, False], y=None)

        with pytest.raises(TypeError, match="multiple values for argument 'x'"):
            func(42, (0.0, "foo"), "extra", x=[True, False], y=None)

        with pytest.raises(TypeError, match="got an unexpected keyword argument 'z'"):
            func(42, (6.0, "foo"), z="wat")

    def test_method_decorator_only_evals_lambdas_once(self):
        calls = []

        def param():
            calls.append(None)
            return Int

        @typecheck_promote(param)
        def foo(x):
            assert isinstance(x, Int)

        foo(1)
        assert len(calls) == 1
        foo(2)
        assert len(calls) == 1

    def test_decorator_positional_kwargs(self):
        @typecheck_promote(Int, Bool)
        def with_varargs(a1, a2=True):
            assert isinstance(a1, Int)
            assert isinstance(a2, Bool)

        with_varargs(1, a2=False)
        with_varargs(1, False)

    def test_wrong_decorator_fails(self):
        with pytest.raises(TypeError, match="missing a required argument: 'y'"):
            typecheck_promote(Int)(lambda x, y: None)

        with pytest.raises(TypeError, match="too many positional arguments"):
            typecheck_promote(Int, Int, Int)(lambda x, y: None)

        with pytest.raises(TypeError, match="multiple values for argument 'x'"):
            typecheck_promote(Int, Int, x=Int)(lambda x, y: None)

    def test_default_args_promoted(self):
        @typecheck_promote(x=List[Int])
        def func(x=(1, 2)):
            assert isinstance(x, List[Int])

        func([1, 2, 3])
        func()

    def test_no_self(self):
        @typecheck_promote(lambda self: self)
        def func(x):
            pass

        with pytest.raises(TypeError, match="missing 1 required positional argument"):
            func(1)


class WithPromotedMethods(Proxytype):
    cls_member_type = Bool

    def __init__(self, member_type=Bool):
        self.member_type = member_type

    @typecheck_promote(Int, x=Float)
    def basic(self, an_int, x=None):
        assert isinstance(self, WithPromotedMethods)
        assert isinstance(an_int, Int)
        assert isinstance(x, Float)

    @typecheck_promote(Int, lambda: WithPromotedMethods, x=Float)
    def uses_lambda_positional(self, an_int, self_obj, x=None):
        assert isinstance(self, WithPromotedMethods)
        assert isinstance(an_int, Int)
        assert isinstance(self_obj, WithPromotedMethods)
        assert isinstance(x, Float)

    @typecheck_promote(Int, x=Float, y=lambda: WithPromotedMethods)
    def uses_lambda_kwarg(self, an_int, x=None, y=None):
        assert isinstance(self, WithPromotedMethods)
        assert isinstance(an_int, Int)
        assert isinstance(x, Float)
        assert isinstance(y, WithPromotedMethods)

    @typecheck_promote(lambda self: self.member_type)
    def lambda_with_self(self, x):
        assert isinstance(x, self.member_type)

    @typecheck_promote(x=lambda self: self.member_type)
    def lambda_with_self_kwarg(self, x=True):
        assert isinstance(x, self.member_type)

    @classmethod
    @typecheck_promote(Int)
    def a_classmethod(cls, x):
        assert cls is WithPromotedMethods
        assert isinstance(x, Int)

    @classmethod
    @typecheck_promote(Int, lambda: WithPromotedMethods)
    def a_classmethod_lambda(cls, x, my_own_type):
        assert cls is WithPromotedMethods
        assert isinstance(x, Int)
        assert isinstance(my_own_type, WithPromotedMethods)

    @classmethod
    @typecheck_promote(lambda cls: cls.cls_member_type)
    def a_classmethod_lambda_with_self(cls, x):
        assert isinstance(x, cls.cls_member_type)


class TestDecoratorOnClasses(object):
    def test_method_decorator_basic(self):
        obj = WithPromotedMethods()
        obj.basic(1, x=2.2)

    def test_method_decorator_basic_wrong_args(self):
        obj = WithPromotedMethods()
        with pytest.raises(
            TypeError, match="missing 1 required positional argument: 'an_int'"
        ):
            obj.basic(x=2.2)

    def test_method_decorator_selftype(self):
        obj = WithPromotedMethods()
        obj2 = WithPromotedMethods()
        obj.uses_lambda_positional(1, obj, x=2.2)
        obj.uses_lambda_positional(1, obj2, x=2.2)

        obj.uses_lambda_kwarg(1, x=2.2, y=obj)
        obj.uses_lambda_kwarg(1, x=2.2, y=obj2)

    def test_method_decorator_selftype_wrong_args(self):
        obj = WithPromotedMethods()
        with pytest.raises(
            TypeError,
            match=r"Argument 'self_obj' to function uses_lambda_positional\(\)",
        ):
            obj.uses_lambda_positional(1, "wrong_type", x=2.2)

        with pytest.raises(
            TypeError, match=r"Argument 'y' to function uses_lambda_kwarg\(\)"
        ):
            obj.uses_lambda_kwarg(1, x=2.2, y="wrong_type")

    def test_method_decorator_lambda_self(self):
        obj = WithPromotedMethods()
        obj.lambda_with_self(False)
        obj.lambda_with_self_kwarg(x=False)

        obj2 = WithPromotedMethods(Str)
        obj2.lambda_with_self("foo")
        obj2.lambda_with_self_kwarg(x="bar")

    def test_classmethod(self):
        obj = WithPromotedMethods()
        obj.a_classmethod(1)
        obj.a_classmethod_lambda(1, obj)
        with pytest.raises(
            TypeError, match=r"Argument 'x' to function a_classmethod\(\): expected Int"
        ):
            obj.a_classmethod(None)
        with pytest.raises(
            TypeError,
            match=r"Argument 'my_own_type' to function a_classmethod_lambda\(\): expected WithPromotedMethods",
        ):
            obj.a_classmethod_lambda(1, 2)

    def test_classmethod_lambda_self(self):
        obj = WithPromotedMethods(Int)
        obj.a_classmethod_lambda_with_self(False)
