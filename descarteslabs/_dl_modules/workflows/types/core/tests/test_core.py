from typing import Type
import pytest

from ..core import type_params_issubclass, merge_params
from .. import Proxytype, GenericProxytype, is_generic, validate_typespec
from ... import Struct, List, Tuple, Int, Float, Str

# NOTE: we test with non-Proxytype classes for this first test to be a little more hermetic,
# since the GenericProxytypeMetaclass messes around with `isinstance` (to recursively call `type_params_issubclass`)


class Alive(object):
    pass


class Animal(Alive):
    pass


class Bear(Animal):
    pass


class Plant(Alive):
    pass


class Spruce(Plant):
    pass


class TestTypeParamsIssubclass(object):
    def test_base_case(self):
        assert type_params_issubclass(Bear, Animal)
        assert type_params_issubclass(Bear, Alive)
        assert type_params_issubclass(Bear, Bear)

        assert not type_params_issubclass(Bear, Plant)
        assert not type_params_issubclass(Bear, Spruce)
        assert not type_params_issubclass(Plant, Spruce)
        assert not type_params_issubclass(Bear, int)

        assert not type_params_issubclass(Bear, (Animal,))
        assert not type_params_issubclass(Bear, {"x": Animal})
        assert not type_params_issubclass((Bear,), {"x": Animal})

    def test_tuples(self):
        assert type_params_issubclass(tuple(), tuple())
        assert type_params_issubclass((Bear,), (Animal,))
        assert type_params_issubclass((Bear, Spruce), (Animal, Plant))

        assert not type_params_issubclass((Bear, Spruce), (Animal, Animal))
        assert not type_params_issubclass((int, Alive), (Plant, Animal))
        assert not type_params_issubclass((Bear, Spruce), (Animal,))
        assert not type_params_issubclass((Bear,), (Animal, Plant))

    def test_dicts(self):
        assert type_params_issubclass({}, {})
        assert type_params_issubclass({"x": Spruce}, {"x": Plant})
        assert type_params_issubclass({"x": Spruce, "y": Bear}, {"x": Plant, "y": Bear})

        assert not type_params_issubclass({}, {"x": Plant})
        assert not type_params_issubclass({"foo": Spruce}, {"bar": Plant})
        assert not type_params_issubclass(
            {"x": Spruce, "y": Bear}, {"y": Bear, "x": Spruce}
        )
        assert not type_params_issubclass({"x": Spruce}, {"x": Animal})
        assert not type_params_issubclass(
            {"x": Spruce, "y": Bear}, {"x": Plant, "y": Plant}
        )

    def test_nested(self):
        assert type_params_issubclass({"x": tuple()}, {"x": tuple()})
        assert type_params_issubclass(({},), ({},))
        assert type_params_issubclass(
            ({"x": Bear, "y": Plant},), ({"x": Bear, "y": Alive},)
        )
        assert type_params_issubclass(
            ({"x": Bear, "y": Plant}, Spruce), ({"x": Bear, "y": Alive}, Plant)
        )
        assert type_params_issubclass(
            {"x": (Plant, (Spruce, Animal)), "y": Bear},
            {"x": (Alive, (Plant, Animal)), "y": Animal},
        )

        assert not type_params_issubclass({"x": tuple()}, {"x": (Bear,)})
        assert not type_params_issubclass(
            ({"x": Bear, "y": Plant},), ({"x": Bear, "y": Bear},)
        )
        assert not type_params_issubclass(
            {"x": (Plant, (Spruce, Animal)), "y": Bear},
            {"x": (Alive, (Animal, Animal)), "y": Animal},
        )


# Now we test the full `issubclass` on actual Proxytypes


class Foo(Proxytype):
    pass


class FooChild(Foo):
    pass


class Bar(Proxytype):
    pass


class BarChild(Bar):
    pass


class Containy(GenericProxytype):
    pass


class SubContainy(Containy):
    pass


class OtherContainy(GenericProxytype):
    pass


def test_singleton_concrete_subtypes():
    assert Containy[Foo] is Containy[Foo]
    assert Containy[Bar] is Containy[Bar]
    assert Containy[Foo] is not Containy[Bar]
    assert Containy[BarChild] is not Containy[Bar]

    assert Containy[Containy[Foo]] is Containy[Containy[Foo]]
    assert OtherContainy[Containy[Foo]] is OtherContainy[Containy[Foo]]
    assert Containy[Containy[Bar]] is not SubContainy[Containy[Bar]]

    assert SubContainy[Foo] is SubContainy[Foo]
    assert SubContainy[Foo] is not Containy[Foo]

    assert Containy[Foo] is not OtherContainy[Foo]

    assert Containy[1] is Containy[1]
    assert Containy[1] is not Containy[2]
    assert Containy[Containy[1]] is Containy[Containy[1]]
    assert SubContainy[1] is SubContainy[1]
    assert SubContainy[1] is not Containy[1]
    assert Containy[1] is not OtherContainy[1]


def test_subclasscheck_rejects_non_classes():
    with pytest.raises(TypeError, match="arg 1 must be a class"):
        issubclass(1, Containy[Foo])
    with pytest.raises(TypeError, match="arg 1 must be a class"):
        issubclass(Containy[Foo](), Containy[Foo])


class TestCovariantSubclass(object):
    def test_basic(self):
        assert issubclass(Proxytype, Proxytype)
        assert issubclass(GenericProxytype, GenericProxytype)
        assert issubclass(Containy, GenericProxytype)

        assert not issubclass(Bar, BarChild)
        assert not issubclass(Containy, SubContainy)
        assert not issubclass(Containy, OtherContainy)

    def test_concrete_basic(self):
        assert issubclass(Containy[Foo], Containy)
        assert issubclass(Containy[Foo], Containy[Foo])
        assert issubclass(Containy[SubContainy[Foo]], Containy[SubContainy[Foo]])
        assert issubclass(Containy[SubContainy[Foo]], Containy[SubContainy])

        assert not issubclass(Containy, Containy[Foo])
        assert not issubclass(OtherContainy[Foo], Containy[Foo])
        assert not issubclass(Containy[Foo], SubContainy[Foo])

    def test_concrete_with_value_basic(self):
        assert issubclass(Containy[1], Containy)
        assert issubclass(Containy[1], Containy[1])
        assert issubclass(Containy[Foo, 1], Containy[Foo, 1])
        assert issubclass(Containy[SubContainy[1]], Containy[SubContainy[1]])
        assert issubclass(Containy[SubContainy[1]], Containy[SubContainy])
        assert issubclass(Containy[FooChild, 1], Containy[Foo, 1])

        assert not issubclass(Containy, Containy[1])
        assert not issubclass(Containy[1], Containy[2])
        assert not issubclass(Containy[1], Containy[Int])
        assert not issubclass(Containy[Foo, 1], Containy[Foo, 2])
        assert not issubclass(Containy[FooChild, 1], Containy[Foo, 2])
        assert not issubclass(OtherContainy[1], Containy[1])
        assert not issubclass(Containy[1], SubContainy[1])

    def test_covariance(self):
        assert issubclass(Containy[BarChild], Containy[Bar])
        assert issubclass(SubContainy[Bar], Containy[Bar])
        assert issubclass(SubContainy[BarChild], Containy[Bar])
        assert issubclass(SubContainy[BarChild], Containy)

        assert not issubclass(Containy[Bar], Containy[BarChild])
        assert not issubclass(Containy[Bar], SubContainy[Bar])
        assert not issubclass(Containy[Bar], SubContainy[BarChild])
        assert not issubclass(Containy, SubContainy[BarChild])


class Subclasscheck(GenericProxytype):
    @classmethod
    def _issubclass(cls, other: Type["Subclasscheck"]) -> bool:
        "Allow anything to be a subclass if `my_type_params[1]` is True, otherwise normal behavior"
        my_contained, allow_any = cls._type_params
        other_contained, _ = other._type_params
        return issubclass(other_contained, my_contained) if not allow_any else True


def test_custom_subclasscheck():
    assert issubclass(Subclasscheck[Foo, False], Subclasscheck[Foo, False])
    assert issubclass(Subclasscheck[FooChild, False], Subclasscheck[Foo, False])

    # custom subclass check is being used: things pass as subclasses that shouldn't by normal logic
    assert issubclass(Subclasscheck[Foo, True], Subclasscheck[Foo, True])
    assert issubclass(Subclasscheck[Bar, True], Subclasscheck[Foo, True])
    assert issubclass(Subclasscheck[Bar, False], Subclasscheck[Foo, True])
    assert issubclass(Subclasscheck["x", False], Subclasscheck[Foo, True])

    # custom subclass check not used when the generic types don't match
    assert not issubclass(Containy["x", False], Subclasscheck[Foo, True])
    assert not issubclass(int, Subclasscheck[Foo, True])


class CustomizedClassGetitem(GenericProxytype):
    @staticmethod
    def __class_getitem_hook__(name, bases, dct, type_params):
        dct["my_name"] = name
        dct["foo"] = type_params[0]
        dct["bar"] = type_params[1]

        return name, bases, dct


def test_class_getitem_hook():
    custom = CustomizedClassGetitem[Foo, Bar]

    assert custom.my_name == "CustomizedClassGetitem[Foo, Bar]"
    assert custom.foo is Foo
    assert custom.bar is Bar


class InitSubclasser(GenericProxytype):
    @classmethod
    def __init_subclass__(subcls):
        assert subcls is not InitSubclasser
        assert subcls.foo == "bar"
        subcls.bar = "baz"


def test_init_subclass():
    class Subclass(InitSubclasser):
        foo = "bar"

    assert Subclass.foo == "bar"  # on base
    assert Subclass.bar == "baz"  # added by __init_subclass__

    assert not hasattr(InitSubclasser, "foo")
    assert not hasattr(InitSubclasser, "bar")


def test_is_generic_list():
    assert is_generic(List)


def test_not_is_generic_int():
    assert not is_generic(Int)


def test_not_is_generic_list_of_ints():
    assert not is_generic(List[Int])


def test_is_generic_on_list_of_generic_list():
    assert is_generic(List[List])


def test_not_is_generic_on_list_of_list():
    assert not is_generic(List[List[Int]])


def test_validate_typespec():
    validate_typespec((1,))
    validate_typespec((Int,))
    validate_typespec((List[Int],))
    validate_typespec((Int, Float, Str))
    validate_typespec({"a": "test"})
    validate_typespec({"a": List[Int], "b": 1, "c": Struct[{"x": Tuple[Int, Float]}]})

    with pytest.raises(TypeError, match="must be Proxytypes, Python primitive values"):
        validate_typespec((int,))
        validate_typespec((List[int],))
        validate_typespec((Int, Float, str))
        validate_typespec({"a": str})
        validate_typespec(
            {"a": List[Int], "b": 1, "c": Struct[{"x": Tuple[Int, float]}]}
        )


class TestProxytypePyInterfaceErrors:
    def test_bool(self):
        with pytest.raises(
            TypeError, match="Truth value of Proxytype Foo objects is not supported"
        ):
            bool(Foo())

    def test_contains(self):
        with pytest.raises(TypeError, match="object of type Foo does not support `in`"):
            Foo() in Foo()

        class HasContains(Proxytype):
            def contains(self, other):
                pass

        with pytest.raises(
            TypeError, match=r"Please use HasContains\.contains\(other\)"
        ):
            HasContains() in HasContains()

    def test_len(self):
        with pytest.raises(TypeError, match="object of type Foo has no len()"):
            len(Foo())

        class HasLength(Proxytype):
            def length(self):
                pass

        with pytest.raises(TypeError, match=r"Please use HasLength\.length\(\)"):
            len(HasLength())

    def test_iter(self):
        with pytest.raises(TypeError, match="object of type Foo is not iterable"):
            iter(Foo())

        class HasMap(Proxytype):
            def map(self, func):
                pass

        with pytest.raises(
            TypeError,
            match=(
                r"Proxy HasMap is not iterable\. "
                r"Consider using HasMap\.map\(\.\.\.\)"
            ),
        ):
            iter(HasMap())


class TestMergeParameters:
    def test_basic(self):
        x = Int._as_param("x")
        y = Int._as_param("y")
        z = Float._as_param("z")

        assert merge_params() == ()
        assert merge_params(x) == (x,)
        assert merge_params(x, y, z) == (x, y, z)
        assert merge_params(x, "foo", y, 1234, z) == (x, y, z)

    def test_from_apply(self):
        x = Int._as_param("x")
        y = Int._as_param("y")
        z = Float._as_param("z")

        res = Str._from_apply("foo_func", y, bar=z)
        assert res.params == (y, z)

        res_funcparam = Str._from_apply(x, y, bar=z)
        assert res_funcparam.params == (x, y, z)

    def test_dedup(self):
        x = Int._as_param("x")
        y = Int._as_param("y")

        assert merge_params(x, x, x) == (x,)
        assert merge_params(x, x, y) == (x, y)
        assert merge_params(x, y, x) == (x, y)

    def test_duplicate_name_error(self):
        x = Int._as_param("x")
        x2 = Int._as_param("x")
        y = Int._as_param("y")

        with pytest.raises(
            ValueError, match="If these parameters 'x' mean different things"
        ):
            merge_params(x, y, x2)
