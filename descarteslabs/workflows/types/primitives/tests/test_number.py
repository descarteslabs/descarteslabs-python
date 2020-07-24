import operator

import pytest
import numpy as np

from ...core import ProxyTypeError
from ...containers import Tuple, List
from ..bool_ import Bool
from ..string import Str
from ..number import Float, Int, Number, _binop_result

from ...core.tests.utils import operator_test


class TestPromote(object):
    def test_number_unpromotable(self):
        with pytest.raises(ProxyTypeError):
            Number._promote(2.2)
        with pytest.raises(ProxyTypeError):
            Number._promote(0)

    def test_primitives(self):
        assert isinstance(Int._promote(0), Int)
        assert isinstance(Float._promote(2.2), Float)

    def test_proxytypes(self):
        assert isinstance(Int._promote(Int(0)), Int)
        assert isinstance(Float._promote(Float(2.2)), Float)

    def test_wrong_primitives(self):
        with pytest.raises(ProxyTypeError):
            Int._promote(2.2)
        with pytest.raises(ProxyTypeError):
            Float._promote(0)

    def test_wrong_proxytypes(self):
        with pytest.raises(
            ProxyTypeError, match=r"You need to convert it explicitly, like `Int\(x\)`"
        ):
            Int._promote(Float(2.2))
        with pytest.raises(
            ProxyTypeError,
            match=r"You need to convert it explicitly, like `Float\(x\)`",
        ):
            Float._promote(Int(0))


class TestConstruct(object):
    def test_explicit_cast_passthrough(self):
        i = Int(Int(1))
        assert i.graft[i.graft["returns"]] == 1

    def test_explicit_cast_to_int(self):
        i = Int(Float(1.0))
        assert isinstance(i, Int)
        assert i.graft[i.graft["returns"]][0] == "wf.Int.cast"

        i = Int(Bool(True))
        assert isinstance(i, Int)
        assert i.graft[i.graft["returns"]][0] == "wf.Int.cast"

        i = Int(Str("1"))
        assert isinstance(i, Int)
        assert i.graft[i.graft["returns"]][0] == "wf.Int.cast"

    def test_explicit_cast_to_float(self):
        f = Float(Int(1))
        assert isinstance(f, Float)
        assert f.graft[f.graft["returns"]][0] == "wf.Float.cast"

        f = Float(Bool(True))
        assert isinstance(f, Float)
        assert f.graft[f.graft["returns"]][0] == "wf.Float.cast"

        f = Float(Str("1"))
        assert isinstance(f, Float)
        assert f.graft[f.graft["returns"]][0] == "wf.Float.cast"


class TestNumPyScalars(object):
    @pytest.mark.parametrize(
        "val",
        [
            np.uint8(1),
            np.uint16(1),
            np.uint32(1),
            np.uint64(1),
            np.int8(1),
            np.int16(1),
            np.int32(1),
            np.int64(1),
        ],
    )
    def test_int(self, val):
        i = Int(val)
        assert isinstance(i.graft[i.graft["returns"]], int)

    @pytest.mark.parametrize("val", [np.float16(1), np.float32(1), np.float64(1)])
    def test_float(self, val):
        i = Float(val)
        assert isinstance(i.graft[i.graft["returns"]], float)

    def test_failure(self):
        with pytest.raises(TypeError):
            Float(np.int32(1))
        with pytest.raises(TypeError):
            Int(np.float64(1))
        with pytest.raises(TypeError):
            Int(np.datetime64("2020-01-01"))


@pytest.mark.parametrize(
    "a, b, expected",
    [
        (Int(0), Int(0), Int),
        (Float(0.0), Float(0.0), Float),
        (Int(0), Float(0.0), Float),
        (Float(0.0), Int(0), Float),
    ],
)
def test_binop_result(a, b, expected):
    assert _binop_result(a, b) == expected


class TestAllOperators(object):
    int_obj = Int(0)
    float_obj = Float(0.0)
    all_values_to_try = [Int(1), Float(2.2), Bool(True), List[Int]([1, 2])]
    # ^ we use pre-promoted Proxytypes, not py types, since the `operator_test`
    # helper checks if `type(value) is in accepted_types`

    @pytest.mark.parametrize(
        "operator, accepted_types, return_type",
        [
            ["__abs__", (), Int],
            ["__add__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__div__", (Int, Float, Bool), (Int, Float)],
            [
                "__divmod__",
                (Int, Float, Bool),
                {
                    Float: Tuple[Float, Float],
                    Int: Tuple[Int, Int],
                    Bool: Tuple[Int, Int],
                },
            ],
            ["__eq__", (Int, Float, Bool), Bool],
            ["__floordiv__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__ge__", (Int, Float, Bool), Bool],
            ["__gt__", (Int, Float, Bool), Bool],
            ["__invert__", (), Int],
            ["__le__", (Int, Float, Bool), Bool],
            ["__lt__", (Int, Float, Bool), Bool],
            ["__mod__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__mul__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__ne__", (Int, Float, Bool), Bool],
            ["__neg__", (), Int],
            ["__pos__", (), Int],
            ["__pow__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__radd__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__rdiv__", (Int, Float, Bool), (Int, Float)],
            [
                "__rdivmod__",
                (Int, Float, Bool),
                {
                    Float: Tuple[Float, Float],
                    Int: Tuple[Int, Int],
                    Bool: Tuple[Int, Int],
                },
            ],
            ["__rfloordiv__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__rmod__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__rmul__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__rpow__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__rsub__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__rtruediv__", (Int, Float, Bool), (Int, Float)],
            ["__sub__", (Int, Float, Bool), {Float: Float, Int: Int, Bool: Int}],
            ["__truediv__", (Int, Float, Bool), (Int, Float)],
            # Int-specific methods
            ["__and__", [Int, Bool], Int],
            ["__lshift__", [Int, Bool], Int],
            ["__or__", [Int, Bool], Int],
            ["__rand__", [Int, Bool], Int],
            ["__rlshift__", [Int, Bool], Int],
            ["__ror__", [Int, Bool], Int],
            ["__rrshift__", [Int, Bool], Int],
            ["__rshift__", [Int, Bool], Int],
            ["__rxor__", [Int, Bool], Int],
            ["__xor__", [Int, Bool], Int],
        ],
    )
    def test_all_operators_int(self, operator, accepted_types, return_type):
        operator_test(
            self.int_obj, self.all_values_to_try, operator, accepted_types, return_type
        )

    @pytest.mark.parametrize(
        "operator, accepted_types, return_type",
        [
            ["__abs__", (), Float],
            ["__add__", (Int, Float, Bool), Float],
            ["__div__", (Int, Float, Bool), Float],
            ["__divmod__", (Int, Float, Bool), Tuple[Float, Float]],
            ["__eq__", (Int, Float, Bool), Bool],
            ["__floordiv__", (Int, Float, Bool), Float],
            ["__ge__", (Int, Float, Bool), Bool],
            ["__gt__", (Int, Float, Bool), Bool],
            ["__invert__", (), Float],
            ["__le__", (Int, Float, Bool), Bool],
            ["__lt__", (Int, Float, Bool), Bool],
            ["__mod__", (Int, Float, Bool), Float],
            ["__mul__", (Int, Float, Bool), Float],
            ["__ne__", (Int, Float, Bool), Bool],
            ["__neg__", (), Float],
            ["__pos__", (), Float],
            ["__pow__", (Int, Float, Bool), Float],
            ["__radd__", (Int, Float, Bool), Float],
            ["__rdiv__", (Int, Float, Bool), Float],
            ["__rdivmod__", (Int, Float, Bool), Tuple[Float, Float]],
            ["__rfloordiv__", (Int, Float, Bool), Float],
            ["__rmod__", (Int, Float, Bool), Float],
            ["__rmul__", (Int, Float, Bool), Float],
            ["__rpow__", (Int, Float, Bool), Float],
            ["__rsub__", (Int, Float, Bool), Float],
            ["__rtruediv__", (Int, Float, Bool), Float],
            ["__sub__", (Int, Float, Bool), Float],
            ["__truediv__", (Int, Float, Bool), Float],
        ],
    )
    def test_all_operators_float(self, operator, accepted_types, return_type):
        operator_test(
            self.float_obj,
            self.all_values_to_try,
            operator,
            accepted_types,
            return_type,
        )

    @pytest.mark.parametrize("obj", [Int(0), Float(2.2)])
    @pytest.mark.parametrize(
        "op, exception",
        [(operator.truth, TypeError), (operator.index, TypeError), (hex, TypeError)],
    )
    def test_unsupported_unary_methods(self, obj, op, exception):
        with pytest.raises(exception):
            op(obj)
