import sys
import numpy as np
from inspect import Signature, Parameter
from typing import Union

from .utils import copy_docstring_from_numpy
from ..core import ProxyTypeError
from ..core.promote import _promote
from ..core.codegen import stringify_signature
from ..primitives import Float, Int, Bool
from ..array import Array, MaskedArray, BaseArray, Scalar


def _ufunc_result_type(obj, other=None, return_type_override=None):
    if other is None:
        if return_type_override is None:
            return type(obj)
        return return_type_override if not isinstance(obj, BaseArray) else type(obj)

    if return_type_override is not None:
        dtype = return_type_override
    else:
        obj_dtype, other_dtype = (
            Scalar if isinstance(a, BaseArray) else type(a) for a in (obj, other)
        )

        # dtype precedence (from highest to lowest): Scalar, Float, Int, Bool
        if issubclass(obj_dtype, Scalar) or issubclass(other_dtype, Scalar):
            dtype = Scalar
        elif issubclass(obj_dtype, Float) or issubclass(other_dtype, Float):
            dtype = Float
        elif issubclass(obj_dtype, Int) or issubclass(other_dtype, Int):
            dtype = Int
        else:
            dtype = Bool

    if isinstance(obj, MaskedArray) or isinstance(other, MaskedArray):
        return MaskedArray
    elif isinstance(obj, Array) or isinstance(other, Array):
        return Array
    else:
        return dtype


HANDLED_UFUNCS = {}


def raise_(e):
    raise e


def ufunc(np_ufunc, return_type_override=None):
    if not isinstance(np_ufunc, np.ufunc):
        raise TypeError(
            "Must be an instance of `np.ufunc`, got {}".format(type(np_ufunc))
        )

    def wf_ufunc(*args):
        if len(args) != np_ufunc.nin:
            raise TypeError(
                "Invalid number of arguments to function `{}`".format(np_ufunc.__name__)
            )

        # Since typecheck_promote doesn't support variadic arguments, manually
        # attempt to promote each argument to an Array or scalar
        promoted = []
        for i, arg in enumerate(args):
            try:
                if isinstance(arg, BaseArray):
                    promoted.append(arg)
                elif isinstance(arg, np.ma.core.MaskedArray):
                    promoted.append(MaskedArray._promote(arg))
                elif isinstance(arg, np.ndarray):
                    promoted.append(Array._promote(arg))
                else:
                    promoted.append(
                        _promote(arg, (Bool, Int, Float, Scalar), i, np_ufunc.__name__)
                    )
                    # TODO(gabe) not great to be relying on internal `_promote` here
            except (ProxyTypeError, TypeError):
                raise ProxyTypeError(
                    "Argument {} to function {} must be a Workflows Array, Scalar, Int, Float,"
                    "Bool, or a type promotable to one of those, not {}".format(
                        i + 1, np_ufunc.__name__, type(arg)
                    )
                )

        return_type = _ufunc_result_type(
            *promoted, return_type_override=return_type_override
        )

        return return_type._from_apply("wf.numpy." + np_ufunc.__name__, *promoted)

    HANDLED_UFUNCS[np_ufunc.__name__] = wf_ufunc

    copy_docstring_from_numpy(wf_ufunc, np_ufunc)

    # create an inspect.Signature object
    return_annotation = (
        return_type_override
        if return_type_override is not None
        else Union[Array, MaskedArray, Scalar, Int, Float, Bool]
    )

    if np_ufunc.nin == 2:
        signature = Signature(
            [
                Parameter(
                    "x1",
                    Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Union[Array, MaskedArray, Scalar, Int, Float, Bool],
                ),
                Parameter(
                    "x2",
                    Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Union[Array, MaskedArray, Scalar, Int, Float, Bool],
                ),
            ],
            return_annotation=return_annotation,
        )
    else:
        signature = Signature(
            [
                Parameter(
                    "x",
                    Parameter.POSITIONAL_OR_KEYWORD,
                    annotation=Union[Array, MaskedArray, Scalar, Int, Float, Bool],
                )
            ],
            return_annotation=return_annotation,
        )

    # set __annotations__
    if "sphinx" not in sys.modules:
        wf_ufunc.__annotations__ = {
            name: param.annotation for name, param in signature.parameters.items()
        }
        wf_ufunc.__annotations__["return"] = signature.return_annotation

    # set __signature__ to the stringified version
    wf_ufunc.__signature__ = stringify_signature(signature)

    # forward attributes
    wf_ufunc.nin = np_ufunc.nin
    wf_ufunc.nargs = np_ufunc.nargs
    wf_ufunc.nout = np_ufunc.nout
    wf_ufunc.ntypes = np_ufunc.ntypes
    wf_ufunc.identity = np_ufunc.identity
    wf_ufunc.signature = np_ufunc.signature
    wf_ufunc.types = np_ufunc.types

    # NOTE: we currently don't support other attributes of ufuncs (besides `__call__`)
    wf_ufunc.reduce = lambda *args: raise_(
        NotImplementedError(
            "The `reduce` ufunc method is not supported by Workflows types"
        )
    )
    wf_ufunc.reduceat = lambda *args: raise_(
        NotImplementedError(
            "The `reduceat` ufunc method is not supported by Workflows types"
        )
    )
    wf_ufunc.accumulate = lambda *args: raise_(
        NotImplementedError(
            "The `accumulate` ufunc method is not supported by Workflows types"
        )
    )
    wf_ufunc.outer = lambda *args: raise_(
        NotImplementedError(
            "The `outer` ufunc method is not supported by Workflows types"
        )
    )

    return wf_ufunc


# math operations
add = ufunc(np.add)
subtract = ufunc(np.subtract)
multiply = ufunc(np.multiply)
divide = ufunc(np.divide)
logaddexp = ufunc(np.logaddexp, return_type_override=Float)
logaddexp2 = ufunc(np.logaddexp2, return_type_override=Float)
true_divide = ufunc(np.true_divide, return_type_override=Float)
floor_divide = ufunc(np.floor_divide)
negative = ufunc(np.negative)
power = ufunc(np.power)
float_power = ufunc(np.float_power, return_type_override=Float)
remainder = ufunc(np.remainder)
mod = ufunc(np.mod)
conj = conjugate = ufunc(np.conj)
exp = ufunc(np.exp, return_type_override=Float)
exp2 = ufunc(np.exp2, return_type_override=Float)
log = ufunc(np.log, return_type_override=Float)
log2 = ufunc(np.log2, return_type_override=Float)
log10 = ufunc(np.log10, return_type_override=Float)
log1p = ufunc(np.log1p, return_type_override=Float)
expm1 = ufunc(np.expm1, return_type_override=Float)
sqrt = ufunc(np.sqrt, return_type_override=Float)
square = ufunc(np.square)
cbrt = ufunc(np.cbrt, return_type_override=Float)
reciprocal = ufunc(np.reciprocal)

# trigonometric functions
sin = ufunc(np.sin, return_type_override=Float)
cos = ufunc(np.cos, return_type_override=Float)
tan = ufunc(np.tan, return_type_override=Float)
arcsin = ufunc(np.arcsin, return_type_override=Float)
arccos = ufunc(np.arccos, return_type_override=Float)
arctan = ufunc(np.arctan, return_type_override=Float)
arctan2 = ufunc(np.arctan2, return_type_override=Float)
hypot = ufunc(np.hypot)
sinh = ufunc(np.sinh, return_type_override=Float)
cosh = ufunc(np.cosh, return_type_override=Float)
tanh = ufunc(np.tanh, return_type_override=Float)
arcsinh = ufunc(np.arcsinh, return_type_override=Float)
arccosh = ufunc(np.arccosh, return_type_override=Float)
arctanh = ufunc(np.arctanh, return_type_override=Float)
deg2rad = ufunc(np.deg2rad, return_type_override=Float)
rad2deg = ufunc(np.rad2deg, return_type_override=Float)

# bit-twiddling functions
bitwise_and = ufunc(np.bitwise_and)
bitwise_or = ufunc(np.bitwise_or)
bitwise_xor = ufunc(np.bitwise_xor)
bitwise_not = invert = ufunc(np.bitwise_not)

# comparision functions
greater = ufunc(np.greater, return_type_override=Bool)
greater_equal = ufunc(np.greater_equal, return_type_override=Bool)
less = ufunc(np.less, return_type_override=Bool)
less_equal = ufunc(np.less_equal, return_type_override=Bool)
not_equal = ufunc(np.not_equal, return_type_override=Bool)
equal = ufunc(np.equal, return_type_override=Bool)
logical_and = ufunc(np.logical_and, return_type_override=Bool)
logical_or = ufunc(np.logical_or, return_type_override=Bool)
logical_xor = ufunc(np.logical_xor, return_type_override=Bool)
logical_not = ufunc(np.logical_not, return_type_override=Bool)
maximum = ufunc(np.maximum)
minimum = ufunc(np.minimum)
fmax = ufunc(np.fmax)
fmin = ufunc(np.fmin)

# floating functions
isfinite = ufunc(np.isfinite, return_type_override=Bool)
isinf = ufunc(np.isinf, return_type_override=Bool)
isnan = ufunc(np.isnan, return_type_override=Bool)
signbit = ufunc(np.signbit, return_type_override=Bool)
copysign = ufunc(np.copysign, return_type_override=Float)
nextafter = ufunc(np.nextafter, return_type_override=Float)
spacing = ufunc(np.spacing, return_type_override=Float)
ldexp = ufunc(np.ldexp)
fmod = ufunc(np.fmod)
floor = ufunc(np.floor)
ceil = ufunc(np.ceil)
trunc = ufunc(np.trunc)

# NOTE (stephanie): modf and frexp have multiple outputs, the dask version also
#   only accepts arrays and masked arrays (as opposed to all other ufuncs that
#   also accept scalar values)
# modf = ufunc(np.modf)
# frexp = ufunc(np.frexp)

degrees = ufunc(np.degrees, return_type_override=Float)
radians = ufunc(np.radians, return_type_override=Float)
rint = ufunc(np.rint, return_type_override=Float)
fabs = ufunc(np.fabs, return_type_override=Float)
sign = ufunc(np.sign)
absolute = ufunc(np.absolute)
