import numpy as np

from ..core import typecheck_promote, ProxyTypeError
from ..primitives import Float, Int, Bool, NoneType


def _DelayedTuple():
    from ..containers import Tuple

    return Tuple


def _DelayedArray():
    from ..containers import Array

    return Array


def _DelayedIntList():
    from ..containers import List

    return List[Int]


def _ufunc_result_dtype(obj, other=None):
    from ..containers import Array

    if other is None:
        return obj.dtype if isinstance(obj, Array) else type(obj)
    else:
        obj, other = (
            a.dtype if isinstance(a, Array) else type(a) for a in (obj, other)
        )

    # If either are Float, the result is a Float
    if obj is Float or other is Float:
        return Float
    # Neither are Float, so if either are Int, the result is an Int
    if obj is Int or other is Int:
        return Int
    # Neither are Float, neither are Int, they must be Bool, so the result is Bool
    return Bool


HANDLED_FUNCTIONS = {}
HANDLED_UFUNCS = {}

##################
# ufunc operations
##################


class ufunc:
    _forward_attrs = {
        "nin",
        "nargs",
        "nout",
        "ntypes",
        "identity",
        "signature",
        "types",
    }

    def __init__(self, ufunc):
        if not isinstance(ufunc, np.ufunc):
            raise TypeError(
                "Must be an instance of `np.ufunc`, got {}".format(type(ufunc))
            )

        self._ufunc = ufunc
        self.__name__ = ufunc.__name__

        HANDLED_UFUNCS[ufunc.__name__] = ufunc

    def __call__(self, *args, **kwargs):
        from ..containers import Array

        if len(args) != self._ufunc.nin:
            raise TypeError(
                "Invalid number of arguments to function `{}`".format(self.__name__)
            )

        # Since typecheck_promote doesn't support variadic arguments, manually
        # attempt to promote each argument to an Array
        promoted = []
        for i, arg in enumerate(args):
            try:
                if isinstance(arg, np.ndarray):
                    promoted.append(Array.from_numpy(arg))
                else:
                    promoted.append(Array._promote(arg))
            except ProxyTypeError:
                try:
                    promoted.append(Int._promote(arg))
                except ProxyTypeError:
                    try:
                        promoted.append(Float._promote(arg))
                    except ProxyTypeError:
                        raise ProxyTypeError(
                            "Argument {} to function {} must be a Workflows Array, number, or "
                            "a type promotable to one of those, not {}".format(
                                i + 1, self.__name__, type(arg)
                            )
                        )

        return_dtype = _ufunc_result_dtype(*promoted)
        if any(isinstance(p, Array) for p in promoted):
            return Array[return_dtype, args[0].ndim]._from_apply(
                self.__name__, *promoted
            )
        else:
            return return_dtype._from_apply(self.__name__, *promoted)

    def reduce(self):
        raise NotImplementedError(
            "The `reduce` ufunc method is not supported by Workflows Arrays"
        )

    def reduceat(self):
        raise NotImplementedError(
            "The `reduceat` ufunc method is not supported by Workflows Arrays"
        )

    def accumulate(self):
        raise NotImplementedError(
            "The `accumulate` ufunc method is not supported by Workflows Arrays"
        )

    def outer(self):
        raise NotImplementedError(
            "The `outer` ufunc method is not supported by Workflows Arrays"
        )


# math operations
add = ufunc(np.add)
subtract = ufunc(np.subtract)
multiply = ufunc(np.multiply)
divide = ufunc(np.divide)
logaddexp = ufunc(np.logaddexp)
logaddexp2 = ufunc(np.logaddexp2)
true_divide = ufunc(np.true_divide)
floor_divide = ufunc(np.floor_divide)
negative = ufunc(np.negative)
power = ufunc(np.power)
float_power = ufunc(np.float_power)
remainder = ufunc(np.remainder)
mod = ufunc(np.mod)
conj = conjugate = ufunc(np.conj)
exp = ufunc(np.exp)
exp2 = ufunc(np.exp2)
log = ufunc(np.log)
log2 = ufunc(np.log2)
log10 = ufunc(np.log10)
log1p = ufunc(np.log1p)
expm1 = ufunc(np.expm1)
sqrt = ufunc(np.sqrt)
square = ufunc(np.square)
cbrt = ufunc(np.cbrt)
reciprocal = ufunc(np.reciprocal)

# trigonometric functions
sin = ufunc(np.sin)
cos = ufunc(np.cos)
tan = ufunc(np.tan)
arcsin = ufunc(np.arcsin)
arccos = ufunc(np.arccos)
arctan = ufunc(np.arctan)
arctan2 = ufunc(np.arctan2)
# hypot = ufunc(np.hypot)
sinh = ufunc(np.sinh)
cosh = ufunc(np.cosh)
tanh = ufunc(np.tanh)
arcsinh = ufunc(np.arcsinh)
arccosh = ufunc(np.arccosh)
arctanh = ufunc(np.arctanh)
deg2rad = ufunc(np.deg2rad)
rad2deg = ufunc(np.rad2deg)

# bit-twiddling functions
# bitwise_and = ufunc(np.bitwise_and)
# bitwise_or = ufunc(np.bitwise_or)
# bitwise_xor = ufunc(np.bitwise_xor)
# bitwise_not = ufunc(np.bitwise_not)
# TODO: invert

# comparision functions
greater = ufunc(np.greater)
greater_equal = ufunc(np.greater_equal)
less = ufunc(np.less)
less_equal = ufunc(np.less_equal)
not_equal = ufunc(np.not_equal)
equal = ufunc(np.equal)
# isneginf = partial(equal, -np.inf)
# isposinf = partial(equal, np.inf)
logical_and = ufunc(np.logical_and)
logical_or = ufunc(np.logical_or)
logical_xor = ufunc(np.logical_xor)
logical_not = ufunc(np.logical_not)
maximum = ufunc(np.maximum)
minimum = ufunc(np.minimum)
fmax = ufunc(np.fmax)
fmin = ufunc(np.fmin)

# floating functions
isfinite = ufunc(np.isfinite)
isinf = ufunc(np.isinf)
isnan = ufunc(np.isnan)
signbit = ufunc(np.signbit)
copysign = ufunc(np.copysign)
nextafter = ufunc(np.nextafter)
spacing = ufunc(np.spacing)
# modf = ufunc(np.modf) # has multiple outputs
# ldexp = ufunc(np.ldexp)
# frexp = ufunc(np.frexp) # has multiple outputs
fmod = ufunc(np.fmod)
floor = ufunc(np.floor)
ceil = ufunc(np.ceil)
trunc = ufunc(np.trunc)

degrees = ufunc(np.degrees)
radians = ufunc(np.radians)
rint = ufunc(np.rint)
fabs = ufunc(np.fabs)
sign = ufunc(np.sign)
absolute = ufunc(np.absolute)


######################
# non-ufunc operations
######################


def implements(numpy_func):
    def decorator(wf_func):
        HANDLED_FUNCTIONS[numpy_func] = wf_func
        return wf_func

    return decorator


# Once we support checking variadic positional args in typecheck_promote, we can use typecheck_promote instead
@implements(np.concatenate)
@typecheck_promote(None, axis=Int)
def concatenate(seq, axis=0):
    from ..containers import Array

    if len(seq) < 2:
        raise ValueError("Concatenate requires at least 2 Arrays for concatenation")

    for obj in seq:
        if not isinstance(obj, Array):
            raise TypeError(
                "Argument 'seq' to concatenate(): expected sequence of Array objects but got ({})".format(
                    ", ".join(type(a).__name__ for a in seq)
                )
            )

    for i in range(len(seq)):
        try:
            if seq[i].ndim != seq[i + 1].ndim:
                raise ValueError(
                    "All input arrays must have the same number of dimensions, "
                    "but the array at index {} has {} dimension(s) "
                    "and the array at index {} has {} dimension(s).".format(
                        i, seq[i].ndim, i + 1, seq[i + 1].ndim
                    )
                )
        except IndexError:
            pass

    return seq[0]._from_apply("concatenate", seq, axis=axis)


@implements(np.transpose)
@typecheck_promote(_DelayedArray, axes=(_DelayedIntList, NoneType))
def transpose(arr, axes=None):
    return arr._from_apply("transpose", arr, axes=axes)
