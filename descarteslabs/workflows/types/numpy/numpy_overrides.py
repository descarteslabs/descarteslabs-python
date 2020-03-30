import numpy as np

from ..core import typecheck_promote, ProxyTypeError
from ..core.promote import _promote
from ..primitives import Float, Int, Bool, NoneType
from ..array import Array, MaskedArray
from ..containers import List, Tuple


def _ufunc_result_type(obj, other=None, return_type_override=None):
    if other is None:
        if return_type_override is None:
            return type(obj)
        return return_type_override if not isinstance(obj, Array) else type(obj)

    if return_type_override is not None:
        dtype = return_type_override
    else:
        obj_dtype, other_dtype = (
            Float if isinstance(a, Array) else type(a) for a in (obj, other)
        )

        # If either are Float, the result is a Float
        if obj_dtype is Float or other_dtype is Float:
            dtype = Float
        # Neither are Float, so if either are Int, the result is an Int
        elif obj_dtype is Int or other_dtype is Int:
            dtype = Int
        # Neither are Float, neither are Int, they must be Bool, so the result is Bool
        else:
            dtype = Bool

    if isinstance(obj, Array) or isinstance(other, Array):
        if isinstance(obj, Array) and isinstance(other, Array):
            result_type = type(obj)
            other_type = type(other)
            if issubclass(other_type, result_type):
                result_type = other_type
        else:
            result_type = type(obj) if isinstance(obj, Array) else type(other)
        return result_type
    else:
        return dtype


def derived_from(original_method):
    """Decorator to attach original method's docstring to the wrapped method"""

    def wrapper(method):
        doc = original_method.__doc__.replace("*,", "\*,")  # noqa
        doc = doc.replace(
            ":ref:`ufunc docs <ufuncs.kwargs>`.",
            "`ufunc docs <https://docs.scipy.org/doc/numpy/reference/ufuncs.html#ufuncs-kwargs>`_.",
        )

        # remove examples
        doc = doc.split("\n\n    Examples\n")[0]

        # remove references
        doc = [a for a in doc.split("\n\n") if "References\n----------\n" not in a]

        # remove "See Also" section
        doc = [a for a in doc if "See Also\n" not in a]

        l1 = "This docstring was copied from numpy.{}".format(original_method.__name__)
        l2 = "Some inconsistencies with the Workflows version may exist"

        if isinstance(original_method, np.ufunc):
            # what the function does
            info = doc[1]

            # parameters (sometimes listed on separate lines, someimtes not)
            parameters = [a for a in doc if "Parameters\n" in a][0].split("\n")
            if parameters[4][0] == "x":
                parameters = "\n".join(parameters[:6])
            else:
                parameters = "\n".join(parameters[:4])

            # return value
            returns = [a for a in doc if "Returns\n" in a][0]

            # final docstring
            doc = "\n\n".join([info, l1, l2, parameters, returns])
        else:
            # does the first line contain the function signature? (not always the case)
            if doc[0][-1] == ")":
                doc = (
                    [doc[1]]
                    + ["\n\n" + "    {}\n\n    {}\n\n".format(l1, l2)]
                    + doc[2:]
                )
            else:
                doc = (
                    [doc[0]]
                    + ["\n\n" + "    {}\n\n    {}\n\n".format(l1, l2)]
                    + doc[1:]
                )
            doc = "\n\n".join(doc)

        method.__doc__ = doc
        return method

    return wrapper


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

    def __init__(self, ufunc, return_type_override=None):
        if not isinstance(ufunc, np.ufunc):
            raise TypeError(
                "Must be an instance of `np.ufunc`, got {}".format(type(ufunc))
            )

        self._ufunc = ufunc
        self.__name__ = ufunc.__name__
        self._return_type_override = return_type_override

        if isinstance(ufunc, np.ufunc):
            derived_from(ufunc)(self)

        HANDLED_UFUNCS[ufunc.__name__] = self

    def __call__(self, *args, **kwargs):
        if len(args) != self._ufunc.nin:
            raise TypeError(
                "Invalid number of arguments to function `{}`".format(self.__name__)
            )

        # Since typecheck_promote doesn't support variadic arguments, manually
        # attempt to promote each argument to an Array or scalar
        promoted = []
        for i, arg in enumerate(args):
            try:
                if isinstance(arg, Array):
                    promoted.append(arg)
                elif isinstance(arg, np.ndarray):
                    promoted.append(Array._promote(arg))
                else:
                    promoted.append(_promote(arg, (Bool, Int, Float), i, self.__name__))
                    # TODO(gabe) not great to be relying on internal `_promote` here
            except (ProxyTypeError, TypeError):
                raise ProxyTypeError(
                    "Argument {} to function {} must be a Workflows Array, Int, Float, Bool, or "
                    "a type promotable to one of those, not {}".format(
                        i + 1, self.__name__, type(arg)
                    )
                )

        return_type = _ufunc_result_type(
            *promoted, return_type_override=self._return_type_override
        )
        return return_type._from_apply(self.__name__, *promoted)

    def reduce(self):
        raise NotImplementedError(
            "The `reduce` ufunc method is not supported by Workflows types"
        )

    def reduceat(self):
        raise NotImplementedError(
            "The `reduceat` ufunc method is not supported by Workflows types"
        )

    def accumulate(self):
        raise NotImplementedError(
            "The `accumulate` ufunc method is not supported by Workflows types"
        )

    def outer(self):
        raise NotImplementedError(
            "The `outer` ufunc method is not supported by Workflows types"
        )


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
# hypot = ufunc(np.hypot)
sinh = ufunc(np.sinh, return_type_override=Float)
cosh = ufunc(np.cosh, return_type_override=Float)
tanh = ufunc(np.tanh, return_type_override=Float)
arcsinh = ufunc(np.arcsinh, return_type_override=Float)
arccosh = ufunc(np.arccosh, return_type_override=Float)
arctanh = ufunc(np.arctanh, return_type_override=Float)
deg2rad = ufunc(np.deg2rad, return_type_override=Float)
rad2deg = ufunc(np.rad2deg, return_type_override=Float)

# bit-twiddling functions
# bitwise_and = ufunc(np.bitwise_and)
# bitwise_or = ufunc(np.bitwise_or)
# bitwise_xor = ufunc(np.bitwise_xor)
# bitwise_not = ufunc(np.bitwise_not)
# TODO: invert

# comparision functions
greater = ufunc(np.greater, return_type_override=Bool)
greater_equal = ufunc(np.greater_equal, return_type_override=Bool)
less = ufunc(np.less, return_type_override=Bool)
less_equal = ufunc(np.less_equal, return_type_override=Bool)
not_equal = ufunc(np.not_equal, return_type_override=Bool)
equal = ufunc(np.equal, return_type_override=Bool)
# isneginf = partial(equal, -np.inf)
# isposinf = partial(equal, np.inf)
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
# modf = ufunc(np.modf) # has multiple outputs
# ldexp = ufunc(np.ldexp)
# frexp = ufunc(np.frexp) # has multiple outputs
fmod = ufunc(np.fmod)
floor = ufunc(np.floor)
ceil = ufunc(np.ceil)
trunc = ufunc(np.trunc)

degrees = ufunc(np.degrees, return_type_override=Float)
radians = ufunc(np.radians, return_type_override=Float)
rint = ufunc(np.rint, return_type_override=Float)
fabs = ufunc(np.fabs, return_type_override=Float)
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


def asarray(obj):
    # TODO dtype!!
    if isinstance(obj, Array):
        return obj
    if isinstance(obj, (Int, Float, Bool)):
        return Array(obj)
    if not isinstance(obj, np.ndarray):
        # Dumb hack to save writing list traversal ourselves: just try to make it into an ndarray
        obj = np.asarray(obj)
    return Array._promote(obj)


@implements(np.concatenate)
@typecheck_promote((List[MaskedArray], List[Array]), axis=Int)
@derived_from(np.concatenate)
def concatenate(seq, axis=0):
    return_type = seq._element_type
    return return_type._from_apply("concatenate", seq, axis=axis)


@implements(np.transpose)
@typecheck_promote((MaskedArray, Array), axes=(List[Int], NoneType))
@derived_from(np.transpose)
def transpose(arr, axes=None):
    return arr._from_apply("transpose", arr, axes=axes)


@implements(np.histogram)
@typecheck_promote(
    (MaskedArray, Array),
    bins=(List[Int], List[Float], Int),
    range=(Tuple[Int, Int], Tuple[Float, Float], NoneType),
    weights=(Array, NoneType),
    density=None,
)
@derived_from(np.histogram)
def histogram(arr, bins=10, range=None, weights=None, density=None):
    if density is not None and not isinstance(density, bool):
        raise TypeError("Histogram argument 'density' must be None or a bool.")

    if isinstance(bins, Int) and isinstance(range, NoneType):
        raise ValueError(
            "Histogram requires range to be specified if bins is given as an int."
        )

    return Tuple[Array, Array]._from_apply(
        "histogram", arr, bins=bins, range=range, weights=weights, density=density
    )


@implements(np.reshape)
@typecheck_promote((MaskedArray, Array), newshape=None)
@derived_from(np.reshape)
def reshape(arr, newshape):
    newshape = _promote_newshape(newshape)
    return arr._from_apply("reshape", arr, newshape)


def _promote_newshape(newshape):
    if isinstance(newshape, (Tuple, tuple, list)):
        type_params = (Int,) * len(newshape)
        try:
            return Tuple[type_params]._promote(newshape)
        except ProxyTypeError:
            pass
    raise TypeError(
        "'newshape' must be a list or tuple of ints, received {!r}".format(newshape)
    )


@implements(np.stack)
@typecheck_promote((List[MaskedArray], List[Array]), axis=Int)
@derived_from(np.stack)
def stack(seq, axis=0):
    element_type = seq._element_type
    return element_type._from_apply("stack", seq, axis=axis)


@implements(np.argmin)
@typecheck_promote(Array, axis=(NoneType, Int))
@derived_from(np.argmin)
def argmin(arr, axis=None):
    if isinstance(axis, NoneType):
        return_type = Int
    else:
        return_type = type(arr)
    return return_type._from_apply("argmin", arr, axis=axis)


@implements(np.argmax)
@typecheck_promote(Array, axis=(NoneType, Int))
@derived_from(np.argmax)
def argmax(arr, axis=None):
    if isinstance(axis, NoneType):
        return_type = Int
    else:
        return_type = type(arr)
    return return_type._from_apply("argmax", arr, axis=axis)


@implements(np.all)
@typecheck_promote(Array, axis=None)
@derived_from(np.all)
def all(arr, axis=None):
    if axis is None:
        return_type = Bool
    elif isinstance(axis, int):
        return_type = type(arr)
    elif isinstance(axis, (list, tuple)):
        for idx, ax in enumerate(axis):
            if not isinstance(ax, int):
                raise TypeError(
                    "In all: Element {} to `axis`: expected int but got type {!r}".format(
                        idx, type(ax)
                    )
                )
        return_type = type(arr)
    else:
        raise TypeError(
            "In all: Expected None, int, or tuple of ints for argument `axis`, but got type `{}`".format(
                type(axis)
            )
        )
    return return_type._from_apply("all", arr, axis=axis)


@implements(np.any)
@typecheck_promote(Array, axis=None)
@derived_from(np.any)
def any(arr, axis=None):
    if axis is None:
        return_type = Bool
    elif isinstance(axis, int):
        return_type = type(arr)
    elif isinstance(axis, (list, tuple)):
        for idx, ax in enumerate(axis):
            if not isinstance(ax, int):
                raise TypeError(
                    "In any: Element {} to `axis`: expected int but got type {!r}".format(
                        idx, type(ax)
                    )
                )
        return_type = type(arr)
    else:
        raise TypeError(
            "In any: Expected None, int, or tuple of ints for argument `axis`, but got type `{}`".format(
                type(axis)
            )
        )
    return return_type._from_apply("any", arr, axis=axis)
