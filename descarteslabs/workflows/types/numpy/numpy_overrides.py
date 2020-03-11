import numpy as np

from ..core import typecheck_promote, ProxyTypeError, Proxytype
from ..core.promote import _promote
from ..primitives import Float, Int, Bool, NoneType
from ..array import Array, MaskedArray
from ..containers import List, Tuple


def _ufunc_result_type(obj, other=None, is_bool=False):
    if other is None:
        if not is_bool:
            return type(obj)
        return Bool if not isinstance(obj, Array) else Array[Bool, obj.ndim]

    if is_bool:
        dtype = Bool
    else:
        obj_dtype, other_dtype = (
            a.dtype if isinstance(a, Array) else type(a) for a in (obj, other)
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
        ndim = max(getattr(a, "ndim", -1) for a in (obj, other))
        if isinstance(obj, Array) and isinstance(other, Array):
            result_generictype = type(obj)._generictype
            other_generictype = type(other)._generictype
            if issubclass(other_generictype, result_generictype):
                result_generictype = other_generictype
        else:
            result_generictype = (
                type(obj)._generictype
                if isinstance(obj, Array)
                else type(other)._generictype
            )
        return result_generictype[dtype, ndim]
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
                doc = [doc[1]] + ["\n\n" + "    {}\n\n    {}\n\n".format(l1, l2)] + doc[2:]
            else:
                doc = [doc[0]] + ["\n\n" + "    {}\n\n    {}\n\n".format(l1, l2)] + doc[1:]
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

    def __init__(self, ufunc, is_bool=False):
        if not isinstance(ufunc, np.ufunc):
            raise TypeError(
                "Must be an instance of `np.ufunc`, got {}".format(type(ufunc))
            )

        self._ufunc = ufunc
        self.__name__ = ufunc.__name__
        self._is_bool = is_bool

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
                    numpy_promoter = (
                        MaskedArray.from_numpy
                        if isinstance(arg, np.ma.MaskedArray)
                        else Array.from_numpy
                    )
                    promoted.append(numpy_promoter(arg))
                else:
                    promoted.append(_promote(arg, (Bool, Int, Float), i, self.__name__))
                    # TODO(gabe) not great to be relying on internal `_promote` here
            except (ProxyTypeError, TypeError):
                raise ProxyTypeError(
                    "Argument {} to function {} must be a Workflows Array, number, bool, or "
                    "a type promotable to one of those, not {}".format(
                        i + 1, self.__name__, type(arg)
                    )
                )

        return_type = _ufunc_result_type(*promoted, is_bool=self._is_bool)
        return return_type._from_apply(self.__name__, *promoted)

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
greater = ufunc(np.greater, is_bool=True)
greater_equal = ufunc(np.greater_equal, is_bool=True)
less = ufunc(np.less, is_bool=True)
less_equal = ufunc(np.less_equal, is_bool=True)
not_equal = ufunc(np.not_equal, is_bool=True)
equal = ufunc(np.equal, is_bool=True)
# isneginf = partial(equal, -np.inf)
# isposinf = partial(equal, np.inf)
logical_and = ufunc(np.logical_and, is_bool=True)
logical_or = ufunc(np.logical_or, is_bool=True)
logical_xor = ufunc(np.logical_xor, is_bool=True)
logical_not = ufunc(np.logical_not, is_bool=True)
maximum = ufunc(np.maximum)
minimum = ufunc(np.minimum)
fmax = ufunc(np.fmax)
fmin = ufunc(np.fmin)

# floating functions
isfinite = ufunc(np.isfinite, is_bool=True)
isinf = ufunc(np.isinf, is_bool=True)
isnan = ufunc(np.isnan, is_bool=True)
signbit = ufunc(np.signbit, is_bool=True)
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


def asarray(obj):
    # TODO dtype!!
    if isinstance(obj, Array):
        return obj
    if not isinstance(obj, np.ndarray):
        # Dumb hack to save writing list traversal ourselves: just try to make it into an ndarray
        obj = np.asarray(obj)
    numpy_promoter = (
        MaskedArray.from_numpy
        if isinstance(obj, np.ma.MaskedArray)
        else Array.from_numpy
    )
    return numpy_promoter(obj)


def _promote_to_list_of_same_arrays(seq, func_name):
    if isinstance(seq, List[Array]):
        # All elements in List are the same type, so we don't have to check that dtype/ndim match
        return seq
    elif isinstance(seq, Proxytype):
        raise TypeError(
            "Cannot {} type {}. Must be a proxy List[Array], "
            "or a Python iterable of Arrays.".format(func_name, type(seq).__name__)
        )
    else:
        # Python iterable of array-like objects
        try:
            iterator = iter(seq)
        except TypeError:
            raise TypeError(
                "{} expected an iterable of Array-like objects. "
                "{!r} is not iterable.".format(func_name, seq)
            )

        promoted = []
        return_type = None
        for i, elem in enumerate(iterator):
            try:
                promoted_elem = asarray(elem)
            except ProxyTypeError:
                raise TypeError(
                    "Element {} to {}: expected an Array-like object, "
                    "but got {!r}".format(i, func_name, elem)
                )

            if return_type is not None:
                if promoted_elem.ndim != return_type._type_params[1]:
                    raise ValueError(
                        "Cannot {} Arrays of different dimensionality. "
                        "Element 0 is {}-dimensional, and element {} is {}-dimensional.".format(
                            func_name,
                            return_type._type_params[1],
                            i,
                            promoted_elem.ndim,
                        )
                    )
                if not issubclass(promoted_elem.dtype, return_type._type_params[0]):
                    raise TypeError(
                        "Cannot {} Arrays with different dtypes."
                        " Element 0 has dtype {}, and element {} has dtype {}.".format(
                            func_name,
                            return_type._type_params[0].__name__,
                            i,
                            promoted_elem.dtype.__name__,
                        )
                    )
            else:
                return_type = type(promoted_elem)

            promoted.append(promoted_elem)
        if len(promoted) == 0:
            raise ValueError("need at least one Array to {}".format(func_name))

        return List[return_type](seq)


@implements(np.concatenate)
@typecheck_promote(None, axis=Int)
@derived_from(np.concatenate)
def concatenate(seq, axis=0):
    seq = _promote_to_list_of_same_arrays(seq, "concatenate")
    return_type = seq._element_type
    return return_type._from_apply("concatenate", seq, axis=axis)


@implements(np.transpose)
@typecheck_promote(Array, axes=(List[Int], NoneType))
@derived_from(np.transpose)
def transpose(arr, axes=None):
    return arr._from_apply("transpose", arr, axes=axes)


@implements(np.histogram)
@typecheck_promote(
    Array,
    bins=(List[Int], List[Float], Int),
    range=(Tuple[Int, Int], NoneType),
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

    hist_return_dtype = Float if density else Int
    hist_return_type = Array[hist_return_dtype, 1]

    return Tuple[hist_return_type, Array[Float, 1]]._from_apply(
        "histogram", arr, bins=bins, range=range, weights=weights, density=density
    )


@implements(np.reshape)
@typecheck_promote(Array, None)
@derived_from(np.reshape)
def reshape(arr, newshape):
    newshape = _promote_newshape(newshape)
    ndim = len(newshape)
    return type(arr)._generictype[arr.dtype, ndim]._from_apply("reshape", arr, newshape)


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
@typecheck_promote(None, axis=Int)
@derived_from(np.stack)
def stack(seq, axis=0):
    seq = _promote_to_list_of_same_arrays(seq, "stack")
    element_type = seq._element_type
    return_type = element_type._generictype[
        element_type._type_params[0], element_type._type_params[1] + 1
    ]
    return return_type._from_apply("stack", seq, axis=axis)
