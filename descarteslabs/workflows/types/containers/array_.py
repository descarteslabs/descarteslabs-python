import functools

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import (
    GenericProxytype,
    typecheck_promote,
    ProxyTypeError,
    assert_is_proxytype,
)
from ..primitives import Int, Float, Bool, NoneType
from .slice import Slice
from .tuple_ import Tuple
from .list_ import List


@serializable()
class Array(GenericProxytype):
    def __init__(self, arr):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic Array; "
                "the dtype must and dimensionality must be specified (like `Array[Float, 3]`)"
            )

        list_type = functools.reduce(
            lambda accum, cur: List[accum],
            range(self._type_params[1]),
            self._type_params[0],
        )
        try:
            arr = list_type._promote(arr)
        except ProxyTypeError:
            raise ValueError("Cannot instantiate an Array from {!r}".format(arr))

        self.graft = client.apply_graft("array.create", arr)

    @classmethod
    def _validate_params(cls, type_params):
        assert len(type_params) == 2, "Both Array dtype and ndim must be specified"
        error_message = "Array dtype must be a Proxytype, got {}".format(type_params[0])
        assert_is_proxytype(type_params[0], error_message=error_message)
        assert isinstance(
            type_params[1], (int, Int)
        ), "Array ndim must be an instance of a python integer or proxy integer, got {}".format(
            type_params[1]
        )

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        try:
            return super(Array, cls)._promote(obj)
        except TypeError:
            raise ProxyTypeError("Cannot promote {} to {}".format(obj, cls))

    @property
    def dtype(self):
        "The type of the data contained in the Array."
        return self._type_params[0]

    @property
    def ndim(self):
        "The number of dimensions of the Array."
        return self._type_params[1]

    @property
    def shape(self):
        "The shape of the Array."
        return_type = Tuple[(Int,) * self.ndim]
        return return_type._from_apply("array.shape", self)

    def __getitem__(self, idx):
        idx, return_ndim = typecheck_getitem(idx, self.ndim)

        if return_ndim == 0:
            return_type = self.dtype
        elif return_ndim > 0:
            return_type = Array[self.dtype, return_ndim]
        return return_type._from_apply("array.getitem", self, idx)

    def __neg__(self):
        return self._from_apply("neg", self)

    def __pos__(self):
        return self._from_apply("pos", self)

    def __abs__(self):
        return self._from_apply("abs", self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __lt__(self, other):
        return _result_type(self, other, is_bool=True)._from_apply("lt", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __le__(self, other):
        return _result_type(self, other, is_bool=True)._from_apply("le", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __gt__(self, other):
        return _result_type(self, other, is_bool=True)._from_apply("gt", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __ge__(self, other):
        return _result_type(self, other, is_bool=True)._from_apply("ge", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __add__(self, other):
        return _result_type(self, other)._from_apply("add", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __sub__(self, other):
        return _result_type(self, other)._from_apply("sub", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __mul__(self, other):
        return _result_type(self, other)._from_apply("mul", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __div__(self, other):
        return _result_type(self, other)._from_apply("div", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __floordiv__(self, other):
        return _result_type(self, other)._from_apply("floordiv", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __truediv__(self, other):
        return _result_type(self, other)._from_apply("truediv", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __mod__(self, other):
        return _result_type(self, other)._from_apply("mod", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __pow__(self, other):
        return _result_type(self, other)._from_apply("pow", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __radd__(self, other):
        return _result_type(self, other)._from_apply("add", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rsub__(self, other):
        return _result_type(self, other)._from_apply("sub", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rmul__(self, other):
        return _result_type(self, other)._from_apply("mul", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rdiv__(self, other):
        return _result_type(self, other)._from_apply("div", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rfloordiv__(self, other):
        return _result_type(self, other)._from_apply("floordiv", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rtruediv__(self, other):
        return _result_type(self, other)._from_apply("truediv", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rmod__(self, other):
        return _result_type(self, other)._from_apply("mod", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rpow__(self, other):
        return _result_type(self, other)._from_apply("pow", other, self)

    def min(self, axis=None):
        return _stats_return_type(self, axis)._from_apply("min", self, axis)

    def max(self, axis=None):
        return _stats_return_type(self, axis)._from_apply("max", self, axis)

    def mean(self, axis=None):
        return _stats_return_type(self, axis)._from_apply("mean", self, axis)

    def median(self, axis=None):
        return _stats_return_type(self, axis)._from_apply("median", self, axis)

    def sum(self, axis=None):
        return _stats_return_type(self, axis)._from_apply("sum", self, axis)

    def std(self, axis=None):
        return _stats_return_type(self, axis)._from_apply("std", self, axis)

    def count(self, axis=None):
        return _stats_return_type(self, axis)._from_apply("count", self, axis)


def _result_type(self, other, is_bool=False):
    dtype = _result_dtype(self, other, is_bool)
    ndim = self.ndim
    if self.ndim < getattr(other, "ndim", -1):
        ndim = other.ndim
    return Array[dtype, ndim]


def _result_dtype(self, other, is_bool=False):
    if is_bool:
        return Bool
    # If either are Float, the result is a Float
    if self.dtype is Float or other.dtype is Float:
        return Float
    # Neither are Float, so if either are Int, the result is an Int
    if self.dtype is Int or other.dtype is Int:
        return Int
    # Neither are Float, neither are Int, they must be Bool, so the result is Bool
    return Bool


def _stats_return_type(self, axis):
    if axis is None:
        return_type = self.dtype
    if isinstance(axis, tuple):
        for a in axis:
            assert isinstance(a, (int, Int)), "Axis must be an integer, got {}".format(
                type(a)
            )
        num_axes = len(axis)
        if self.ndim - num_axes < 0:
            raise ValueError(
                "Too many axes for {}-dimensional array.".format(self.ndim)
            )
        elif self.ndim - num_axes == 0:
            return_type = self.dtype
        else:
            return_type = Array[self.dtype, self.ndim - num_axes]
    else:
        return_type = Array[self.dtype, self.ndim - 1]
    return return_type


def typecheck_getitem(idx, ndim):
    return_ndim = ndim

    if idx is None or isinstance(idx, NoneType):
        return_ndim += 1
    elif isinstance(idx, (int, Int)):
        return_ndim -= 1
        idx = Int._promote(idx)
    elif isinstance(idx, (tuple, Tuple)):
        list_seen = False
        _type_params = ()
        idx_types = (
            tuple(map(type, idx)) if isinstance(idx, tuple) else idx._type_params
        )
        for idx_type in idx_types:
            if issubclass(idx_type, (int, Int)):
                return_ndim -= 1
                _type_params += (Int,)
            elif issubclass(idx_type, (list, List)):
                if list_seen:
                    raise ValueError(
                        "Cannot slice an Array with lists in multiple axes."
                    )
                list_seen = True
                _type_params += (List[Int],)
            elif issubclass(idx_type, (slice, Slice)):
                _type_params += (Slice,)
            elif issubclass(idx_type, (type(None), NoneType)):
                return_ndim += 1
                _type_params += (NoneType,)
            else:
                raise ValueError(f"Invalid Array index {idx_type!r}.")
        idx = Tuple[_type_params](idx)
    elif isinstance(idx, (list, List)):
        try:
            idx = List[Int]._promote(idx)
        except ProxyTypeError:
            raise ValueError(
                f"Cannot slice an Array with list {idx!r}. "
                "It must be an integer list and cannot have lists in multiple axes."
            )
    elif isinstance(idx, Array):
        if idx.dtype not in (Int, Bool):
            raise ValueError(
                "Cannot slice an Array with an Array of type {}. Must be Int.".format(
                    idx.dtype
                )
            )
        return_ndim += idx.ndim - 1
    elif isinstance(idx, (slice, Slice)):
        idx = Slice._promote(idx)
    else:
        raise TypeError("Invalid type for indexing: {}.".format(type(idx).__name__))

    return idx, return_ndim
