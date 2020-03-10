import functools

from ... import env
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
from .dict_ import Dict


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

        ndim = type_params[1]
        assert isinstance(
            ndim, int
        ), "Array ndim must be a Python integer, got {}".format(ndim)
        assert ndim >= 0, "Array ndim must be >= 0, not {}".format(ndim)

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
        "The shape of the Array. If the shape of the Array is unknown along a dimension, it will be -1."
        return_type = Tuple[(Int,) * self.ndim]
        return return_type._from_apply("array.shape", self)

    def __getitem__(self, idx):
        idx, return_ndim = typecheck_getitem(idx, self.ndim)

        if return_ndim == 0:
            return_type = self.dtype
        elif return_ndim > 0:
            return_type = type(self)._generictype[self.dtype, return_ndim]
        return return_type._from_apply("array.getitem", self, idx)

    def to_imagery(self, properties=None, bandinfo=None):
        """
        Turns a proxy Array into an `~.geospatial.Image` or `~.geospatial.ImageCollection`
        depending on the dimenstionalty of the Array.

        Parameters
        ----------
        properties: Dict or List, default None
            Properties of the new `~.geospatial.Image` or `~.geospatial.ImageCollection`.
            If the Array is 3-dimensional, properties should be a dictionary. If the Array is
            4-dimensional and properties is a dictionary, the properties will be broadcast to the
            length of the new `~.geospatial.ImageCollection`. If the Array is 4-dimensional and
            properties is a list, the length of the list must be equal to the length of the outermost
            dimension of the Array (``arr.shape[0]``). If no properties are given, the properties will
            be an empty dictionary (`~.geospatial.Image`), or list of empty dictionaries
            (`~.geospatial.ImageCollection`).

        bandinfo: Dict, default None
            Bandinfo for the new `~.geospatial.Image` or `~.geospatial.ImageCollection`.
            Must be equal in length to the number of bands in the Array.
            Therefore, if the Array is 3-dimensional (an `~.geospatial.Image`), bandinfo
            must be the length of ``arr.shape[0]``. If the Array is 4-dimensional
            (an `~.geospatial.ImageCollection`), bandinfo must be the length of ``arr.shape[1]``.
            If no bandinfo is given, the bandinfo will be a dict of bandname (of the format 'band_<num>',
            where 'num' is 1...N) to empty dictionary.
        """
        from ..geospatial import Image, ImageCollection

        if not isinstance(properties, (type(None), dict, list, Dict, List)):
            raise TypeError(
                "Provided properties must be a Dict (3-dimensional Array) or List (4-dimensional Array), got {}".format(
                    type(properties)
                )
            )

        if not isinstance(bandinfo, (type(None), dict, Dict)):
            raise TypeError(
                "Provided bandinfo must be a Dict, got {}".format(type(properties))
            )

        if self.ndim == 3:
            return_type = Image
        elif self.ndim == 4:
            return_type = ImageCollection
        else:
            raise ValueError(
                "Cannot turn a {}-dimensional Array into an Image/ImageCollection, must be 3 or 4-dimensional.".format(
                    self.ndim
                )
            )

        return return_type._from_apply(
            "to_imagery", self, properties, bandinfo, env.geoctx
        )

    def __neg__(self):
        return self._from_apply("neg", self)

    def __pos__(self):
        return self._from_apply("pos", self)

    def __abs__(self):
        return self._from_apply("abs", self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __lt__(self, other):
        return self._result_type(other, is_bool=True)._from_apply("lt", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __le__(self, other):
        return self._result_type(other, is_bool=True)._from_apply("le", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __gt__(self, other):
        return self._result_type(other, is_bool=True)._from_apply("gt", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __ge__(self, other):
        return self._result_type(other, is_bool=True)._from_apply("ge", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __add__(self, other):
        return self._result_type(other)._from_apply("add", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __sub__(self, other):
        return self._result_type(other)._from_apply("sub", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __mul__(self, other):
        return self._result_type(other)._from_apply("mul", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __div__(self, other):
        return self._result_type(other)._from_apply("div", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __floordiv__(self, other):
        return self._result_type(other)._from_apply("floordiv", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __truediv__(self, other):
        return self._result_type(other)._from_apply("truediv", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __mod__(self, other):
        return self._result_type(other)._from_apply("mod", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __pow__(self, other):
        return self._result_type(other)._from_apply("pow", self, other)

    @typecheck_promote((lambda: Array, Int, Float))
    def __radd__(self, other):
        return self._result_type(other)._from_apply("add", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rsub__(self, other):
        return self._result_type(other)._from_apply("sub", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rmul__(self, other):
        return self._result_type(other)._from_apply("mul", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rdiv__(self, other):
        return self._result_type(other)._from_apply("div", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rfloordiv__(self, other):
        return self._result_type(other)._from_apply("floordiv", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rtruediv__(self, other):
        return self._result_type(other)._from_apply("truediv", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rmod__(self, other):
        return self._result_type(other)._from_apply("mod", other, self)

    @typecheck_promote((lambda: Array, Int, Float))
    def __rpow__(self, other):
        return self._result_type(other)._from_apply("pow", other, self)

    def min(self, axis=None):
        return self._stats_return_type(axis)._from_apply("min", self, axis)

    def max(self, axis=None):
        return self._stats_return_type(axis)._from_apply("max", self, axis)

    def mean(self, axis=None):
        return self._stats_return_type(axis)._from_apply("mean", self, axis)

    def median(self, axis=None):
        return self._stats_return_type(axis)._from_apply("median", self, axis)

    def sum(self, axis=None):
        return self._stats_return_type(axis)._from_apply("sum", self, axis)

    def std(self, axis=None):
        return self._stats_return_type(axis)._from_apply("std", self, axis)

    def _stats_return_type(self, axis):
        if axis is None:
            return_type = self.dtype
        if isinstance(axis, tuple):
            for a in axis:
                assert isinstance(
                    a, (int, Int)
                ), "Axis must be an integer, got {}".format(type(a))
            num_axes = len(axis)
            if self.ndim - num_axes < 0:
                raise ValueError(
                    "Too many axes for {}-dimensional array.".format(self.ndim)
                )
            elif self.ndim - num_axes == 0:
                return_type = self.dtype
            else:
                return_type = type(self)._generictype[self.dtype, self.ndim - num_axes]
        else:
            return_type = type(self)._generictype[self.dtype, self.ndim - 1]
        return return_type

    def _result_type(self, other, is_bool=False):
        result_generictype = type(self)._generictype
        try:
            other_generictype = type(other)._generictype
        except AttributeError:
            pass
        else:
            if issubclass(other_generictype, result_generictype):
                result_generictype = other_generictype
        dtype = self._result_dtype(other, is_bool)
        ndim = self.ndim
        other_ndim = getattr(other, "ndim", -1)
        if ndim < other_ndim:
            ndim = other_ndim
        return result_generictype[dtype, ndim]

    def _result_dtype(self, other, is_bool=False):
        if is_bool:
            return Bool
        other_dtype = getattr(other, "dtype", None)
        # If either are Float, the result is a Float
        if self.dtype is Float or other_dtype is Float:
            return Float
        # Neither are Float, so if either are Int, the result is an Int
        if self.dtype is Int or other.dtype is Int:
            return Int
        # Neither are Float, neither are Int, they must be Bool, so the result is Bool
        return Bool


def typecheck_getitem(idx, ndim):
    return_ndim = ndim
    list_or_array_seen = False
    proxy_idx = []

    if not isinstance(idx, (tuple, Tuple)):
        idx = (idx,)

    num_newindex = sum(1 for x in idx if isinstance(x, (NoneType._pytype, NoneType)))
    num_idx = len(idx) - num_newindex
    if num_idx > ndim:
        raise ValueError("Too many indicies ({}) for a {}D Array".format(num_idx, ndim))

    for i, idx_elem in enumerate(idx):
        if isinstance(idx_elem, (int, Int)):
            return_ndim -= 1
            proxy_idx.append(Int._promote(idx_elem))
        elif isinstance(idx_elem, (slice, Slice)):
            proxy_idx.append(Slice._promote(idx_elem))
        elif isinstance(idx_elem, type(Ellipsis)):
            num_ellipsis = ndim - (num_idx - 1)
            proxy_idx += [Slice(None, None, None)] * num_ellipsis
        elif isinstance(idx_elem, (NoneType._pytype, NoneType)):
            proxy_idx.append(NoneType._promote(idx_elem))

        elif isinstance(idx_elem, (list, List)):
            if list_or_array_seen:
                raise ValueError(
                    "While slicing Array, position {}: cannot slice an Array "
                    "with lists or Arrays in multiple axes.".format(i)
                )
            list_or_array_seen = True

            # Python container case
            if isinstance(idx_elem, list):
                try:
                    # NOTE(gabe): `bool` is a subclass of `int` in Python, so bools work here too.
                    # Doesn't ultimately matter that we mangle the type.
                    idx_elem = List[Int]._promote(idx_elem)
                except ProxyTypeError:
                    raise TypeError(
                        "While slicing Array, position {}: Arrays can only be sliced with 1D lists, "
                        "and elements must be all Ints or Bools. Invalid types "
                        "in {!r}".format(i, idx_elem)
                    )

            # Proxy List case
            else:
                if not isinstance(idx_elem._element_type, (Int, Bool)):
                    raise TypeError(
                        "While slicing Array, position {}: Arrays can only be sliced with 1D List[Int] "
                        "or List[Bool], not {}".format(i, type(idx_elem).__name__)
                    )
            proxy_idx.append(idx_elem)

        elif isinstance(idx_elem, Array):
            if list_or_array_seen:
                raise ValueError(
                    "While slicing Array, position {}: cannot slice an Array "
                    "with lists or Arrays in multiple axes.".format(i)
                )
            list_or_array_seen = True

            if idx_elem.dtype not in (Int, Bool):
                raise TypeError(
                    "While slicing Array, position {}: "
                    "cannot slice an Array with an Array of type {}. "
                    "Must be Int or Bool.".format(i, idx_elem.dtype)
                )
            if idx_elem.dtype is Int:
                if idx_elem.ndim != 1:
                    raise ValueError(
                        "While slicing Array, position {}: "
                        "tried to slice with a {}D Int Array, must be 1D.\n"
                        "Slicing an Array with a multidimensional Array of Ints "
                        "is not supported.".format(i, idx_elem.ndim)
                    )
                # No change in ndim
            else:
                if idx_elem.ndim == return_ndim:
                    # NOTE(gabe): we use `return_ndim`, not `ndim`, to capture any slicing that's
                    # already occurred in the idx tuple before this bool array.
                    # For example, in `arr3D[idx]`, `idx` must be 3D. In `arr3D[0, idx]`, `idx` must be 2D.
                    # NOTE(gabe) also we don't have to worry about `return_ndim == 0`
                    # (weird edge case that breaks stuff) thanks to the "Too many indicies for Array" check

                    return_ndim = 1
                    # "If obj.ndim == x.ndim, x[obj] returns a 1-dimensional array"
                    # (otherwise, no change in ndim)
                elif idx_elem.ndim != 1:
                    raise ValueError(
                        "While slicing Array, position {}: "
                        "tried to slice with a {}D Bool Array, must be 1D{}.\n"
                        "Slicing with an Array of Bools is only supported when it's 1D, "
                        "or has the same dimensionality as the array once it's been "
                        "sliced by any prior indexing.".format(
                            i,
                            idx_elem.ndim,
                            " or {}D".format(return_ndim) if return_ndim > 1 else "",
                        )
                    )
            proxy_idx.append(idx_elem)
        else:
            raise TypeError(
                "While slicing Array, position {}: "
                "Invalid Array index {!r}.".format(i, idx_elem)
            )

    if isinstance(idx, Tuple):
        # it's passed all the checks; return the original Tuple
        # instead of building a new one from `(idx[0], idx[1], ...)`
        proxy_idx = idx
    else:
        if len(proxy_idx) == 1:
            proxy_idx = proxy_idx[0]
            # cleaner graft for a common case
        else:
            types = tuple(map(type, proxy_idx))
            proxy_idx = Tuple[types](proxy_idx)
            # ^ NOTE(gabe): not try/excepting this because we've already promoted everything in `proxy_idx`

    return proxy_idx, return_ndim + num_newindex
