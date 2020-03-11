import functools
import numpy as np

from ... import env
from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import GenericProxytype, typecheck_promote, ProxyTypeError
from ..containers import Slice, Tuple, List, Dict
from ..primitives import Int, Float, Bool, NoneType


DTYPE_KIND_TO_WF = {"b": Bool, "i": Int, "f": Float}
WF_TO_DTYPE_KIND = dict(zip(DTYPE_KIND_TO_WF.values(), DTYPE_KIND_TO_WF.keys()))


@serializable()
class Array(GenericProxytype):
    """
    ``Array[DType, NDim]``: Proxy object representing a multidimensional, homogenous array of fixed-size items.
    The data-type must be a Proxytype (Int, Float, Bool etc.) and the number of dimensions must be a Python integer.

    Can be instantiated from a NumPy ndarray (via `from_numpy`), or a Python iterable.
    Currently, Arrays can only be constructed from small local arrays (< 10MB).
    Array follows the same syntax as NumPy arrays. It supports vectorized operations, broadcasting,
    and multidimensional indexing. There are some limitations including slicing with lists/arrays in multiple
    axes (``x[[1, 2, 3], [3, 2, 1]]``) and slicing with a multidimensional list/array of integers.

    Note
    ----
    Array is an experimental API. It may be changed in the future, will not necessarily be
    backwards compatible, and may have unexpected bugs. Please contact us with any feedback!

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> # Create a 1-dimensional Array of Ints
    >>> arr = wf.Array[wf.Int, 1]([1, 2, 3, 4, 5])
    >>> arr
    <descarteslabs.workflows.types.array.array_.Array[Int, 1] object at 0x...>
    >>> arr.compute(geoctx) # doctest: +SKIP
    array([1, 2, 3, 4, 5])

    >>> import numpy as np
    >>> import descarteslabs.workflows as wf
    >>> ndarray = np.ones((3, 10, 10))
    >>> # Create an Array from the 3-dimensional numpy array
    >>> arr = wf.Array.from_numpy(ndarray)
    >>> arr
    <descarteslabs.workflows.types.array.array_.Array[Float, 3] object at 0x...>
    """

    def __init__(self, arr):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic Array; "
                "the dtype and dimensionality must be specified (like `Array[Float, 3]`). "
                "Alternatively, Arrays can be instantiated with `from_numpy` "
                "(like `Array.from_numpy(my_array)`)."
            )

        if isinstance(arr, np.ndarray):
            if arr.dtype.kind != WF_TO_DTYPE_KIND[self.dtype]:
                raise TypeError(
                    "Invalid dtype {} for an {}".format(arr.dtype, type(self).__name__)
                )
            if arr.ndim != self.ndim:
                raise ValueError(
                    "Cannot instantiate a {}-dimensional Workflows Array from a "
                    "{}-dimensional NumPy array".format(self.ndim, arr.ndim)
                )

            arr_list = arr.tolist()
            self.graft = client.apply_graft("array.create", arr_list)

        else:
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
        dtype, ndim = type_params
        assert dtype in (
            Int,
            Float,
            Bool,
        ), "Array dtype must be Int, Float, or Bool, got {}".format(dtype)
        assert isinstance(
            ndim, int
        ), "Array ndim must be a Python integer, got {}".format(ndim)
        assert ndim >= 0, "Array ndim must be >= 0, not {}".format(ndim)

    @classmethod
    def from_numpy(cls, arr):
        """
        Construct a Workflows Array from a NumPy ndarray, inferring `dtype` and `ndim`

        Parameters
        ----------
        arr: numpy.ndarray

        Returns
        -------
        ~descarteslabs.workflows.Array
        """
        if cls._type_params:
            # don't infer dtype, ndim from `arr`, since our cls is already parametrized
            return cls(arr)

        # infer dtype, ndim from numpy array
        try:
            dtype = DTYPE_KIND_TO_WF[arr.dtype.kind]
        except KeyError:
            raise ProxyTypeError(
                "Creating a Workflows Array from a NumPy Array with dtype "
                "`{}` is not supported. Supported dtypes kinds are float, "
                "int, and bool.".format(arr.dtype)
            )
        return cls[dtype, arr.ndim](arr)

    @classmethod
    def _promote(cls, obj):
        try:
            return super(Array, cls)._promote(obj)
        except TypeError:
            # `_promote` contract expectes ProxyTypeError, not TypeError
            raise ProxyTypeError("Cannot promote {} to {}".format(obj, cls))

    @property
    def dtype(self):
        """The type of the data contained in the Array.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.dtype
        <class 'descarteslabs.workflows.types.primitives.number.Float'>
        """
        return self._type_params[0]

    @property
    def ndim(self):
        """The number of dimensions of the Array.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.ndim
        3
        """
        return self._type_params[1]

    @property
    def shape(self):
        """The shape of the Array. If the shape of the Array is unknown along a dimension, it will be -1.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> arr = rgb.ndarray
        >>> # The x and y pixel shapes are dependent upon 'geoctx'
        >>> arr.shape.compute(geoctx) # doctest: +SKIP
        (3, 512, 512)
        """
        return_type = Tuple[(Int,) * self.ndim]
        return return_type._from_apply("array.shape", self)

    def reshape(self, *newshape):
        """
        Returns an `Array` containing the same data with a new shape.

        See `~.numpy.reshape` for full documentation.
        """
        from ..numpy import reshape

        return reshape(self, newshape)

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

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR",
        ...     start_datetime="2017-01-01", end_datetime="2017-12-01")
        >>>
        >>> # Take images 1, 2, and 3, as well as their first 3 bands
        >>> # This complicated indexing cannot be done on an ImageCollection
        >>> # so we index the underlying Array instead
        >>> arr = col.ndarray[[1, 2, 3], :3]
        >>>
        >>> # Construct a new ImageCollection with specified bandinfo
        >>> new_col = arr.to_imagery(bandinfo={"red": {}, "green": {}, "blue": {}})
        >>> new_col.compute(geoctx) # doctest: +SKIP
        ImageCollectionResult of length 3:
          * ndarray: MaskedArray<shape=(3, 3, 512, 512), dtype=float64>
          * properties: 3 items
          * bandinfo: 'red', 'green', 'blue'
          * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
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

    def __array_function__(self, func, types, args, kwargs):
        """
        Override the behavior of a subset of NumPy functionality.

        Parameters
        ----------
        func: The NumPy function object that was called
        types: Collection of unique argument types from the original NumPy function
            call that implement `__array_function__`
        args: arguments directly passed from the original call
        kwargs: kwargs directly passed from the original call
        """
        from descarteslabs.workflows.types.numpy import numpy_overrides

        if func not in numpy_overrides.HANDLED_FUNCTIONS:
            raise NotImplementedError(
                "Using `{}` with a Workflows "
                "Array is not supported. If you want to use "
                "this function, you will first need to call "
                "`.compute` on your Workflows Array.".format(func.__name__)
            )

        try:
            return numpy_overrides.HANDLED_FUNCTIONS[func](*args, **kwargs)
        except TypeError as e:
            e.args = (
                "When attempting to call numpy.{} with a "
                "Workflows Array, the following error occurred:\n\n".format(
                    func.__name__
                )
                + e.args[0],
            )
            raise

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        """
        Override the behavior of NumPy's ufuncs.

        Parameters
        ----------
        ufunc: The ufunc object that was called
        method: Which ufunc method was called (one of "__call__", "reduce",
            "reduceat", "accumulate", "outer" or "inner")
        inputs: Tuple of the input arguments to ufunc
        kwargs: Dict of optional input arguments to ufunc
        """
        from descarteslabs.workflows.types.numpy import numpy_overrides

        if method == "__call__":
            if ufunc.__name__ not in numpy_overrides.HANDLED_UFUNCS:
                return NotImplemented
            else:
                return numpy_overrides.HANDLED_UFUNCS[ufunc.__name__](*inputs, **kwargs)
        else:
            # We currently don't support ufunc methods apart from __call__
            return NotImplemented

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
        """ Minimum along a given axis.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.min(axis=2).compute(geoctx) # doctest: +SKIP
        masked_array(
          data=[[0.0901, 0.0901, 0.0901, ..., 0.1025, 0.1025, 0.1025],
                [0.0642, 0.0645, 0.065 , ..., 0.0792, 0.0788, 0.079 ],
                [0.0462, 0.0462, 0.0464, ..., 0.0614, 0.0616, 0.0622],
                ...,
                [0.    , 0.    , 0.    , ..., 0.    , 0.    , 0.    ],
                [0.    , 0.    , 0.    , ..., 0.    , 0.    , 0.    ],
                [0.    , 0.    , 0.    , ..., 0.    , 0.    , 0.    ]],
        mask=False,
        fill_value=1e+20)
        """
        return self._stats_return_type(axis)._from_apply("min", self, axis)

    def max(self, axis=None):
        """ Maximum along a given axis.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.max(axis=2).compute(geoctx) # doctest: +SKIP
        masked_array(
          data=[[0.3429, 0.3429, 0.3429, ..., 0.4685, 0.4685, 0.4685],
                [0.4548, 0.4758, 0.5089, ..., 0.4457, 0.4548, 0.4589],
                [0.4095, 0.4338, 0.439 , ..., 0.417 , 0.4261, 0.4361],
                ...,
                [0.    , 0.    , 0.    , ..., 0.    , 0.    , 0.    ],
                [1.    , 1.    , 1.    , ..., 1.    , 1.    , 1.    ],
                [1.    , 1.    , 1.    , ..., 1.    , 1.    , 1.    ]],
        mask=False,
        fill_value=1e+20)
        """
        return self._stats_return_type(axis)._from_apply("max", self, axis)

    def mean(self, axis=None):
        """ Mean along a given axis.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.mean(axis=2).compute(geoctx) # doctest: +SKIP
        masked_array(
          data=[[0.12258809, 0.12258809, 0.12258809, ..., 0.20478262, 0.20478262, 0.20478262],
                [0.11682598, 0.11911875, 0.11996387, ..., 0.17967012, 0.18027852, 0.1817543 ],
                [0.10004336, 0.10156348, 0.10262227, ..., 0.17302051, 0.17299277, 0.17431074],
                ...,
                [0.        , 0.        , 0.        , ..., 0.        , 0.        , 0.        ],
                [0.00390625, 0.00390625, 0.00390625, ..., 0.05859375, 0.05859375, 0.05859375],
                [0.00390625, 0.00390625, 0.00390625, ..., 0.05859375, 0.05859375, 0.05859375]],
        mask=False,
        fill_value=1e+20)
        """
        return self._stats_return_type(axis)._from_apply("mean", self, axis)

    def median(self, axis=None):
        """ Median along a given axis.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.median(axis=2).compute(geoctx) # doctest: +SKIP
        masked_array(
          data=[[0.1128 , 0.1128 , 0.1128 , ..., 0.1613 , 0.1613 , 0.1613 ],
                [0.0881 , 0.08595, 0.08545, ..., 0.133  , 0.1306 , 0.13135],
                [0.0739 , 0.0702 , 0.0695 , ..., 0.13035, 0.13025, 0.1308 ],
                ...,
                [0.     , 0.     , 0.     , ..., 0.     , 0.     , 0.     ],
                [0.     , 0.     , 0.     , ..., 0.     , 0.     , 0.     ],
                [0.     , 0.     , 0.     , ..., 0.     , 0.     , 0.     ]],
        mask=False,
        fill_value=1e+20)
        """
        return self._stats_return_type(axis)._from_apply("median", self, axis)

    def sum(self, axis=None):
        """ Sum along a given axis.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.sum(axis=2).compute(geoctx) # doctest: +SKIP
        masked_array(
          data=[[ 62.7651,  62.7651,  62.7651, ..., 104.8487, 104.8487, 104.8487],
                [ 59.8149,  60.9888,  61.4215, ...,  91.9911,  92.3026,  93.0582],
                [ 51.2222,  52.0005,  52.5426, ...,  88.5865,  88.5723,  89.2471],
                ...,
                [  0.    ,   0.    ,   0.    , ...,   0.    ,   0.    ,   0.    ],
                [  2.    ,   2.    ,   2.    , ...,  30.    ,  30.    ,  30.    ],
                [  2.    ,   2.    ,   2.    , ...,  30.    ,  30.    ,  30.    ]],
        mask=False,
        fill_value=1e+20)
        """
        return self._stats_return_type(axis)._from_apply("sum", self, axis)

    def std(self, axis=None):
        """ Standard deviation along a given axis.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.std(axis=2).compute(geoctx) # doctest: +SKIP
        masked_array(
          data=[[0.04008153, 0.04008153, 0.04008153, ..., 0.09525769, 0.09525769, 0.09525769],
                [0.08456076, 0.09000384, 0.09356615, ..., 0.09512879, 0.09453823, 0.09345682],
                [0.07483621, 0.08026347, 0.08554651, ..., 0.0923489 , 0.09133476, 0.09047391],
                ...,
                [0.        , 0.        , 0.        , ..., 0.        , 0.        , 0.        ],
                [0.06237781, 0.06237781, 0.06237781, ..., 0.23486277, 0.23486277, 0.23486277],
                [0.06237781, 0.06237781, 0.06237781, ..., 0.23486277, 0.23486277, 0.23486277]],
        mask=False,
        fill_value=1e+20)
        """
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
                if idx_elem._element_type not in (Int, Bool):
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
