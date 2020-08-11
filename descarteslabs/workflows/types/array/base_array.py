from ... import env
from .dtype import DType
from .scalar import Scalar
from ..containers import Dict, KnownDict, Ellipsis as wf_Ellipsis, List, Slice, Tuple
from ..core import typecheck_promote, allow_reflect, Proxytype, ProxyTypeError
from ..mixins import NumPyMixin
from ..primitives import Bool, Int, Str, NoneType


def _delayed_numpy():
    # avoid circular imports
    from descarteslabs.workflows.types import numpy

    return numpy


class BaseArray(NumPyMixin, Proxytype):
    @property
    def dtype(self):
        """
        The type of the data contained in the Array.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.dtype.compute(geoctx) # doctest: +SKIP
        dtype("float64")
        """

        return DType._from_apply("wf.array.dtype", self)

    @property
    def ndim(self):
        """
        The number of dimensions of the Array.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.ndim.compute(geoctx) # doctest: +SKIP
        3
        """
        return Int._from_apply("wf.array.ndim", self)

    @property
    def shape(self):
        """
        The shape of the Array. If the shape of the Array is unknown along a
        dimension, it will be -1.

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
        return List[Int]._from_apply("wf.array.shape", self)

    @property
    def size(self):
        """
        The number of elements in the Array. If the shape of the Array is unknown
        along a dimension, the resulting size is also unknown. In this case `size`
        will return -1.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> rgb = img.pick_bands("red green blue")
        >>> arr = rgb.ndarray
        >>> arr.size.compute() # doctest: +SKIP
        786432
        """
        return Int._from_apply("wf.array.size", self)

    @property
    def literal_value(self):
        "Python literal value this proxy object was constructed with, or None if not constructed from a literal value."
        return getattr(self, "_literal_value", None)

    @typecheck_promote((Str, DType))
    def astype(self, dtype):
        """
        Return ``Array`` cast to a specific type.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> arr = wf.Array([True, False, True])
        >>> int_arr = arr.astype("int")
        >>> int_arr.dtype.compute() #doctest: +SKIP
        dtype('int64')
        >>> int_arr.compute() # doctest: +SKIP
        array([1, 0, 1])
        """
        return self._from_apply("wf.array.astype", self, dtype)

    def flatten(self):
        """
        Return a contiguous flattened version of `Array`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> arr = wf.Array([[1, 2, 3], [4, 5, 6]])
        >>> arr.flatten().compute() # doctest: +SKIP
        array([1, 2, 3, 4, 5, 6])
        """
        return self._from_apply("wf.array.flatten", self)

    def reshape(self, *newshape):
        """
        Returns an `Array` containing the same data with a new shape.

        See `~.numpy.reshape` for full documentation.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> arr = wf.Array([[1, 2, 3], [4, 5, 6]])
        >>> arr.reshape(-1).compute() # doctest: +SKIP
        array([1, 2, 3, 4, 5, 6])
        """
        return _delayed_numpy().reshape(self, newshape)

    def to_imagery(self, properties=None, bandinfo=None):
        """
        Turns a proxy Array into an `~.geospatial.ImageCollection`.

        Note that this function always returns an `~.geospatial.ImageCollection`, even if the
        Array is only 3D. If you are expecting an `~.geospatial.Image`, you can index into the result like `my_col[0]`.

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
        from ..geospatial import ImageCollection

        if not isinstance(properties, (type(None), dict, list, Dict, KnownDict, List)):
            raise TypeError(
                "Provided properties must be a Dict (3-dimensional Array) or List (4-dimensional Array), got {}".format(
                    type(properties)
                )
            )

        if not isinstance(bandinfo, (type(None), dict, Dict, KnownDict)):
            raise TypeError(
                "Provided bandinfo must be a Dict, got {}".format(type(properties))
            )

        return ImageCollection._from_apply(
            "wf.to_imagery", self, properties, bandinfo, env.geoctx
        )

    def __getitem__(self, idx):
        idx = typecheck_getitem(idx)

        return self._from_apply("wf.array.getitem", self, idx)

    def __neg__(self):
        return _delayed_numpy().negative(self)

    def __pos__(self):
        return self._from_apply("wf.pos", self)

    def __abs__(self):
        return _delayed_numpy().absolute(self)

    @allow_reflect
    def __lt__(self, other):
        return _delayed_numpy().less(self, other)

    @allow_reflect
    def __le__(self, other):
        return _delayed_numpy().less_equal(self, other)

    @allow_reflect
    def __gt__(self, other):
        return _delayed_numpy().greater(self, other)

    @allow_reflect
    def __ge__(self, other):
        return _delayed_numpy().greater_equal(self, other)

    @allow_reflect
    def __eq__(self, other):
        return _delayed_numpy().equal(self, other)

    @allow_reflect
    def __ne__(self, other):
        return _delayed_numpy().not_equal(self, other)

    @allow_reflect
    def __add__(self, other):
        return _delayed_numpy().add(self, other)

    @allow_reflect
    def __sub__(self, other):
        return _delayed_numpy().subtract(self, other)

    @allow_reflect
    def __mul__(self, other):
        return _delayed_numpy().multiply(self, other)

    @allow_reflect
    def __div__(self, other):
        return _delayed_numpy().divide(self, other)

    @allow_reflect
    def __floordiv__(self, other):
        return _delayed_numpy().floor_divide(self, other)

    @allow_reflect
    def __truediv__(self, other):
        return _delayed_numpy().true_divide(self, other)

    @allow_reflect
    def __mod__(self, other):
        return _delayed_numpy().mod(self, other)

    @allow_reflect
    def __pow__(self, other):
        return _delayed_numpy().power(self, other)

    @allow_reflect
    def __radd__(self, other):
        return _delayed_numpy().add(other, self)

    @allow_reflect
    def __rsub__(self, other):
        return _delayed_numpy().subtract(other, self)

    @allow_reflect
    def __rmul__(self, other):
        return _delayed_numpy().multiply(other, self)

    @allow_reflect
    def __rdiv__(self, other):
        return _delayed_numpy().divide(other, self)

    @allow_reflect
    def __rfloordiv__(self, other):
        return _delayed_numpy().floor_divide(other, self)

    @allow_reflect
    def __rtruediv__(self, other):
        return _delayed_numpy().true_divide(other, self)

    @allow_reflect
    def __rmod__(self, other):
        return _delayed_numpy().mod(other, self)

    @allow_reflect
    def __rpow__(self, other):
        return _delayed_numpy().power(other, self)

    @allow_reflect
    def __and__(self, other):
        return _delayed_numpy().bitwise_and(self, other)

    @allow_reflect
    def __or__(self, other):
        return _delayed_numpy().bitwise_or(self, other)

    @allow_reflect
    def __rand__(self, other):
        return _delayed_numpy().bitwise_and(other, self)

    @allow_reflect
    def __ror__(self, other):
        return _delayed_numpy().bitwise_or(other, self)

    @allow_reflect
    def __rxor__(self, other):
        return _delayed_numpy().bitwise_xor(other, self)

    @allow_reflect
    def __xor__(self, other):
        return _delayed_numpy().bitwise_xor(self, other)

    def __invert__(self):
        return _delayed_numpy().bitwise_not(self)

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
        return _delayed_numpy().min(self, axis)

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
        return _delayed_numpy().max(self, axis)

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
        return _delayed_numpy().mean(self, axis)

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
        return self._stats_return_type(axis)._from_apply("wf.median", self, axis=axis)

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
        return _delayed_numpy().sum(self, axis)

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
        return _delayed_numpy().std(self, axis)

    def _stats_return_type(self, axis):
        if axis is None:
            return_type = Scalar
        else:
            return_type = type(self)
        return return_type


def typecheck_getitem(idx):
    list_or_array_seen = False
    proxy_idx = []

    if not isinstance(idx, (tuple, Tuple)):
        idx = (idx,)

    for i, idx_elem in enumerate(idx):
        if isinstance(idx_elem, (int, Int)):
            proxy_idx.append(Int._promote(idx_elem))
        elif isinstance(idx_elem, (slice, Slice)):
            proxy_idx.append(Slice._promote(idx_elem))
        elif isinstance(idx_elem, type(Ellipsis)):
            proxy_idx.append(wf_Ellipsis())
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

        elif isinstance(idx_elem, BaseArray):
            if list_or_array_seen:
                raise ValueError(
                    "While slicing Array, position {}: cannot slice an Array "
                    "with lists or Arrays in multiple axes.".format(i)
                )
            list_or_array_seen = True
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

    return proxy_idx
