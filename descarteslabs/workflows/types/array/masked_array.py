import numpy as np

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..primitives import Bool, Int, Float
from ..core import ProxyTypeError
from .array_ import Array
from .base_array import BaseArray


@serializable()
class MaskedArray(BaseArray):
    """
    Proxy MaskedArray representing a multidimensional, homogenous array of fixed-size items
    that may have missing or invalid entries.
    MaskedArray follows the same syntax as NumPy masked arrays. It supports vectorized operations, broadcasting,
    and multidimensional indexing. There are some limitations including slicing with lists/arrays in multiple
    axes (``x[[1, 2, 3], [3, 2, 1]]``) and slicing with a multidimensional list/array of integers.

    Note
    ----
    MaskedArray is an experimental API. It may be changed in the future, will not necessarily be
    backwards compatible, and may have unexpected bugs. Please contact us with any feedback!

    Examples
    --------
    >>> import descarteslabs.workflows as wf
    >>> arr = wf.MaskedArray(data=[1, 2, 3, 4], mask=[True, False, False, True], fill_value=0)
    >>> arr
    <descarteslabs.workflows.types.array.masked_array.MaskedArray object at 0x...>
    >>> arr.compute(geoctx) # doctest: +SKIP
    masked_array(data=[--, 2, 3, --],
                 mask=[ True, False, False,  True],
           fill_value=0)
    """

    def __init__(self, data, mask=False, fill_value=None):
        if isinstance(data, np.ndarray):
            if data.dtype.kind not in ("b", "i", "f"):
                raise TypeError(
                    "Invalid dtype {} for {}".format(data.dtype, type(self).__name__)
                )
            data = data.tolist()

        if isinstance(mask, (bool, np.bool_, Bool)):
            if isinstance(mask, np.bool_):
                mask = mask is True
            mask = Bool._promote(mask)
        elif isinstance(mask, np.ndarray):
            if mask.dtype.kind != "b":
                raise TypeError(
                    "Invalid dtype {} for a mask array, should be boolean".format(
                        mask.dtype
                    )
                )
            mask = mask.tolist()

        fill_value = _promote_fill_value(self, fill_value)
        self.graft = client.apply_graft("maskedarray.create", data, mask, fill_value)

    @classmethod
    def from_numpy(cls, arr):
        """
        Construct a Workflows MaskedArray from a NumPy MaskedArray, inferring `dtype`
        and `ndim`.

        Parameters
        ----------
        arr: numpy.ma.MaskedArray

        Returns
        -------
        ~descarteslabs.workflows.MaskedArray
        """
        try:
            data = np.ma.getdata(arr)
        except Exception:
            data = arr

        try:
            mask = np.ma.getmask(arr)
        except Exception:
            mask = False

        fill_value = getattr(arr, "fill_value", None)
        return cls(data, mask, fill_value)

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, (Int, Float, Bool)):
            return MaskedArray(obj)

        try:
            return obj.cast(cls)
        except Exception:
            if not isinstance(obj, np.ndarray):
                obj = np.asarray(obj)
            try:
                return MaskedArray.from_numpy(obj)
            except Exception:
                raise ProxyTypeError("Cannot promote {} to MaskedArray".format(obj))

    def getdata(self):
        """The data array underlying this `MaskedArray`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.getdata().compute(geoctx) # doctest: +SKIP
        array([[[0.3429, 0.3429, 0.3429, ..., 0.0952, 0.0952, 0.0952],
                [0.3429, 0.3429, 0.3429, ..., 0.0952, 0.0952, 0.0952],
                [0.3429, 0.3429, 0.3429, ..., 0.0952, 0.0952, 0.0952],
        ...
        """
        return Array._from_apply("maskedarray.getdata", self)

    def getmaskarray(self):
        """The mask array underlying this `MaskedArray`.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.getmaskarray().compute(geoctx) # doctest: +SKIP
        array([[[False, False, False, ..., False, False, False],
                [False, False, False, ..., False, False, False],
                [False, False, False, ..., False, False, False],
        ...
        """
        return Array._from_apply("maskedarray.getmaskarray", self)

    def filled(self, fill_value=None):
        """
        Returns an Array with all masked data replaced by the given fill value.
        If no `fill_value` argument is provided, the fill value on this `MaskedArray`
        will be used.

        Parameters
        ----------
        fill_value: scalar or Array, optional, default None
            The value used to replace masked data.

        Returns
        -------
        a: Array

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> # In this case, 'geoctx' results in fully masked data
        >>> arr.filled(0.1).compute(geoctx) # doctest: +SKIP
        array([[[0.1, 0.1, 0.1, ..., 0.1, 0.1, 0.1],
                [0.1, 0.1, 0.1, ..., 0.1, 0.1, 0.1],
                [0.1, 0.1, 0.1, ..., 0.1, 0.1, 0.1],
        ...
        """
        fill_value = _promote_fill_value(self, fill_value)
        return Array._from_apply("maskedarray.filled", self, fill_value)

    def count(self, axis=None):
        """ Count unmasked pixels along a given axis.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> img = wf.Image.from_id("sentinel-2:L1C:2019-05-04_13SDV_99_S2B_v1")
        >>> arr = img.ndarray
        >>> arr.count(axis=2).compute(geoctx) # doctest: +SKIP
        masked_array(
          data=[[512., 512., 512., ..., 512., 512., 512.],
                [512., 512., 512., ..., 512., 512., 512.],
                [512., 512., 512., ..., 512., 512., 512.],
                ...,
                [512., 512., 512., ..., 512., 512., 512.],
                [512., 512., 512., ..., 512., 512., 512.],
                [512., 512., 512., ..., 512., 512., 512.]],
        mask=False,
        fill_value=1e+20)
        """
        return self._stats_return_type(axis)._from_apply("count", self, axis)


def _promote_fill_value(self, fill_value):
    if fill_value is None:
        return None
    if isinstance(fill_value, Array):
        return fill_value

    if isinstance(fill_value, np.generic):
        cast_func = None
        if isinstance(fill_value, np.integer):
            cast_func = int
        elif isinstance(fill_value, np.float):
            cast_func = float
        elif isinstance(fill_value, (np.bool, np.bool_)):
            cast_func = bool

        if cast_func is not None:
            fill_value = cast_func(fill_value)
        return fill_value

    if isinstance(fill_value, (Int, Float, Bool)):
        return fill_value
