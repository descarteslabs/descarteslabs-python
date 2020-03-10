import functools

import numpy as np

from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import ProxyTypeError
from ..primitives import Bool
from .array_ import Array
from .list_ import List


@serializable()
class MaskedArray(Array):
    """
    ``MaskedArray[DType, NDim]``: Proxy object representing a multidimensional, homogenous array of fixed-size items
    that may have missing or invalid entries.
    The data-type must be a Proxytype (Int, Float, Bool etc.) and the number of dimensions must be a Python integer.
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
    >>> arr = wf.MaskedArray[wf.Int, 1](data=[1, 2, 3, 4], mask=[True, False, False, True], fill_value=0)
    >>> arr
    <descarteslabs.workflows.types.containers.masked_array.MaskedArray[Int, 1] object at 0x...>
    >>> arr.compute(geoctx) # doctest: +SKIP
    masked_array(data=[--, 2, 3, --],
                 mask=[ True, False, False,  True],
           fill_value=0)
    """

    def __init__(self, data, mask=False, fill_value=None):
        if self._type_params is None:
            raise TypeError(
                "Cannot instantiate a generic MaskedArray; "
                "the dtype and dimensionality must be specified (like `MaskedArray[Float, 3]`)"
            )

        data_list_type = functools.reduce(
            lambda accum, cur: List[accum],
            range(self._type_params[1]),
            self._type_params[0],
        )
        try:
            data = data_list_type._promote(data)
        except ProxyTypeError:
            raise ValueError("Cannot instantiate the data Array from {!r}".format(data))

        if isinstance(mask, (bool, np.bool_, Bool)):
            mask = Bool._promote(mask)
        else:
            # TODO(Clark): Support mask broadcasting to data shape?  This could be done
            # client-side or server-side.
            mask_list_type = functools.reduce(
                lambda accum, cur: List[accum], range(self._type_params[1]), Bool
            )
            try:
                mask = mask_list_type._promote(mask)
            except ProxyTypeError:
                raise ValueError(
                    "Cannot instantiate the mask Array from {!r}".format(mask)
                )

        fill_value = _promote_fill_value(self, fill_value)

        self.graft = client.apply_graft("maskedarray.create", data, mask, fill_value)

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
        return_type = Array[self.dtype, self.ndim]
        return return_type._from_apply("maskedarray.getdata", self)

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
        return_type = Array[Bool, self.ndim]
        return return_type._from_apply("maskedarray.getmaskarray", self)

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
        return_type = Array[self.dtype, self.ndim]
        fill_value = _promote_fill_value(self, fill_value)
        return return_type._from_apply("maskedarray.filled", self, fill_value)

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
        if fill_value.dtype != self._type_params[0]:
            raise ValueError(
                "Cannot use fill value {!r}.  The dtype of the fill value array must "
                "be the same as the dtype of the MaskedArray, but the dtype of the fill "
                "value is {!r} and the dtype of the MaskedArray is {!r}.".format(
                    fill_value, fill_value.dtype, self._type_params[0]
                )
            )
        return fill_value

    try:
        return self._type_params[0]._promote(fill_value)
    except ProxyTypeError:
        raise ValueError(
            "Cannot use fill value {!r}. The type of the fill value must be the same "
            "as the dtype of the MaskedArray, but the type of the fill value is {!r} "
            "and the dtype of the MaskedArray is {!r}.".format(
                fill_value, type(fill_value), self._type_params[0]
            )
        )
