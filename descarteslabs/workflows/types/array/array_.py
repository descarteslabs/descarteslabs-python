import numpy as np

from descarteslabs.common.graft import client

from ...cereal import serializable
from ..core import ProxyTypeError
from ..containers import List
from ..primitives import Int, Float, Bool
from .base_array import BaseArray


DTYPE_KIND_TO_WF = {"b": Bool, "i": Int, "f": Float}
WF_TO_DTYPE_KIND = dict(zip(DTYPE_KIND_TO_WF.values(), DTYPE_KIND_TO_WF.keys()))


@serializable()
class Array(BaseArray):
    """
    Proxy Array representing a multidimensional, homogenous array of fixed-size items.

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
    >>> arr = wf.Array([1, 2, 3, 4, 5])
    >>> arr
    <descarteslabs.workflows.types.array.array_.Array object at 0x...>
    >>> arr.compute(geoctx) # doctest: +SKIP
    array([1, 2, 3, 4, 5])

    >>> import numpy as np
    >>> import descarteslabs.workflows as wf
    >>> ndarray = np.ones((3, 10, 10))
    >>> # Create an Array from the 3-dimensional numpy array
    >>> arr = wf.Array(ndarray)
    >>> arr
    <descarteslabs.workflows.types.array.array_.Array object at 0x...>
    """

    def __init__(self, arr):
        if isinstance(arr, np.generic):
            arr = arr.tolist()
        if isinstance(arr, (int, float, bool)):
            self._literal_value = arr
            self.graft = client.apply_graft("wf.array.create", arr)
        elif isinstance(arr, (Int, Float, Bool, List)):
            self.graft = client.apply_graft("wf.array.create", arr)
        else:
            if not isinstance(arr, np.ndarray):
                try:
                    arr = np.asarray(arr)
                except Exception:
                    raise ValueError("Cannot construct Array from {!r}".format(arr))

            if arr.dtype.kind not in ("b", "i", "f"):
                raise TypeError("Invalid dtype {} for an Array".format(arr.dtype))

            self._literal_value = arr
            arr_list = arr.tolist()
            self.graft = client.apply_graft("wf.array.create", arr_list)

        self.params = getattr(arr, "params", ())

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        try:
            return obj.cast(cls)
        except Exception:
            try:
                return Array(obj)
            except Exception as e:
                raise ProxyTypeError("Cannot promote {} to Array: {}".format(obj, e))
