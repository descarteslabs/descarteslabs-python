from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import Proxytype, typecheck_promote
from ..primitives import Int, NoneType


@serializable()
class Slice(Proxytype):
    """
    Proxy Slice object.
    Internal class used as a promotion target for Python slices.
    You shouldn't use this directly; Python slices can be used to achieve the same result.

    Examples
    --------
    >>> from descarteslabs.workflows import List, Int
    >>> from descarteslabs.workflows.types.containers import Slice
    >>> my_slice = Slice(0, 4, 2)
    >>> my_slice
    <descarteslabs.workflows.types.containers.slice.Slice object at 0x...>
    >>> my_slice.compute() # doctest: +SKIP
    slice(0, 4, 2)
    >>> my_list = List[Int]([1, 2, 3, 4, 5])
    >>> my_list[my_slice].compute() # doctest: +SKIP
    [1, 3]
    """

    @typecheck_promote(
        start=(Int, NoneType), stop=(Int, NoneType), step=(Int, NoneType)
    )
    def __init__(self, start=None, stop=None, step=None):
        if not (
            (isinstance(start, Int) and start.literal_value is None)
            or (isinstance(stop, Int) and stop.literal_value is None)
            or (isinstance(step, Int) and step.literal_value is None)
        ):
            self._literal_value = slice(
                start.literal_value, stop.literal_value, step.literal_value
            )
        self.graft = client.apply_graft(
            "wf.Slice.create", start=start, stop=stop, step=step
        )

    @classmethod
    def from_slice(cls, obj):
        """Construct a Workflows Slice from a Python slice object.

        Example
        -------
        >>> from descarteslabs.workflows.types.containers import Slice
        >>> py_slice = slice(0, 4, 2)
        >>> my_slice = Slice.from_slice(py_slice)
        >>> my_slice.compute() # doctest: +SKIP
        slice(0, 4, 2)
        """
        return cls(start=obj.start, stop=obj.stop, step=obj.step)

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        elif isinstance(obj, slice):
            return cls.from_slice(obj)
        else:
            return super(Slice, cls)._promote(obj)

    @property
    def literal_value(self):
        "Python literal value this proxy object was constructed with, or None if not constructed from a literal value."
        return getattr(self, "_literal_value", None)
