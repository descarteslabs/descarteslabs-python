from ..core import typecheck_promote

from . import List
from ..primitives import Int, NoneType


@typecheck_promote(start=(Int, NoneType), stop=(Int, NoneType), step=(Int, NoneType))
def range(start=None, stop=None, step=1):
    """
    Returns a `List` of `Int` containing a sequence of numbers starting from
    0 by default, incremented by 1 (default), ending at a specified number.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> my_range = wf.range(start=2, stop=10, step=2)
    >>> my_range
    <descarteslabs.workflows.types.containers.list_.List[Int] object at 0x...>
    >>> my_range.compute() # doctest: +SKIP
    [2, 4, 6, 8]
    """
    if isinstance(stop, NoneType):
        if isinstance(start, NoneType):
            raise TypeError("Must pass the stop value to range")
        return List[Int]._from_apply("wf.range", start)
    return List[Int]._from_apply("wf.range", start, stop, step=step)
