from ..identifier import identifier
from ..primitives import Float


e = identifier("wf.constants.e", Float)
"""e = 2.71828182845904523536028747135266249775724709369995...

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.e
    <descarteslabs.workflows.types.primitives.number.Float object at 0x...>
    >>> wf.e.compute() # doctest: +SKIP
    2.718281828459045
"""


inf = identifier("wf.constants.inf", Float)
"""Floating point representation of positive infinity.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.inf
    <descarteslabs.workflows.types.primitives.number.Float object at 0x...>
    >>> wf.inf.compute() # doctest: +SKIP
    inf
"""


nan = identifier("wf.constants.nan", Float)
"""Floating point representation of Not a Number.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.nan
    <descarteslabs.workflows.types.primitives.number.Float object at 0x...>
    >>> wf.nan.compute() # doctest: +SKIP
    nan
"""


pi = identifier("wf.constants.pi", Float)
"""pi = 3.1415926535897932384626433...

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.pi
    <descarteslabs.workflows.types.primitives.number.Float object at 0x...>
    >>> wf.pi.compute() # doctest: +SKIP
    3.141592653589793
"""
