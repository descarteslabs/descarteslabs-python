from ..core import ProxyTypeError

from .list_ import List
from .tuple_ import Tuple


def zip(*lists):
    """
    Returns a `List` of `Tuple`, where each tuple contains the i-th element
    from each of the argument `List`. All arguments must be Proxytype `List`.
    The returned `List` is truncated in length to the length of the shortest
    argument sequence.
    """
    for i, seq in enumerate(lists):
        if not isinstance(seq, List):
            raise ProxyTypeError(
                "All arguments to 'zip' must be Proxytype Lists, "
                "but argument {} is {!r}: {}".format(i, type(seq).__name__, seq)
            )
    itemtypes = tuple(l._type_params[0] for l in lists)
    tuple_type = Tuple[itemtypes]

    return List[tuple_type]._from_apply("zip", *lists)
