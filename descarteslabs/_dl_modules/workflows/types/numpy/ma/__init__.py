from .numpy_ma import np_ma
from ...array import MaskedArray

globals().update(np_ma)

masked_array = MaskedArray

__all__ = list(np_ma.keys()) + ["masked_array"]
