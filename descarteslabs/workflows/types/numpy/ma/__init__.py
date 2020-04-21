from .numpy_ma import np_ma

globals().update(np_ma)

__all__ = list(np_ma.keys())
