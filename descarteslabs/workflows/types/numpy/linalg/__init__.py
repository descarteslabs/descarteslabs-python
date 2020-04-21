from .numpy_linalg import np_linalg

globals().update(np_linalg)

__all__ = list(np_linalg.keys())
