import numpy as np

from ..core.codegen import wf_func
from .signatures import (
    NUMPY_SIGNATURES as NP_SIGS,
    DISPLAY_SIGNATURE_OVERRIDES as SIG_OVERRIDES,
)
from .utils import copy_docstring_from_numpy


HANDLED_FUNCTIONS = {}


def np_func(numpy_func, name, signatures):
    """
    Generate a NumPy function implementation
    that handles Workflows types and can have multiple signatures.

    If multiple signatures are given, they will be dispatched
    based on both argument types and keyword argument types.

    Also handles typechecking and promotion.

    Parameters
    ----------
    numpy_func: NumPy function
        The NumPy function to add Workflows type handling to.
    name: str
        The name of the NumPy function.
    signatures: inspect.Signature
        A Signature or list of Signatures the function will accept.
        Used for dispatching and typechecking.
    """
    func = wf_func(
        name,
        signatures,
        merged_signature=SIG_OVERRIDES.get(name, None),
        namespace="wf.numpy",
    )
    HANDLED_FUNCTIONS[numpy_func] = func
    copy_docstring_from_numpy(func, numpy_func)
    return func


np_funcs = {
    name: np_func(getattr(np, name), name, sigs) for name, sigs in NP_SIGS.items()
}
