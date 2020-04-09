import numpy as np

from typing import Union
from inspect import Parameter, Signature
from descarteslabs.common.typing import get_args, get_origin

from ..core import ProxyTypeError
from ..primitives import Float, Int, Bool, Str, NoneType
from ..array import Array, MaskedArray
from ..containers import List, Tuple, Dict
from .numpy_ufuncs import derived_from


HANDLED_FUNCTIONS = {}


def promote_to_signature(signature, *args, **kwargs):
    """
    Helper for typechecking and promoting args and kwargs to
    a signature.

    Raises a TypeError if the args and kwargs are not compatible with the signature,
    and a ProxyTypeError if their types are not compatible with the annotations.

    Parameters
    ----------
    signature: inspect.Signature
        The signature to check compatibility with.
    *args: tuple
    **kwargs: dict

    Returns
    -------
    The signature, the promoted args, and the promoted kwargs.
    """
    bound_sig = signature.bind(*args, **kwargs)
    bound_sig.apply_defaults()

    promoted_args = []
    promoted_kwargs = {}
    for (b_name, bound), (p_name, param) in zip(
        bound_sig.arguments.items(), signature.parameters.items()
    ):
        arg_type = param.annotation
        if arg_type is Parameter.empty:
            raise TypeError(
                "Argument {} does not have a type annotation.".format(p_name)
            )

        if not isinstance(arg_type, tuple):
            if get_origin(arg_type) is Union:
                arg_types = get_args(arg_type)
            else:
                arg_types = (arg_type,)

        if param.kind is not Parameter.VAR_POSITIONAL:
            bound = (bound,)

        promoted = []
        for type_ in arg_types:
            if len(promoted) == len(bound):
                break
            if param.kind is Parameter.VAR_KEYWORD:
                type_ = Dict[Str, type_]
            for b in bound:
                if isinstance(b, type_):
                    promoted.append(b)
                else:
                    try:
                        arg = type_._promote(b)
                    except ProxyTypeError:
                        break
                    else:
                        promoted.append(arg)

        if len(promoted) > 0:
            if param.kind in (Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD):
                promoted_args += promoted
            else:
                if (
                    param.kind is Parameter.POSITIONAL_OR_KEYWORD
                    and param.default is Parameter.empty
                ):
                    promoted_args.append(promoted[0])
                else:
                    promoted_kwargs[p_name] = promoted[0]
        else:
            raise ProxyTypeError(p_name)

    return_type = signature.return_annotation
    if return_type is Parameter.empty:
        raise TypeError("No return type specified in signature: {}".format(signature))

    return promoted_args, promoted_kwargs, signature.return_annotation


def format_dispatch_error(name, signatures, failed, *args, **kwargs):
    """
    Helper for use in the generic case of dispatched functions.
    Returns (not raises) a TypeError listing the types
    for which a function is registered to be called.

    Parameters
    ----------
    name: str
        Name of function that failed to dispatch.
    signatures: list
        The list of signatures the function accepts.
    *args: tuple
        The args the function was called with.
    **kwargs: dict
        The kwargs the function was called with.
    """
    valid_types = ",\n".join(
        "({})".format(
            ", ".join(
                "{}: {}".format(
                    k,
                    "({})".format(", ".join(a.__name__ for a in arg.annotation))
                    if isinstance(arg.annotation, tuple)
                    else "Union[{}]".format(
                        ", ".join(t.__name__ for t in get_args(arg.annotation))
                    )
                    if get_origin(arg.annotation) is Union
                    else arg.annotation.__name__,
                )
                for k, arg in sig.parameters.items()
            )
        )
        for sig in signatures
    )

    arg_types = ", ".join("{} = {}".format(type(arg).__name__, arg) for arg in args)
    kwarg_types = ", ".join(
        "{}: {} = {}".format(k, type(arg).__name__, arg) for k, arg in kwargs.items()
    )
    called_with = ", ".join(x for x in [arg_types, kwarg_types] if len(x) > 0)

    return TypeError(
        "Cannot call function {} with types: ({}). "
        "Must be one of:\n{}\n\n"
        "Specifically, the promotion of {} failed.".format(
            name, called_with, valid_types, ", ".join(repr(p) for p in set(failed))
        )
    )


def wf_func(func_name, signatures, doc=None):
    """
    Generate a function that can have multiple signatures.

    If multiple signatures are given, they will be dispatched
    based on both argument types and keyword argument types.

    Also handles typechecking and promotion.

    Parameters
    ----------
    func_name: str
        The name of the generated function.
    signatures: inspect.Signature
        A Signature or list of Signatures the function will accept.
        Used for dispatching and typechecking.
    doc: str, optional
        The function's docstring.
    """
    if not isinstance(signatures, list):
        signatures = [signatures]

    def func(*args, **kwargs):
        failed = []
        return_type = None
        for sig in signatures:
            try:
                promoted_args, promoted_kwargs, return_type = promote_to_signature(
                    sig, *args, **kwargs
                )
            except ProxyTypeError as e:
                failed.append(str(e))
                pass
            else:
                break

        if return_type is None:
            raise format_dispatch_error(func_name, signatures, failed, *args, **kwargs)

        return return_type._from_apply(func_name, *promoted_args, **promoted_kwargs)

    func.__name__ = func_name
    if doc is not None:
        func.__doc__ = doc
    return func


def np_func(numpy_func, signatures):
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
    signatures: inspect.Signature
        A Signature or list of Signatures the function will accept.
        Used for dispatching and typechecking.
    """
    func = wf_func(numpy_func.__name__, signatures)
    HANDLED_FUNCTIONS[numpy_func] = func
    derived_from(numpy_func)(func)
    return func


EMPTY = Parameter.empty
Param = lambda name, annotation=None, default=None, kind=Parameter.POSITIONAL_OR_KEYWORD: Parameter(  # noqa: E731
    name, kind=kind, default=default, annotation=annotation
)
Sig = lambda parameters, return_annotation: Signature(  # noqa: E731
    parameters=parameters, return_annotation=return_annotation
)

# Argmin + argmax
sig = Sig([Param("a", Array, EMPTY), Param("axis", Int)], Array)
sig2 = Sig([Param("a", Array, EMPTY), Param("axis", NoneType)], Int)
argmin = np_func(np.argmin, signatures=[sig, sig2])
argmax = np_func(np.argmax, signatures=[sig, sig2])

# Any + all
sig_masked = Sig(
    [Param("a", MaskedArray, EMPTY), Param("axis", Union[Int, List[Int]])], MaskedArray
)
sig = Sig([Param("a", Array, EMPTY), Param("axis", Union[Int, List[Int]])], Array)
sig2 = Sig([Param("a", Array, EMPTY), Param("axis", NoneType)], Bool)
all = np_func(np.all, signatures=[sig, sig2, sig_masked])
any = np_func(np.any, signatures=[sig, sig2, sig_masked])

# Stack + concatenate
sig_masked = Sig(
    [Param("arrays", List[MaskedArray], EMPTY), Param("axis", Int, 0)], MaskedArray
)
sig = Sig([Param("arrays", List[Array], EMPTY), Param("axis", Int, 0)], Array)
concatenate = np_func(np.concatenate, signatures=[sig, sig_masked])
stack = np_func(np.stack, signatures=[sig, sig_masked])

# Transpose
sig_masked = Sig(
    [Param("a", MaskedArray, EMPTY), Param("axes", Union[List[Int], NoneType])],
    MaskedArray,
)
sig = Sig([Param("a", Array, EMPTY), Param("axes", Union[List[Int], NoneType])], Array)
transpose = np_func(np.transpose, signatures=[sig, sig_masked])

# Reshape
sig_masked = Sig(
    [Param("a", MaskedArray, EMPTY), Param("newshape", List[Int], EMPTY)], MaskedArray
)
sig = Sig([Param("a", Array, EMPTY), Param("newshape", List[Int], EMPTY)], Array)
reshape = np_func(np.reshape, signatures=[sig, sig_masked])

# Histogram
sig = Sig(
    [
        Param("a", Array, EMPTY),
        Param("bins", Union[List[Int], List[Float], Int], 10),
        Param("range", Union[Tuple[Int, Int], Tuple[Float, Float], NoneType]),
        Param("weights", Union[Array, NoneType]),
        Param("density", Union[Bool, NoneType]),
    ],
    Tuple[Array, Array],
)
histogram = np_func(np.histogram, signatures=sig)
