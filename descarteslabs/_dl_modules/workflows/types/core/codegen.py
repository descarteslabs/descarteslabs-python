import sys
from typing import Union
from inspect import Parameter, Signature

from .core import ProxyTypeError
from ..primitives import Str
from ..containers import Dict


class BindError(Exception):
    pass


def is_union(type_):
    return getattr(type_, "__origin__", None) is Union


def promote_to_signature(signature, *args, **kwargs):
    """
    Helper for typechecking and promoting args and kwargs to
    a signature.

    Raises a ValueError if the args/kwargs cannot be bound to the signature,
    a TypeError if the args and kwargs are not compatible with the signature,
    and a ProxyTypeError if their types are not compatible with the annotations.

    Parameters
    ----------
    signature: inspect.Signature
        The signature to check compatibility with.
    *args: tuple
    **kwargs: dict

    Returns
    -------
    The promoted args and promoted kwargs.
    """
    try:
        bound_sig = signature.bind(*args, **kwargs)
    except TypeError:
        raise BindError(
            "Unable to bind arguments {!r} and keyword arguments {!r} to the signature {!r}.".format(
                args, kwargs, signature
            )
        )

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
            if is_union(arg_type):
                arg_types = getattr(arg_type, "__args__")
            else:
                arg_types = (arg_type,)

        if param.kind is not Parameter.VAR_POSITIONAL:
            bound = (bound,)

        promoted = []
        for b in bound:
            if len(promoted) == len(bound):
                break
            for type_ in arg_types:
                if param.kind is Parameter.VAR_KEYWORD:
                    type_ = Dict[Str, type_]
                try:
                    arg = type_._promote(b)
                except (ProxyTypeError, TypeError):
                    pass
                else:
                    promoted.append(arg)
                    break

        if len(promoted) == len(bound):
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

    return promoted_args, promoted_kwargs


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
                        ", ".join(
                            t.__name__ for t in getattr(arg.annotation, "__args__")
                        )
                    )
                    if is_union(arg.annotation)
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

    promotion_message = (
        "Specifically, the promotion of {} failed.".format(
            ", ".join(repr(p) for p in set(failed))
        )
        if len(failed) > 0
        else ""
    )

    return TypeError(
        "Cannot call function {} with types: ({}). "
        "Must be one of:\n{}\n\n{}".format(
            name, called_with, valid_types, promotion_message
        )
    )


def stringify_type(type_):
    """
    Concisely stringify a type for a display-only type annotation.

    * Stringifies plain types as their unqualified ``__name__``.
    * Turns Union[X, None] into Optional[X].
    * Stringifies Unions and Optionals as the literal string ``"Union[...]"``,
      to prevent ugly `ForwardRef('X')`s from showing up in ipython interactive help.
    """
    args = getattr(type_, "__args__") if is_union(type_) else (type_,)
    str_types = set(t.__name__ if not isinstance(t, str) else t for t in args)
    # ^ We use the name here because it prints nicely

    if len(str_types) == 2 and "NoneType" in str_types:
        str_types.remove("NoneType")
        str_types = "Optional[{}]".format(str_types.pop())
    else:
        str_types = tuple(str_types)
        str_types = (
            "Union[{}]".format(", ".join(str_types))
            if len(str_types) > 1
            else str_types[0]
        )

    return str_types


def stringify_signature(signature):
    parameters = []
    for name, param in signature.parameters.items():
        type_ = param.annotation
        str_types = stringify_type(type_)
        parameters.append(param.replace(annotation=str_types))

    return Signature(
        parameters, return_annotation=stringify_type(signature.return_annotation)
    )


def merge_types(t1, t2):
    t1_args = getattr(t1, "__args__") if is_union(t1) else (t1,)
    t2_args = getattr(t2, "__args__") if is_union(t2) else (t2,)
    new_types = list(t1_args + t2_args)
    new_types = tuple(set(new_types))
    return Union[new_types] if len(new_types) > 1 else new_types[0]


def merge_signatures(signatures):
    "Merge a list of signatures into one display signature."
    merged = None
    for sig in signatures:
        if merged is None:
            merged = sig
        else:
            parameters = []
            for (merged_name, merged_param), (sig_name, sig_param) in zip(
                merged.parameters.items(), sig.parameters.items()
            ):
                assert merged_name == sig_name
                assert merged_param.kind == sig_param.kind
                assert merged_param.default == sig_param.default

                assert merged_param.annotation is not Parameter.empty
                assert sig_param.annotation is not Parameter.empty

                new_type = merge_types(merged_param.annotation, sig_param.annotation)
                new_param = merged_param.replace(annotation=new_type)
                parameters.append(new_param)

            assert merged.return_annotation is not Parameter.empty
            assert sig.return_annotation is not Parameter.empty

            return_type = merge_types(merged.return_annotation, sig.return_annotation)
            merged = Signature(parameters, return_annotation=return_type)

    return merged


def wf_func(func_name, signatures, merged_signature=None, doc=None, namespace=""):
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
    merged_signature: inspect.Signature
        The merged signature used for displaying in docs and for
        typechecking on the backend. If None, the signature is
        automatically merged. Useful for functions with
        signatures that cannot be automatically merged.
    doc: str, optional
        The function's docstring.
    namespace: str, optional
        The namespace of the function on the backend.
    """
    if not isinstance(signatures, list):
        signatures = [signatures]

    def func(*args, **kwargs):
        failed = []
        signature = None
        for sig in signatures:
            try:
                promoted_args, promoted_kwargs = promote_to_signature(
                    sig, *args, **kwargs
                )
            except ProxyTypeError as e:
                failed.append(str(e))
                pass
            except BindError:
                pass
            else:
                signature = sig
                break

        if signature is None:
            raise format_dispatch_error(func_name, signatures, failed, *args, **kwargs)

        return_type = signature.return_annotation
        if return_type is Parameter.empty:
            raise TypeError(
                "No return type specified in signature: {}".format(signature)
            )

        if namespace != "":
            name = namespace + "." + func_name
        else:
            name = func_name

        return return_type._from_apply(name, *promoted_args, **promoted_kwargs)

    func.__name__ = func_name
    if doc is not None:
        func.__doc__ = doc

    # use 1 merged signature---"for display only"
    if merged_signature is None:
        try:
            merged_signature = merge_signatures(signatures)
        except AssertionError:
            raise ValueError("Signatures for {} could not be merged.".format(func_name))

    func.merged_signature = merged_signature
    # set a stringified version as `__signature__` so documentation tools (`help`, ipython, jupyter) can show it
    # in a more readable form, without fully-qualified names.
    func.__signature__ = stringify_signature(merged_signature)

    # and set `__annotations__` to the un-stringified types for type-aware tools (IDEs, jupyter, mypy) to use.
    if "sphinx" not in sys.modules:
        # BUT sphinx undoes all our stringification work, resulting in hard-to-read documentation.
        # and sphinx fights dirty. so we fight dirty back.
        # https://github.com/sphinx-doc/sphinx/blob/af62fa61e6cbd88d0798963211e73e5ba0d55e6d/sphinx/util/inspect.py#L356-L360
        func.__annotations__ = {
            name: param.annotation
            for name, param in merged_signature.parameters.items()
        }
        if merged_signature.return_annotation is not Signature.empty:
            func.__annotations__["return"] = merged_signature.return_annotation

    return func
