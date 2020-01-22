from ..primitives import Bool, Any


DEFAULT_UNSUPPORTED_MSG = "{prefix}: {type_name} does not support operator {op_name}"
DEFAULT_INVALID_MSG = (
    "{prefix}: operator {op_name} on {type_name} produces type {result_name}. "
    "Must produce one of: {valid_result_names}."
)


def check_valid_binop_for(
    binop,
    type_,
    error_prefix,
    valid_result_types=(Bool, Any),
    unsupported_msg=DEFAULT_UNSUPPORTED_MSG,
    invalid_msg=DEFAULT_INVALID_MSG,
):
    """
    Check that ``binop(inst, inst)`` produces one of ``valid_result_types``, where ``inst`` is an instance of ``type_``

    Parameters
    ----------
    binop: Callable[[type_, type_], T]
        Operator to try
    type_: Proxtype
        Type to test. An instance will be created with ``type_._from_apply("")``.
    error_prefix: str
        "{error_prefix}: " is prepended to the default error messages
    valid_result_types: Tuple[type, ...]
        Types for ``binop`` to return that are considered valid
    unsupported_msg: str
        Error message to raise if calling ``binop`` raises a TypeError.

        This string is formatted with these named arguments:

        * ``prefix``: whatever's passed as `error_prefix`
        * ``type_name``: ``type_.__name__``
        * ``op_name``: ``binop.__name__``
    invalid_msg: str
        Error message to raise if ``binop`` returns a type not in ``valid_result_types``.

        This string is formatted with these named arguments:

        * ``prefix``: whatever's passed as `error_prefix`
        * ``type_name``: ``type_.__name__``
        * ``op_name``: ``binop.__name__``
        * ``result_name``: ``type(result).__name__``, where ``result`` is what's returned by ``binop``
        * ``valid_result_names``: comma-separated string of the names of all the types in ``valid_result_types``

    Raises
    ------
    TypeError:
        If ``binop(inst, inst)`` raises a TypeError, or produces an invalid type
    """
    item = type_._from_apply("")
    try:
        result = binop(item, item)
    except TypeError:
        raise TypeError(
            unsupported_msg.format(
                prefix=error_prefix, type_name=type_.__name__, op_name=binop.__name__
            )
        )
    else:
        if not isinstance(result, valid_result_types):
            valid_result_names = ", ".join(t.__name__ for t in valid_result_types)
            raise TypeError(
                invalid_msg.format(
                    prefix=error_prefix,
                    type_name=type_.__name__,
                    op_name=binop.__name__,
                    result_name=type(result).__name__,
                    valid_result_names=valid_result_names,
                )
            )
