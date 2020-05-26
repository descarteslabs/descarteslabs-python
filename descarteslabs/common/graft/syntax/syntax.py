import six
import numbers


try:
    # only after py3.4
    from collections import abc
except ImportError:
    import collections as abc


PRIMITIVE_TYPES = six.string_types + (numbers.Number, bool, type(None))
RESERVED_WORDS = ("parameters", "returns")


def is_nonstring_sequence(x, other_types=()):
    return isinstance(x, (abc.Sequence,) + other_types) and not isinstance(
        x, six.string_types
    )
    # abc.ByteString might be preferable, but doesn't exist in py2


def is_key(expr):
    return isinstance(expr, six.string_types) and expr not in RESERVED_WORDS


def is_guid_key(expr):
    try:
        return is_key(expr) and int(expr) > 0
    except ValueError:
        return False


def is_literal(expr):
    return isinstance(expr, PRIMITIVE_TYPES)


def is_quoted_json(expr):
    return (
        is_nonstring_sequence(expr)
        and len(expr) == 1
        and is_nonstring_sequence(expr[0], (abc.Mapping,))
    )


def is_application(expr):
    return (
        is_nonstring_sequence(expr)
        and all(is_key(val) for val in expr[:-1])
        and (
            is_key(expr[-1]) or (len(expr) > 1 and is_named_application_part(expr[-1]))
        )
    )


def is_named_application_part(part):
    return isinstance(part, abc.Mapping) and all(
        is_key(key) and is_key(val) for key, val in six.iteritems(part)
    )


def is_params(expr):
    return is_nonstring_sequence(expr) and all(is_key(name) for name in expr)


def is_graft(expr):
    "Whether expr is a graft-like: it's a mapping and contains the keys 'returns'"
    return isinstance(expr, abc.Mapping) and "returns" in expr


def check_args(
    n_positional_args, named_args_set, param_names, exception_type=TypeError
):
    """
    Validate that the correct positional and/or named arguments are given for a function application.

    Raises an exception of ``exception_type`` on validation failure, otherwise returns None.

    Parameters
    ----------
    n_positional_args: int
        The number of positional arguments given
    named_args_set: Set[str]
        The set of named arguments given
    param_names: Sequence[str]
        The required parameter names for the function
    exception_type: Exception
        Type of exception to raise when validation failes
    """
    if n_positional_args > len(param_names):
        raise exception_type(
            "Too many positional arguments: "
            "expected {}, got {}".format(len(param_names), n_positional_args)
        )

    missing_positional = param_names[n_positional_args:]
    if len(named_args_set) == 0:
        # fastpath
        if len(missing_positional) > 0:
            raise exception_type(
                "Missing required argument{} {}".format(
                    "s" if len(missing_positional) > 1 else "",
                    ", ".join(six.moves.map(repr, missing_positional)),
                )
            )
    else:
        unexpected_names = named_args_set - set(param_names)
        if len(unexpected_names) > 0:
            raise exception_type(
                "Unexpected named argument{} {}".format(
                    "s" if len(unexpected_names) > 1 else "",
                    ", ".join(six.moves.map(repr, sorted(unexpected_names))),
                )
            )

        still_missing_args = set(missing_positional) - named_args_set
        if len(still_missing_args) > 0:
            raise exception_type(
                "Missing required argument{} {}".format(
                    "s" if len(still_missing_args) > 1 else "",
                    ", ".join(six.moves.map(repr, still_missing_args)),
                )
            )
