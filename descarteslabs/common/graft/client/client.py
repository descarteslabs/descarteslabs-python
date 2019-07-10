import six
import itertools

from .. import syntax

try:
    # only after py3.4
    from collections import abc
except ImportError:
    import collections as abc


NO_INITIAL = "_no_initial_"
PARAM = "__param__"
GUID_COUNTER = 0


def guid():
    global GUID_COUNTER
    GUID_COUNTER += 1
    return str(GUID_COUNTER)


def is_delayed(x):
    "Whether x is a delayed-like: ``x.graft`` is a graft-like mapping"
    try:
        return syntax.is_graft(x.graft)
    except AttributeError:
        return False


def value_graft(value, key=None):
    """
    The graft, as a dict, for a value.

    Parameters
    ----------
    value: delayed-like object or JSON-serializable value
        If a JSON-serializable value, returns the graft representing that value
        (a function with no parameters that returns the value).

        If a delayed-like object, returns ``value.graft``.

    Returns
    -------
    graft: dict
    """
    if is_delayed(value):
        return value.graft
    if key is None:
        key = guid()
    if isinstance(value, syntax.PRIMITIVE_TYPES):
        return {key: value, "returns": key}
    elif isinstance(value, (abc.Sequence, abc.Mapping)):
        # Quoted JSON
        return {key: [value], "returns": key}
    else:
        raise TypeError(
            "Value must be a delayed-like object, primitve (one of {}), or JSON-serializable"
            "sequence or mapping, not {}".format(syntax.PRIMITIVE_TYPES, type(value))
        )


def keyref_graft(key):
    """
    Graft for a referring to an arbitrary key.

    Useful for referring to parameters of functions, or builtins.

    Parameters
    ----------
    key: str
    """
    return {"returns": key}


def is_keyref_graft(value):
    return syntax.is_graft(value) and len(value) == 1 and next(iter(value)) == "returns"


def apply_graft(function, *args, **kwargs):
    """
     The graft for calling a function with the given positional and keyword arguments.

    Arguments can be given as Python values, in which case `value_graft`
    will be called on them first, or as delayed-like objects or graft-like mappings.

    Parameters
    ----------
    function: str, graft-like mapping, or delayed-like object
        The function to apply
    **args: delayed-like object, graft-like mapping, or JSON-serializable value
        Positional arguments to apply function to
    **kwargs: delayed-like object, graft-like mapping, or JSON-serializable value
        Named arguments to apply function to

    Returns
    -------
    result_graft: dict
        Graft representing ``function`` applied to ``args`` and ``kwargs``
    """
    pos_args_grafts = [
        arg if syntax.is_graft(arg) else value_graft(arg) for arg in args
    ]
    named_arg_grafts = {
        name: (arg if syntax.is_graft(arg) else value_graft(arg))
        for name, arg in six.iteritems(kwargs)
    }

    if is_delayed(function):
        function = function.graft

    result_graft = {}
    function_key = None
    if isinstance(function, str):
        function_key = function
    elif syntax.is_graft(function):
        if "parameters" in function:
            # function considered an actual function object, insert it as a subgraft
            param_names = function.get("parameters", [])
            syntax.check_args(len(args), six.viewkeys(kwargs), param_names)

            function_key = guid()
            result_graft[function_key] = function
        else:
            # function considered the value it returns; inline its graft.
            # this is the case with higher-order functions,
            # where `function` is an apply expression that returns another function.
            # we don't check args because that would require interpreting the graft.
            result_graft.update(function)
            function_key = function["returns"]
    else:
        raise TypeError(
            "Expected a graft dict, a delayed-like object, or a string as the function; "
            "got {}".format(function)
        )

    positional_args = []
    named_args = {}
    for name, arg_graft in itertools.chain(
        zip(itertools.repeat(None), pos_args_grafts), six.iteritems(named_arg_grafts)
    ):
        if "parameters" in arg_graft:
            # argument considered an actual function object, insert it as a subgraft
            arg_key = guid()
            result_graft[arg_key] = arg_graft
        else:
            # argument considered the value it returns; inline its graft
            result_graft.update(arg_graft)
            arg_key = arg_graft["returns"]

        if name is None:
            positional_args.append(arg_key)
        else:
            named_args[name] = arg_key

    expr = [function_key] + positional_args
    if len(named_args) > 0:
        expr.append(named_args)

    key = guid()
    result_graft[key] = expr
    result_graft["returns"] = key
    return result_graft


def function_graft(result, *parameters):
    """
    Graft for a function that returns ``result``.

    Parameters
    ----------
    result: graft-like mapping or delayed-like object
        The value returned by the function
    *parameters: str or keyref graft
        Names of the parameters to the function, or keyref grafts representing them.
        The graft of ``result`` should include dependencies to these names
        (using `keyref_graft`), but this is not validated. Forgetting to include
        a parameter name required somewhere within ``result`` could result
        in unexpected runtime behavior.

    Returns
    -------
    function_graft: dict
        Graft representing a function that returns ``result`` and takes ``parameters``.
    """
    parameters = [
        param["returns"] if is_keyref_graft(param) else param for param in parameters
    ]
    if not syntax.is_params(parameters):
        raise ValueError("Invalid parameters for a graft: {}".format(parameters))
    result_graft = result if syntax.is_graft(result) else value_graft(result)
    if "parameters" in result_graft:
        # Graft that returns a function object
        key = guid()
        return {"parameters": parameters, key: result_graft, "returns": key}
    else:
        # Graft that returns the value referred to by result
        return dict(result_graft, parameters=parameters)


def merge_value_grafts(**grafts):
    """
    Merge zero-argument grafts into one, with return values available under new names.

    Lets you take multiple grafts that return values (such as ``{'x': 1, 'returns': 'x'}``),
    and construct a graft in which those _returned_ values are available under the names
    specified as keyword arguments---as _values_, not as callables.

    Parameters
    ----------
    **grafts: delayed-like object, graft-like mapping, or JSON-serializable value
        Grafts that take no arguments: delayed-like objects with no dependencies on parameters,
        JSON-serializable values, or grafts without parameters.
        The value _returned_ by each graft will be available as the name given
        by its keyword argument.
        Except for JSON-serializable values, each graft will be kept as a sub-graft within its own scope,
        so overlapping keys between the grafts will not collide.
        Caution: this function accepts both grafts and JSON values, so be careful
        that you do not pass in a JSON value that looks like a graft, since it will not get quoted.
    """
    merged = {}
    for name, value in six.iteritems(grafts):
        if isinstance(value, syntax.PRIMITIVE_TYPES):
            # fastpath for simple case
            merged[name] = value
        else:
            subgraft = value if syntax.is_graft(value) else value_graft(value)
            parameters = subgraft.get("parameters", ())

            if len(parameters) > 0:
                raise ValueError(
                    "Value graft for {}: expected a graft that takes no parameters, "
                    "but this one takes {}".format(name, parameters)
                )
            returned = subgraft[subgraft["returns"]]
            if syntax.is_literal(returned) or syntax.is_quoted_json(returned):
                merged[name] = returned
            else:
                # insert actual subgraft under a different name
                subkey = "_{}".format(name)
                merged[subkey] = subgraft
                # actual name is the invocation of that subgraft, with no arguments
                merged[name] = [subkey, {}]
    return merged
