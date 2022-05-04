from typing import Dict, Tuple

from ...common.graft import syntax as graft_syntax

from ..types import Proxytype, Function


def arguments_to_grafts(**arguments: Proxytype) -> Dict[str, dict]:
    """
    Convert a dict of Proxytype arguments into a dict of grafts or literals.

    If an argument's graft is a literal (like ``{"0": 1.234, "returns": "0"}``),
    the literal is JSON-encoded directly rather than wrapping it in a graft, to prevent
    graft bloat, and improve browser cache hits on query argument.

    Parameters
    ----------
    arguments: Proxytype
        Arguments to use while computing.

        Each argument must be the name of a parameter created with `~.identifier.parameter`.
        Each value must be a `Proxytype` (like `~.geospatial.Image` or `.Timedelta`).
        The values cannot themselves depend on any parameters.

    Returns
    -------
    grafts: dict[str, str]
        Dict of arguments, where keys are argument names, and values
        are their graft representations. Meant to be compatible with
        `merge_value_grafts` from the graft client.

    Raises
    ------
    ValueError:
        If an argument depends on parameters.
    """
    graftables = {}
    for name, arg in arguments.items():
        # Check arguments don't themselves depend on params
        if arg.params:
            param_names = tuple(p._name for p in arg.params)
            raise ValueError(
                f"Arguments to a computation cannot depend on parameters, but the argument for {name!r} depends on "
                f"the parameters {param_names}.\n"
                f"Consider turning the object you're passing in for {name!r} into a Function, "
                f"by passing it into `wf.Function.from_object`. Then call that Function with the values you want set "
                f"for those parameters {param_names}. Then pass the result of *that* into here as `{name}=`.\n\n"
                f"Example:\n"
                f"# assume `{name}` is a variable holding whatever you just passed in as `{name}=`"
                f"{name}_function = wf.Function.from_object({name})\n"
                f"{name}_value = {name}_function({', '.join(f'<value for {n!r}>' for n in param_names)})\n"
                f"wf.compute(..., {name}={name}_value)  # same form for .visualize or .inspect"
            )

        graft = arg.graft

        # Turn value grafts of literals back into their literals, for concision and URL stability.
        # (Basically passing in `Int(1)` is the same as passing in `1`.)
        return_expr = graft[graft["returns"]]
        if graft_syntax.is_literal(return_expr) or graft_syntax.is_quoted_json(
            return_expr
        ):
            graft = return_expr

        graftables[name] = graft

    return graftables


def promote_arguments(obj: Proxytype, arguments: dict) -> Tuple[Proxytype, dict]:
    """
    Check and promote a dict of arguments for a proxy object.

    Converts ``obj`` to a `Function` if it depends on parameters,
    then promotes ``arguments`` to the Function's arguments.

    Returns the possibly-Functionized proxy object, and the promoted arguments in a dict
    (ordered to match with the Function's arguments).
    """
    if len(obj.params) > 0:
        obj = Function.from_object(obj)

    if isinstance(obj, Function):
        if len(obj.arg_types) > 0:
            raise TypeError(
                f"{type(obj).__name__}: cannot use Functions with positional-only arguments "
                "for computation; all arguments must be named."
            )
        _, promoted_kwargs = obj._promote_arguments(**arguments)
        return obj, promoted_kwargs
    else:
        if arguments:
            raise TypeError(
                f"Expected no arguments, since the object does not depend on parameters "
                f"and isn't a Function, but got arguments {tuple(arguments)!r}."
            )
        return obj, arguments
