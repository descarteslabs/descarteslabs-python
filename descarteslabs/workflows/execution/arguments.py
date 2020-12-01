from descarteslabs.common.graft import client as graft_client, syntax as graft_syntax

from ..types import proxify, ProxyTypeError


def arguments_to_grafts(**arguments):
    """
    Convert a dict of arguments into a dict of grafts or literals.

    If a argument is a graft literal (i.e., a Python primitive),
    it's JSON-encoded directly rather than wrapping it in a graft, to prevent
    graft bloat, and improve browser cache hits on query argument.

    Otherwise, ``value_graft`` is called on it, so it should be a `Proxytype`
    or a JSON literal.

    If ``value_graft`` fails, `proxify` is called as a last resort to try to
    convert the value into something that graft can represent.

    Parameters
    ----------
    arguments: JSON-serializable value, Proxytype, `proxify` compatible value
        Arguments to use while computing.

        Each argument must be the name of a parameter created with `~.identifier.parameter`.
        Each value must be a JSON-serializable type (``bool``, ``int``, ``float``,
        ``str``, ``list``, ``dict``, etc.), a `Proxytype` (like `~.geospatial.Image` or `.Timedelta`),
        or a value that `proxify` can handle (like a ``datetime.datetime``).

    Returns
    -------
    grafts: dict[str, str]
        Dict of arguments, where keys are argument names, and values
        are their graft representations. Meant to be compatible with
        `merge_value_grafts` from the graft client.

    Raises
    ------
    TypeError:
        If a parameter value can't be represented as a graft by ``value_graft`` or `proxify`.
    """
    # NOTE(gabe): all the logic that handles turning non-Proxytypes into grafts/proxytypes
    # (`.value_graft`, `proxify`, etc) is only used in the case that `to_computable` gets a
    # `Function` object, where we don't know the parameter names, so the arguments are just being
    # blindly proxified.

    # Once `Function`s support named positional arguments, the only logic we'll need in here is the
    # last part, turning grafts of literals back into their literals, since `Function` will handle
    # all of the checking/promoting of arguments itself.
    grafts = {}
    for name, param in arguments.items():
        if graft_syntax.is_literal(param) or graft_syntax.is_graft(param):
            graftable = param
        else:
            try:
                graftable = graft_client.value_graft(param)
            except TypeError:
                try:
                    graftable = proxify(param).graft
                except NotImplementedError:
                    raise TypeError(
                        "Invalid type for parameter {!r}: {}. "
                        "Must be a JSON-serializable value, Proxytype, "
                        "or object that `proxify` can handle. "
                        "Got: {}".format(name, type(param), param)
                    )

            # Turn value grafts of literals back into their literals, for concision and URL stability.
            # (Basically passing in `Int._promote(1)` is the same as passing in `1`.)
            return_expr = graftable[graftable["returns"]]
            if graft_syntax.is_literal(return_expr) or graft_syntax.is_quoted_json(
                return_expr
            ):
                graftable = return_expr

        grafts[name] = graftable

    return grafts


# NOTE(gabe): when `Function` supports named positional arguments,
# this should essentially be moved to a method on `Function`
def promote_arguments(arguments: dict, params: tuple) -> dict:
    """
    Check and promote a dict of arguments against a tuple of parameter objects.

    Checks that ``arguments`` keys match the names of the parameters.
    Promotes ``arguments`` values to the Proxytypes of the corresponding parameter.
    Checks that ``arguments`` don't themselves depend on parameters.

    Returns the promoted arguments in a dict, ordered to match with `params`.
    """
    # Check names
    expected_param_names = set(arg._name for arg in params)
    if expected_param_names != arguments.keys():
        raise TypeError(
            f"Expected the required arguments {expected_param_names}, got {tuple(arguments)}"
        )

    # Typecheck the arguments
    promoted_args = {}
    for param in params:
        arg = arguments[param._name]

        try:
            promoted = param._promote(arg)
        except ProxyTypeError as e:
            raise TypeError(
                f"For argument {param._name!r}: expected {type(param).__name__} or an object "
                f"promotable to that, but got {type(arg).__name__}: {arg!r}."
                f"\n\n{e}"
            )

        # Check arguments don't themselves depend on params
        if promoted.params:
            name = param._name
            param_names = tuple(p._name for p in promoted.params)
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

        promoted_args[param._name] = promoted

    return promoted_args
