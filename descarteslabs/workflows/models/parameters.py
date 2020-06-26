import six

from descarteslabs.common.graft import client as graft_client, syntax as graft_syntax

from ..types import proxify


def parameters_to_grafts(**parameters):
    """
    Convert a dict of parameters into a dict of grafts or literals.

    If a parameter is a graft literal (i.e., a Python primitive),
    it's JSON-encoded directly rather than wrapping it in a graft, to prevent
    graft bloat, and improve browser cache hits on query argument.

    Otherwise, ``value_graft`` is called on it, so it should be a `Proxytype`
    or a JSON literal.

    If ``value_graft`` fails, `proxify` is called as a last resort to try to
    convert the value into something that graft can represent.

    Parameters
    ----------
    parameters: JSON-serializable value, Proxytype, `proxify` compatible value
        Parameters to use while computing.

        Each argument must be the name of a parameter created with `~.identifier.parameter`.
        Each value must be a JSON-serializable type (``bool``, ``int``, ``float``,
        ``str``, ``list``, ``dict``, etc.), a `Proxytype` (like `~.geospatial.Image` or `.Timedelta`),
        or a value that `proxify` can handle (like a ``datetime.datetime``).

    Returns
    -------
    grafts: dict[str, str]
        Dict of parameters, where keys are argument names, and values
        are their graft representations. Meant to be compatible with
        `merge_value_grafts` from the graft client.

    Raises
    ------
    TypeError:
        If a parameter value can't be represented as a graft by ``value_graft`` or `proxify`.
    """
    grafts = {}
    for name, param in six.iteritems(parameters):
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

        grafts[name] = graftable

    return grafts
