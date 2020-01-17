from descarteslabs.common.graft import client as graft_client
from ..core.core import is_generic


def parameter(name, type_):
    """
    Create a typed parameter.

    Useful for describing computations that will be further
    parameterized at runtime.

    To actually provide values for the parameter at `.Job` compute time,
    pass them in as keyword arguments to `~.models.compute` or `~.geospatial.Image.visualize`.

    Parameters
    ----------
    name: str
        Name of the parameter
    type_: Proxytype
        Type of the parameter

    Returns
    -------
    proxyTime:
        a Proxytype object of type ``type_``.

    Example
    -------
    >>> from descarteslabs.workflows import Float, parameter
    >>> my_program = Float(0.42) * parameter("scale", Float)
    >>> my_program.compute(scale=Float(0.99))  # doctest: +SKIP
    """

    if name.isdigit():
        raise ValueError("Parameter name cannot be a digit")

    if is_generic(type_):
        raise ValueError(
            "Parameter type cannot be generic, must be concrete (like List[Int], not plain List)"
        )

    return identifier(name, type_)


def identifier(name, type_):
    """
    Create a Proxytype instance that references a graft key.
    Internal method meant for references to builtin constants or parameters.
    You shouldn't use this directly; consider `parameter` instead.
    """
    return type_._from_graft(graft_client.keyref_graft(name))
