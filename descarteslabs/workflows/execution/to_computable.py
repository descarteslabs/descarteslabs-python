# prevents type annotations from being evaluated at runtime
# https://www.python.org/dev/peps/pep-0563/#enabling-the-future-behavior-in-python-3-7
from __future__ import annotations

import typing

from ..cereal import serialize_typespec, typespec_to_unmarshal_str
from ..types import proxify, Function, Proxytype

from .arguments import arguments_to_grafts, promote_arguments

if typing.TYPE_CHECKING:
    from descarteslabs.common.proto.typespec import typespec_pb2


def to_computable(
    proxy_object: typing.Union[Proxytype, list, tuple], arguments: dict
) -> (Proxytype, dict, typespec_pb2.Typespec, str):
    """
    Shared logic to convert the proxy object and arguments into the proper form for computation

    * Proxifies ``proxy_object``
    * Turns ``proxy_object`` into a Function if it has any parameters
    * Checks that ``arguments`` matches the names of the parameters
    * Typechecks/promotes the arguments
    * Packages the arguments into a dict of grafts/literals
    * Serialized the typespec of ``proxy_object``
    * Finds the return type name for ``proxy_object``

    Returns
    -------
    proxy_object: Proxytype
        ``proxy_object``, potentially converted to a `.Function`
    arguments: dict
        dict of argument grafts or literals
    typespec: typespec_pb2.Typespc
        Proto message for the serialized typespec of the returned ``proxy_object``
    result_type: str
        Name of the result type
    """
    proxy_object = proxify(proxy_object)

    # Turn objects that depend on parameters into Functions,
    # and validate + promote any arguments.

    if len(proxy_object.params) == 0:
        if len(arguments) > 0 and not isinstance(proxy_object, Function):
            raise TypeError(
                f"Expected no arguments, since the object does not depend on parameters "
                f"and isn't a Function, but got arguments {tuple(arguments)!r}."
            )
        # TODO if you pass in a `Function` as the `proxy_object`, the arguments won't be validated or promoted!
        # Can resolve that once Function supports named positional arguments.
        promoted_args = arguments
    else:
        # TODO we'd like to just be able to do:
        # ````
        # proxy_object = Function.from_object(proxy_object)
        # proxy_object(**arguments)
        # ````
        # and have it validate the names and types of the arguments. but Functions don't support
        # named positional arguments yet.

        promoted_args = promote_arguments(arguments, proxy_object.params)
        proxy_object = Function.from_object(proxy_object)

    arguments = arguments_to_grafts(**promoted_args)

    # get result type based on the pre-functionized proxy object.
    typespec = serialize_typespec(type(proxy_object))
    return_typespec = (
        serialize_typespec(proxy_object.return_type)
        if isinstance(proxy_object, Function)
        else typespec
    )
    result_type = typespec_to_unmarshal_str(return_typespec)
    # ^ this preemptively checks whether the result type is something we'll know how to unmarshal

    return proxy_object, arguments, typespec, result_type
