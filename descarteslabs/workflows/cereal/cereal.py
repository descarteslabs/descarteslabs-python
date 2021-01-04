from typing import Union, Dict, TYPE_CHECKING

from descarteslabs.common.proto.typespec.typespec_pb2 import (
    Typespec,
    Primitive,
    MapFieldEntry,
    Map,
    CompositeType,
)
from descarteslabs.workflows.result_types import unmarshal

if TYPE_CHECKING:
    from descarteslabs.workflows.types import Proxytype  # noqa: F401


PRIMITIVES = (int, float, bool, str)
PrimitiveType = Union[PRIMITIVES]


types: Dict[str, "Proxytype"] = {}


def deserialize_typespec(
    spec: Typespec,
) -> Union[
    PrimitiveType,
    Dict[Union[PrimitiveType, "Proxytype"], Union[PrimitiveType, "Proxytype"]],
    "Proxytype",
]:
    component = spec.WhichOneof("component")
    if component == "primitive":
        primitive_field = spec.primitive.WhichOneof("value")
        assert primitive_field is not None, "Primitive message must have a value set"
        return getattr(spec.primitive, primitive_field)
    elif component == "type":
        try:
            return types[spec.type]
        except KeyError:
            raise ValueError("No known type {!r}".format(spec.type))
    elif component == "map":
        return {
            deserialize_typespec(param.key): deserialize_typespec(param.val)
            for param in spec.map.items
        }
    elif component == "composite":
        generic = deserialize_typespec(Typespec(type=spec.composite.type))
        params = tuple(deserialize_typespec(param) for param in spec.composite.params)
        return generic[params]
    else:
        raise ValueError(
            "Invalid typespec in ``deserialize_typespec``: none of the `component` fields are set."
        )


def serialize_typespec(
    cls: Union[
        PrimitiveType,
        Dict[Union[PrimitiveType, "Proxytype"], Union[PrimitiveType, "Proxytype"]],
        "Proxytype",
    ]
) -> Typespec:
    if isinstance(cls, int):
        return Typespec(primitive=Primitive(int_=cls))
    if isinstance(cls, float):
        return Typespec(primitive=Primitive(float_=cls))
    if isinstance(cls, bool):
        return Typespec(primitive=Primitive(bool_=cls))
    if isinstance(cls, str):
        return Typespec(primitive=Primitive(string_=cls))
    if isinstance(cls, dict):
        return Typespec(
            map=Map(
                items=[
                    MapFieldEntry(
                        key=serialize_typespec(key), val=serialize_typespec(val)
                    )
                    for key, val in cls.items()
                ]
            ),
        )

    if getattr(cls, "_named_concrete_type", False):
        # ^ cls doesn't have that attribute, or it does and the value is falsey
        name = cls.__name__
    else:
        try:
            name = cls._generictype.__name__
        except AttributeError:
            name = cls.__name__

    try:
        expected_cls = types[name]
    except KeyError:
        raise ValueError(
            "{!r} is not in the types registry; cannot serialize it".format(name)
        )

    if not issubclass(cls, expected_cls):
        raise ValueError(
            "{} is not a subclass of {}, even though it has the same `__name__`".format(
                cls, expected_cls
            )
        )

    if not hasattr(cls, "_type_params") or getattr(cls, "_named_concrete_type", False):
        return Typespec(type=name)
    else:
        type_params = cls._type_params
        if type_params is None:
            raise TypeError(
                "Can only serialize concrete types, not the generic type {}".format(cls)
            )

        serialized_params = [serialize_typespec(param) for param in type_params]

        return Typespec(composite=CompositeType(type=name, params=serialized_params))


def serializable(is_named_concrete_type=False):
    if isinstance(is_named_concrete_type, type):
        raise TypeError(
            "On {}, the @serializable decorator must be called: "
            "``@serializable()``".format(is_named_concrete_type.__name__)
        )

    def inner(cls):
        type_name = cls.__name__
        if type_name in types:
            raise TypeError(
                "While registering {}: there was already a type registered for the name {}: {}".format(
                    cls, type_name, types[type_name]
                )
            )
        types[type_name] = cls
        if is_named_concrete_type:
            cls._named_concrete_type = True
        return cls

    return inner


def typespec_to_unmarshal_str(typespec: Typespec) -> str:
    component = typespec.WhichOneof("component")
    if component == "type":
        marshal_type = typespec.type
    elif component == "composite":
        marshal_type = typespec.composite.type
    else:
        raise ValueError(
            "Invalid typespec: the `type` or `composite` field must be set in `component`."
        )

    if marshal_type not in unmarshal.registry:
        raise TypeError(
            "{!r} is not a computable type. Note that if this is a function-like type, "
            "you should call it and compute the result, "
            "not the function itself.".format(marshal_type)
        )
    return marshal_type
