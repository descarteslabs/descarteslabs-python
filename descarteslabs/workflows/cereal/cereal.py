import six

from descarteslabs.common.proto.typespec.typespec_pb2 import (
    Typespec,
    Primitive,
    MapFieldEntry,
    Map,
    CompositeType,
)
from descarteslabs.workflows.result_types import unmarshal


PRIMITIVES = (int, float, bool, str)


types = {}


def deserialize_typespec(spec):
    if spec.has_prim:
        if spec.prim.has_int:
            return spec.prim.int_
        if spec.prim.has_float:
            return spec.prim.float_
        if spec.prim.has_bool:
            return spec.prim.bool_
        else:
            return spec.prim.string_
    elif spec.has_type:
        try:
            return types[spec.type]
        except KeyError:
            raise ValueError("No known type {!r}".format(spec.type))
    elif spec.has_map:
        return {
            deserialize_typespec(param.key): deserialize_typespec(param.val)
            for param in spec.map.items
        }
    elif spec.has_comp:
        generic = deserialize_typespec(Typespec(type=spec.comp.type, has_type=True))
        params = tuple(deserialize_typespec(param) for param in spec.comp.params)
        return generic[params]
    else:
        raise ValueError(
            "Invalid typespec in ``deserialize_typespec``: none of the has_ values are set."
        )


def serialize_typespec(cls):
    if isinstance(cls, int):
        return Typespec(prim=Primitive(int_=cls, has_int=True), has_prim=True)
    if isinstance(cls, float):
        return Typespec(prim=Primitive(float_=cls, has_float=True), has_prim=True)
    if isinstance(cls, bool):
        return Typespec(prim=Primitive(bool_=cls, has_bool=True), has_prim=True)
    if isinstance(cls, str):
        return Typespec(prim=Primitive(string_=cls, has_string=True), has_prim=True)
    if isinstance(cls, dict):
        return Typespec(
            map=Map(
                items=[
                    MapFieldEntry(
                        key=serialize_typespec(key), val=serialize_typespec(val)
                    )
                    for key, val in six.iteritems(cls)
                ]
            ),
            has_map=True,
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
        return Typespec(type=name, has_type=True)
    else:
        type_params = cls._type_params
        if type_params is None:
            raise TypeError(
                "Can only serialize concrete types, not the generic type {}".format(cls)
            )

        serialized_params = [serialize_typespec(param) for param in type_params]

        return Typespec(
            comp=CompositeType(type=name, params=serialized_params), has_comp=True
        )


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


def typespec_to_unmarshal_str(typespec):
    if typespec.has_type:
        marshal_type = typespec.type
    elif typespec.has_comp:
        marshal_type = typespec.comp.type
    else:
        raise ValueError("Invalid typespec: has_type or has_comp must be set.")

    if marshal_type not in unmarshal.registry:
        raise TypeError(
            "{!r} is not a computable type. Note that if this is a function-like type, "
            "you should call it and compute the result, "
            "not the function itself.".format(marshal_type)
        )
    return marshal_type
