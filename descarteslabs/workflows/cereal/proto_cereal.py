import six

from .typespec_pb2 import Typespec, Primitive, MapFieldEntry, Map, CompositeType
from .cereal import types


PRIMITIVES = (int, float, bool, str)


def deserialize_typespec(spec):
    set_field = spec.WhichOneof("spec")
    if set_field == "prim":
        return getattr(spec.prim, spec.prim.WhichOneof("value"))
    elif set_field == "type":
        try:
            return types[spec.type]
        except KeyError:
            raise ValueError("No known type {!r}".format(spec.type))
    elif set_field == "map":
        return {
            deserialize_typespec(param.key): deserialize_typespec(param.val)
            for param in spec.map.items
        }
    elif set_field == "comp":
        generic = deserialize_typespec(Typespec(type=spec.comp.type))
        params = tuple(deserialize_typespec(param) for param in spec.comp.params)
        return generic.__class_getitem__(params)


def serialize_typespec(cls):
    if isinstance(cls, int):
        return Typespec(prim=Primitive(int_=cls))
    if isinstance(cls, float):
        return Typespec(prim=Primitive(float_=cls))
    if isinstance(cls, bool):
        return Typespec(prim=Primitive(bool_=cls))
    if isinstance(cls, str):
        return Typespec(prim=Primitive(string_=cls))

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

        serialized_params = [
            Typespec(
                map=Map(
                    items=[
                        MapFieldEntry(
                            key=serialize_typespec(key), val=serialize_typespec(val)
                        )
                        for key, val in six.iteritems(param)
                    ]
                )
            )
            if isinstance(param, dict)
            else serialize_typespec(param)
            for param in type_params
        ]

        return Typespec(comp=CompositeType(type=name, params=serialized_params))
