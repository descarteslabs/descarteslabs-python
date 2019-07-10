import six

try:
    # only after py3.4
    from collections import abc
except ImportError:
    import collections as abc

# Typespec:
#
# typespec       :: type
#                :: composite-type
#
# type           :: str
# composite-type :: {
#                       "type": type,
#                       "params": [type-param, ...]
#                   }
# type-param    :: typespec
#               :: [string-key, typespec]  # for encoding a dict type param

# >>> serialize_typespec(Int)
# "Int"
#
# >>> serialize_typespec(List[Int])
# {"type": "List", "params": ["Int"]}
#
# >>> serialize_typespec(Tuple[Str, List[Int]])
# {"type": "Tuple", "params": ["Str", {"type": "List", "params": ["Int"]}]}
#
# >>> serialize_typespec(Function[{'x': Float, 'y': Bool}, List[Float]])
# {
#     "type": "Function",
#     "params": [[("x", "Float"), ("y", "Bool")], {"type": "List", "params": ["Float"]}],
# }

types = {}


def deserialize_typespec(spec):
    if isinstance(spec, six.string_types):
        try:
            return types[spec]
        except KeyError:
            raise ValueError("No known type {!r}".format(spec))
    else:
        try:
            generic = spec["type"]
        except KeyError:
            raise ValueError("Typespec missing the key 'type': {}".format(spec))
        except TypeError:
            raise TypeError("Expected a mapping, got {}".format(spec))

        generic = deserialize_typespec(generic)

        try:
            params = spec["params"]
        except KeyError:
            raise ValueError("Typespec missing the key 'params': {}".format(spec))

        if not isinstance(params, abc.Sequence) or isinstance(params, six.string_types):
            raise TypeError(
                "Expected sequence for typespec params, got {}".format(params)
            )

        params = tuple(
            {key: deserialize_typespec(p) for (key, p) in param}
            if isinstance(param, (list, tuple))
            else deserialize_typespec(param)
            for param in params
        )

        return generic.__class_getitem__(params)


def serialize_typespec(cls):
    assert isinstance(
        cls, type
    ), "Can only serialize typespecs of classes, not instances"

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
        return name
    else:
        type_params = cls._type_params
        if type_params is None:
            raise TypeError(
                "Can only serialize concrete types, not the generic type {}".format(cls)
            )

        serialized_params = [
            [(key, serialize_typespec(param)) for key, param in six.iteritems(param)]
            if isinstance(param, dict)
            else serialize_typespec(param)
            for param in type_params
        ]

        return {"type": name, "params": serialized_params}


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
