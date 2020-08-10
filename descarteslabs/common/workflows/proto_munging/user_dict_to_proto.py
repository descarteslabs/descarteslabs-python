from typing import Type, Dict

from google.protobuf.message import Message
from google.protobuf.descriptor import FieldDescriptor

from .which_has import has_options
from .enum_prefix import enum_prefix, without_prefix


def user_dict_to_has_proto(
    params: dict, msg_type: Type[Message], defaults: Dict[Type[Message], dict]
) -> Message:
    """
    Load a user-created dict into a has_-style wrapper protobuf message, applying these niceties:

    * ``not_``-prefixed fields on the proto can be given without the "not_" by users;
      user values are inverted when setting
    * TODO snake_case names from users are converted to camelCase
    * enum values from users are case-insensitive, have the `{msg_name}{enum_name}_` prefix added,
      and are uppercased before setting
    * "type" field is case-insensitive and is lowercased before using

    The ``params`` dict must contain a "type" field, and the value of that type
    must be a field on ``msg_type``.

    Parameters
    ----------
    params: dict
        User params, containing the "type" field
    msg_type: protobuf.message.Message
        Class for the wrapper "has_"-style message to use
    defaults: Dict[Type[Message], dict]
        Dict from specific message types (like formats_pb2.GeoTIFF) -> dict of defaults
        for that specific type. Defaults are given in user-facing format.
    """
    type_ = params["type"].lower()

    msg = msg_type()
    try:
        specific_msg = getattr(msg, type_)
    except AttributeError:
        raise ValueError(
            "Unknown {} {!r}. Must be one of: {}".format(
                msg_type.__name__, type_, has_options(msg)
            )
        )

    specific_defaults = defaults.get(type(specific_msg), {})
    if len(params) == 1:
        # nothing but "type"; just use the defaults
        params_with_defaults = specific_defaults
    else:
        params_with_defaults = dict(specific_defaults, **params)
        del params_with_defaults["type"]

    user_dict_to_proto(params_with_defaults, specific_msg)
    setattr(msg, "has_" + type_, True)
    return msg


def cast_bool(value):
    if isinstance(value, bool):
        return value
    else:
        value = value.lower()
        assert value in ("true", "false", "1", "0")
        cast_value = value == "true" or value == "1"
        return cast_value


cast_bool.__name__ = "bool"


proto_field_type_to_python_type = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT64: int,
    FieldDescriptor.TYPE_UINT64: int,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_BOOL: cast_bool,
    FieldDescriptor.TYPE_STRING: str,
    FieldDescriptor.TYPE_UINT32: int,
}
# ^ A mapping from proto TYPE_* value to python type (only includes the ones we need)
# https://github.com/protocolbuffers/protobuf/blob/master/python/google/protobuf/descriptor.py#L461-L479


class EnumValueError(ValueError):
    pass


def user_dict_to_proto(dct, msg):
    """
    Recursively load a user-created dict into a protobuf message, applying these niceties:

    * ``not_``-prefixed fields on the proto can be given without the "not_" by users;
      user values are inverted when setting
    * TODO snake_case names from users are converted to camelCase
    * enum values from users are case-insensitive, have the `{msg_name}{enum_name}_` prefix added,
      and are uppercased before setting
    """
    field_descriptors = msg.DESCRIPTOR.fields_by_name
    for k, v in dct.items():

        fd = None
        proto_k = k
        proto_v = v
        for transform in [None]:  # , camelcase]:  # TODO camelcase
            proto_k = transform(k) if transform is not None else k
            invert = False
            try:
                fd = field_descriptors[proto_k]
            except KeyError:
                try:
                    proto_k = "not_" + proto_k
                    fd = field_descriptors[proto_k]
                    invert = True
                except KeyError:
                    continue

            if invert:
                try:
                    proto_v = not cast_bool(v)
                except Exception:
                    raise ValueError(
                        "Parameter {!r} ({!r}) must be castable to bool, but it was not.".format(
                            k, v
                        )
                    )
            break
        else:
            raise ValueError("Unknown field {!r} for {}.".format(k, type(msg).__name__))
            # TODO list valid names

        msg_field = getattr(msg, proto_k)

        if fd.message_type and getattr(fd.message_type.GetOptions(), "map_entry", False):
            # absolutely disgusting: proto compiles maps into repeated key,value submessages:
            # https://github.com/protocolbuffers/protobuf/blob/master/src/google/protobuf/descriptor.proto#L499-L510
            if not isinstance(proto_v, dict):
                raise TypeError(
                    "Expected dict for field {!r}, not {!r}.".format(k, proto_v)
                )
            proto_v = [
                {"key": map_k, "value": map_v} for map_k, map_v in proto_v.items()
            ]
            entry_class = msg_field.GetEntryClass()
            for entry_v in proto_v:
                entry = entry_class()
                user_dict_to_proto(entry_v, entry)
                msg_field[entry.key] = entry.value
            continue

        if fd.label == fd.LABEL_REPEATED and not isinstance(proto_v, (list, tuple)):
            proto_v = [proto_v]

        if fd.type == fd.TYPE_MESSAGE:
            # recursively dictify sub-messages
            if fd.label == fd.LABEL_REPEATED:
                for sub_v in proto_v:
                    user_dict_to_proto(sub_v, msg_field.add())
            else:
                user_dict_to_proto(proto_v, msg_field)
        else:

            if fd.type == fd.TYPE_ENUM:

                def cast_func(value):
                    proto_v = str(value).upper()

                    prefix = enum_prefix(type(msg).__name__, proto_k)
                    if not proto_v.startswith(prefix):
                        proto_v = prefix + proto_v

                    enum_mapping = fd.enum_type.values_by_name
                    try:
                        return enum_mapping[proto_v].number
                    except KeyError:
                        raise EnumValueError(
                            "Invalid value for parameter {!r}: {!r}. Must be one of: {}".format(
                                k,
                                value,
                                [
                                    without_prefix(enum_k).lower()
                                    for enum_k in enum_mapping
                                    if not enum_k.endswith("UNSPECIFIED")
                                ],
                            )
                        ) from None

                cast_func.__name__ = "enum"

            else:
                cast_func = proto_field_type_to_python_type[fd.type]

            try:
                proto_v = (
                    [cast_func(v) for v in proto_v]
                    if fd.label == fd.LABEL_REPEATED
                    else cast_func(proto_v)
                )
            except EnumValueError as e:
                raise ValueError(e) from None
            except (AssertionError, ValueError, AttributeError):
                raise ValueError(
                    "Parameter {!r} ({!r}) must be castable to {}, but it was not.".format(
                        k, v, cast_func.__name__
                    )
                )

            if fd.label == fd.LABEL_REPEATED:
                # can't assign directly to repeated fields
                getattr(msg, proto_k)[:] = proto_v
            else:
                setattr(msg, proto_k, proto_v)

    return msg
