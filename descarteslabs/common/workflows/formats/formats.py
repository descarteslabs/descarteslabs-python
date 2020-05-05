from google.protobuf.descriptor import FieldDescriptor

from descarteslabs.common.proto.formats import formats_pb2


proto_field_type_to_python_type = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT64: int,
    FieldDescriptor.TYPE_UINT64: int,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_BOOL: bool,
    FieldDescriptor.TYPE_STRING: str,
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_ENUM: str,
}
# ^ A mapping from proto TYPE_* value to python type (only includes the ones we need)
# https://github.com/protocolbuffers/protobuf/blob/master/python/google/protobuf/descriptor.py#L461-L479


mimetype_to_field_name_and_type = {
    field_descriptor.GetOptions().Extensions[formats_pb2.mimetype]: (
        field_name,
        getattr(formats_pb2, field_descriptor.message_type.name),
    )
    for field_name, field_descriptor in formats_pb2.Format.DESCRIPTOR.fields_by_name.items()
    if not field_name.startswith("has_")
}


message_type_to_field_names_and_types = {
    getattr(formats_pb2, message_name): {
        field_name: proto_field_type_to_python_type[field_descriptor.type]
        for field_name, field_descriptor in message_descriptor.fields_by_name.items()
    }
    for message_name, message_descriptor in formats_pb2.DESCRIPTOR.message_types_by_name.items()
    if not message_name == "Format"
}


def mimetype_to_proto(mimetype: str) -> formats_pb2.Format:
    type_str, *params = mimetype.split(";")
    try:
        field_name, proto_type = mimetype_to_field_name_and_type[type_str]
    except KeyError:
        raise ValueError(
            "Unknown MIME type {}. Must be one of: {}".format(
                type_str, list(mimetype_to_field_name_and_type)
            )
        )

    fields_to_types = message_type_to_field_names_and_types[proto_type]

    proto_params = {}
    for p in params:
        try:
            key, value = p.strip().split("=")
        except ValueError:
            raise ValueError(
                "Invalid MIME type {}. If the final character in your MIME type is a semicolon, remove it.".format(
                    mimetype
                )
            )

        not_key = False
        try:
            cast_func = fields_to_types[key]
        except KeyError:
            try:
                cast_func = fields_to_types["not_" + key]
                not_key = True
            except KeyError:
                raise ValueError(
                    "Unsupported parameter {} for format {}. For supported parameters, "
                    "see the {} protobuf message at "
                    "https://github.com/descarteslabs/descarteslabs-python/blob/master/descarteslabs/common/proto/formats/formats.proto".format(  # noqa
                        key, type_str, proto_type.__name__
                    )
                )

        try:
            if cast_func is bool:
                value = value.lower()
                assert value in ("true", "false", "1", "0")
                cast_value = value == "true" or value == "1"
            else:
                cast_value = cast_func(value)
        except (AssertionError, ValueError):
            raise ValueError(
                "Parameter {} must be castable to {}, but it was not.".format(
                    key, cast_func.__name__
                )
            )

        if not_key:
            proto_params["not_" + key] = not cast_value
        else:
            proto_params[key] = cast_value

    specific_format = proto_type(**proto_params)

    return formats_pb2.Format(
        **{field_name: specific_format, "has_" + field_name: True}
    )
