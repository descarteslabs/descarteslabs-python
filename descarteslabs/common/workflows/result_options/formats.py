from descarteslabs.common.proto.formats import formats_pb2

from .helpers import (
    user_options_to_proto,
    user_facing_options,
    proto_field_type_to_python_type,
)


DEFAULTS = {
    formats_pb2.Geotiff: {
        "compression": "GEOTIFFCOMPRESSION_LZW",
        "overview_resampler": "GEOTIFFOVERVIEWRESAMPLER_NEAREST",
    },
    formats_pb2.Pyarrow: {"compression": "PYARROWCOMPRESSION_LZ4"},
}


mimetype_to_type = {
    field_descriptor.GetOptions().Extensions[formats_pb2.mimetype]: getattr(
        formats_pb2, field_descriptor.message_type.name
    )
    for field_name, field_descriptor in formats_pb2.Format.DESCRIPTOR.fields_by_name.items()
    if not field_name.startswith("has_")
}


format_name_to_type = {
    field_name: getattr(formats_pb2, field_descriptor.message_type.name)
    for field_name, field_descriptor in formats_pb2.Format.DESCRIPTOR.fields_by_name.items()
    if not field_name.startswith("has_")
}


format_message_type_to_field_names_and_types = {
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
        specific_type = mimetype_to_type[type_str]
    except KeyError:
        raise ValueError(
            "Unknown MIME type {}. Must be one of: {}".format(
                type_str, list(mimetype_to_type)
            )
        )

    parsed_params = {}
    for p in params:
        try:
            key, value = p.strip().split("=")
            parsed_params[key] = value
        except ValueError:
            raise ValueError(
                "Invalid MIME type {}. If the final character in your MIME type is a"
                " semicolon, remove it.".format(mimetype)
            )

    return user_options_to_proto(
        parsed_params,
        formats_pb2.Format,
        specific_type,
        format_message_type_to_field_names_and_types,
        DEFAULTS,
    )


def user_format_to_proto(format_: dict) -> formats_pb2.Format:
    # Classic dictionary mutation
    format_copy = format_.copy()
    try:
        format_name = format_copy.pop("type")
    except KeyError:
        raise ValueError(
            "The format dictionary must include a serialization type "
            "(like `'type': 'json'`), but key 'type' does not exist."
        )

    try:
        specific_type = format_name_to_type[format_name]
    except KeyError:
        raise ValueError(
            "Unknown format {!r}. Must be one of: {}".format(
                format_name, list(format_name_to_type)
            )
        )

    return user_options_to_proto(
        format_copy,
        formats_pb2.Format,
        specific_type,
        format_message_type_to_field_names_and_types,
        DEFAULTS,
    )


def format_proto_to_user_facing_format(format_: formats_pb2.Format) -> dict:
    if format_.has_pyarrow:
        specific_format = format_.pyarrow
        type_ = "pyarrow"
    elif format_.has_json:
        specific_format = format_.json
        type_ = "json"
    elif format_.has_geojson:
        raise NotImplementedError
    elif format_.has_csv:
        raise NotImplementedError
    elif format_.has_png:
        raise NotImplementedError
    elif format_.has_geotiff:
        specific_format = format_.geotiff
        type_ = "geotiff"
    elif format_.has_msgpack:
        specific_format = format_.msgpack
        type_ = "msgpack"
    else:
        raise ValueError("Invalid Format protobuf: none of the has_ values are set.")

    return user_facing_options(
        type_, specific_format, format_message_type_to_field_names_and_types
    )
