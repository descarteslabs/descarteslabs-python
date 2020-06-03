from google.protobuf.descriptor import FieldDescriptor


def cast_bool(value):
    if isinstance(value, bool):
        return value
    else:
        value = value.lower()
        assert value in ("true", "false", "1", "0")
        cast_value = value == "true" or value == "1"
        return cast_value


def cast_enum(value):
    return str(value).upper()


def camelize(string):
    return "".join([word.capitalize() for word in string.split("_")])


proto_field_type_to_python_type = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT64: int,
    FieldDescriptor.TYPE_UINT64: int,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_BOOL: cast_bool,
    FieldDescriptor.TYPE_STRING: str,
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_ENUM: cast_enum,
}
# ^ A mapping from proto TYPE_* value to python type (only includes the ones we need)
# https://github.com/protocolbuffers/protobuf/blob/master/python/google/protobuf/descriptor.py#L461-L479


def user_options_to_proto(
    user_options: dict,
    type_: type,  # Format or Destination
    specific_type: type,
    message_type_to_field_names_and_types: dict,
    defaults: dict,
):
    """
    Turns a dictionary of format/destination options into a protobuf message.

    Parameters
    ----------
    user_options: dict
        The format or destination options to protobufify
    type_: type
        The toplevel protobuf message of the result.
        One of 'formats_pb2.Format' or 'destinations_pb2.Destination'
    specific_type: type
        The protobuf message for the specific format/destination.
        Example: 'formats_pb2.GeoTIFF' or 'destinations_pb2.Email'
    message_type_to_field_names_and_types: dict
        A mapping of field names to their protobuf types (for ``specific_type``)
    defaults: dict
        A mapping of field name to their default values (for ``specific_type``)
    """
    field_name = specific_type.__name__.lower()
    fields_to_types = message_type_to_field_names_and_types[specific_type]

    try:
        proto_params = defaults[specific_type].copy()
    except KeyError:
        proto_params = {}

    for key, value in user_options.items():
        not_key = False
        try:
            cast_func = fields_to_types[key]
        except KeyError:
            try:
                cast_func = fields_to_types["not_" + key]
                not_key = True
            except KeyError:
                raise ValueError(
                    "Unsupported parameter '{}' for {} '{}'. For supported parameters, "
                    "see {} in the formats documentation at"
                    " https://docs.descarteslabs.com"
                    "/descarteslabs/workflows/docs/formats.html#output-formats "
                    "or destinations documentation at https://docs.descarteslabs.com"
                    "/descarteslabs/workflows/docs/destinations.html#output-destinations"
                    .format(
                        key, type_.__name__.lower(), field_name, specific_type.__name__
                    )
                ) from None

        field_key = "not_" + key if not_key else key
        is_repeated = (
            getattr(specific_type, field_key).DESCRIPTOR.label
            is FieldDescriptor.LABEL_REPEATED
        )

        try:
            if is_repeated:
                if not isinstance(value, (list, tuple)):
                    value = [value]
                cast_value = [cast_func(v) for v in value]
            else:
                cast_value = cast_func(value)
                # for enums, "NEAREST" becomes "GEOTIFFOVERVIEWRESAMPLER_NEAREST"
                if cast_func is cast_enum:
                    cast_value = (
                        field_name.upper()
                        + field_key.replace("_", "").upper()
                        + "_"
                        + cast_value
                    )
        except (AssertionError, ValueError, AttributeError):
            raise ValueError(
                "Parameter {!r} must be castable to {}, but it was not.".format(
                    key, cast_func.__name__
                )
            )

        if not_key:
            proto_params[field_key] = not cast_value
        else:
            proto_params[field_key] = cast_value

    specific_format = specific_type(**proto_params)
    return type_(**{field_name: specific_format, "has_" + field_name: True})


def user_facing_options(
    type_: str, specific_format, message_type_to_field_names_and_types: dict
):
    """
    Turns a protobuf message into user-facing format/destination options.

    Parameters
    ----------
    type_: str
        The type of the protobuf message
        Example: 'pyarrow' or 'download'
    specific_format: protobuf message
        The protobuf message to dictify
    message_type_to_field_names_and_types: dict
        A mapping of field names to their protobuf types (for type(``specific_format``))
    """
    fields_to_types = message_type_to_field_names_and_types[type(specific_format)]

    output_options = {}
    output_options["type"] = type_
    for key, field_type in fields_to_types.items():
        val = getattr(specific_format, key)

        if key.startswith("not_"):
            key = key.split("_")[1]
            val = not val

        if field_type is cast_enum and isinstance(val, int):
            enum = camelize(type_) + camelize(key)
            val = getattr(specific_format, enum).Name(val)
            val = val.split("_")[1].lower()

        output_options[key] = val

    return output_options
