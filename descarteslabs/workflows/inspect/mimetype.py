from descarteslabs.common.proto.formats import formats_pb2


user_facing_format_to_mimetype = {
    field_name.lower(): field_descriptor.GetOptions().Extensions[formats_pb2.mimetype]
    for field_name, field_descriptor in formats_pb2.Format.DESCRIPTOR.fields_by_name.items()
    if not field_name.startswith("has_")
}


def format_to_mimetype(format, format_options=None):
    if format_options is None:
        format_options = {}

    try:
        mimetype = user_facing_format_to_mimetype[format]
    except KeyError:
        raise ValueError(
            "Output format for inspect must be one of {}, but got {}.".format(
                ", ".join(key for key in user_facing_format_to_mimetype.keys()), format
            )
        )

    for name, val in format_options.items():
        assert isinstance(
            name, str
        ), "Format options keys must be strings, but the key {} is a(n) {}".format(
            name, type(name).__name__
        )
        assert isinstance(
            val, (str, int, float, bool)
        ), "Format options values must be strings, integers, floats, or booleans but the value of {} is a(n) {}".format(
            name, type(val).__name__
        )
        mimetype += "; " + str(name) + "=" + str(val)

    return mimetype
