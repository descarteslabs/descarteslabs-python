from ...proto.formats import formats_pb2
from .user_format_options import user_format_to_proto


mimetype_to_field_name = {
    field_descriptor.GetOptions().Extensions[formats_pb2.mimetype]: field_name
    for field_name, field_descriptor in formats_pb2.Format.DESCRIPTOR.fields_by_name.items()
    if not field_name.startswith("has_")
}


def mimetype_to_proto(mimetype: str) -> formats_pb2.Format:
    "Convert a mimetype representation of a Format into a Format proto message"
    type_str, *params = mimetype.split(";")
    try:
        type_name = mimetype_to_field_name[type_str]
    except KeyError:
        raise ValueError(
            "Unknown MIME type {}. Must be one of: {}".format(
                type_str, list(mimetype_to_field_name)
            )
        )

    parsed_params = {"type": type_name}
    for p in params:
        try:
            key, value = p.strip().split("=")
            parsed_params[key] = value
        except ValueError:
            raise ValueError(
                "Invalid MIME type {!r}. If the final character in your MIME type is a "
                "semicolon, remove it.".format(mimetype)
            )

    return user_format_to_proto(parsed_params)
