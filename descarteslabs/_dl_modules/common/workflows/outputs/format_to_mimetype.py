from typing import Union

from .mimetype_to_proto import mimetype_to_field_name

field_name_to_mimetype = {
    field_name: mimetype for mimetype, field_name in mimetype_to_field_name.items()
}


def user_format_to_mimetype(params: Union[dict, str]) -> str:
    """
    Convert a user format dict/string to its mimetype representation

    Like `user_format_to_proto`, but with no defaults and minimal validation.
    Since that validation will happen on the backend anyway, when converting the mimetype
    into a proto, no need to duplicate it.
    """
    if isinstance(params, str):
        type_name = params
        params = {}
    else:
        try:
            type_name = params["type"]
        except KeyError:
            raise ValueError(
                "The format dictionary must include a serialization type "
                "(like `'type': 'json'`), but key 'type' does not exist."
            ) from None

    try:
        mimetype = field_name_to_mimetype[type_name.lower()]
    except KeyError:
        raise ValueError(
            "Output format for inspect must be one of {}, but got {!r}.".format(
                ", ".join(field_name_to_mimetype), type_name
            )
        ) from None

    mimetype_parts = [mimetype]
    for name, val in params.items():
        if name == "type":
            continue

        if not isinstance(name, str):
            raise TypeError(
                "Format options keys must be strings, "
                "but the key {!r} is a(n) {}".format(name, type(name))
            )
        if not isinstance(val, (str, int, float, bool)):
            raise TypeError(
                "Format options values must be strings, integers, floats, or booleans "
                "but the value of {!r} is a(n) {}".format(name, type(val))
            )
        mimetype_parts.append(name + "=" + str(val))

    return "; ".join(mimetype_parts)
