import operator

from .which_has import which_has
from .enum_prefix import without_prefix


def has_proto_to_user_dict(msg) -> dict:
    fieldname = which_has(msg)
    return {"type": fieldname, **proto_to_user_dict(getattr(msg, fieldname))}


def proto_to_user_dict(msg) -> dict:
    output = {}

    for fd in msg.DESCRIPTOR.fields:
        key = fd.name
        val = getattr(msg, key)

        val_transform = None
        its_a_map = False
        if key.startswith("not_"):
            key = key[4:]
            val_transform = operator.not_
        elif fd.type == fd.TYPE_ENUM:
            val = without_prefix(fd.enum_type.values_by_number[val].name).lower()
        elif fd.type == fd.TYPE_MESSAGE:
            if fd.message_type and getattr(
                fd.message_type.GetOptions(), "map_entry", False
            ):
                its_a_map = True
                if fd.message_type.fields_by_name["key"].type in (
                    fd.TYPE_MESSAGE,
                    fd.TYPE_ENUM,
                ):
                    raise ValueError(
                        "Cannot handle proto maps where keys are messages or enums."
                    )
                if fd.message_type.fields_by_name["value"].type == fd.TYPE_MESSAGE:
                    # values of the map are themselves messages
                    val_transform = proto_to_user_dict
            else:
                val_transform = proto_to_user_dict

        if its_a_map:
            val = {
                k: val_transform(v) if val_transform is not None else v
                for k, v in val.items()
            }
        elif val_transform is not None:
            val = (
                [val_transform(v) for v in val]
                if fd.label == fd.LABEL_REPEATED
                else val_transform(val)
            )

        output[key] = val

    return output
