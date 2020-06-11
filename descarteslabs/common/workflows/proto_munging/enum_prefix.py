def enum_prefix(msg_name, enum_name):
    return msg_name.upper() + enum_name.replace("_", "").upper() + "_"


def without_prefix(name):
    return name.partition("_")[2]
