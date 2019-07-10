registry = {}


def unmarshal(typestr, x):
    try:
        unmarshaller = registry[typestr]
    except KeyError:
        raise TypeError("No unmarshaller registered for '{}'".format(typestr))
    return unmarshaller(x)


def register(typestr, unmarshaller):
    if typestr in registry:
        raise NameError(
            "An unmarshaller is already registered for '{}'".format(typestr)
        )
    registry[typestr] = unmarshaller


def identity(x):
    return x


def astype(typ):
    "Unmarshal by casting into ``typ``, if not already an instance of ``typ``"

    def unmarshaller(x):
        return typ(x) if not isinstance(x, typ) else x

    return unmarshaller


def unpack_into(typ):
    "Unmarshal by unpacking a dict into the constructor for ``typ``"

    def unmarshaller(x):
        return typ(**x)

    return unmarshaller


__all__ = ["unmarshal", "register", "identity", "unpack_into"]
