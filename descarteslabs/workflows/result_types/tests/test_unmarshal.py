import pytest

from .. import unmarshal


def test_register():
    with pytest.raises(TypeError):
        unmarshal.unmarshal("Foobar", "bar")

    unmarshal.register("Foobar", lambda x: "foo: {}".format(x))
    assert unmarshal.unmarshal("Foobar", "bar") == "foo: bar"

    with pytest.raises(NameError):
        unmarshal.register("Foobar", lambda x: x)


def test_identity():
    unmarshal.register("self", unmarshal.identity)
    x = object()
    assert unmarshal.unmarshal("self", x) is x


def test_unpack():
    class Unpacker(object):
        def __init__(self, x, y):
            self.x = x
            self.y = y

    unmarshal.register("unpack", unmarshal.unpack_into(Unpacker))
    unmarshalled = unmarshal.unmarshal("unpack", dict(x=1, y=True))
    assert isinstance(unmarshalled, Unpacker)
    assert unmarshalled.x == 1
    assert unmarshalled.y is True
