import pytest

from .. import client


def test_keyref_graft():
    assert client.keyref_graft("some-key") == {"returns": "some-key"}


def test_keyref_type_error():
    with pytest.raises(TypeError):
        client.keyref_graft(42)


def test_keyref_value_error():
    for keyword in client.RESERVED_WORDS:
        with pytest.raises(ValueError):
            client.keyref_graft(keyword)
