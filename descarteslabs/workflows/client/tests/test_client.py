import grpc
import mock
import pytest

from descarteslabs.common.retry import Retry

from .. import Client
from ..client import _RETRYABLE_STATUS_CODES, wrap_stub


class FakeRpcError(grpc.RpcError):
    def __init__(self, message, code):
        self._code = code

    def code(self):
        return self._code


@mock.patch("descarteslabs.common.proto.health_pb2_grpc.HealthStub")
def test_client_health(stub):
    stub.return_value.Check.return_value = True
    client = Client(auth=mock.Mock())
    assert client.health(timeout=5) == stub.return_value.Check.return_value
    assert stub.return_value.Check.call_args_list[0][1]["timeout"] == 5


@mock.patch("descarteslabs.common.proto.health_pb2_grpc.HealthStub")
def test_client_health_default_retry(stub):
    stub.return_value.Check.side_effect = [
        FakeRpcError("", code=_RETRYABLE_STATUS_CODES[0]),
        True,
    ]
    client = Client(auth=mock.Mock())
    assert client.health() is True
    assert len(stub.return_value.Check.call_args_list) == 2


@mock.patch("descarteslabs.common.proto.health_pb2_grpc.HealthStub")
def test_client_health_default_retry_false_predicate(stub):
    stub.return_value.Check.side_effect = [TypeError(), True]
    client = Client(auth=mock.Mock())

    with pytest.raises(TypeError):
        client.health()

    assert len(stub.return_value.Check.call_args_list) == 1


def test_wrap_stub_with_default_retry():
    def f(*args, **kwargs):
        return args, kwargs

    retry = mock.Mock()
    wrapped = wrap_stub(f, retry)()
    wrapped()
    retry.assert_called_once_with(f)


def test_wrap_stub_with_retry_as_kwarg():
    args = (0,)
    kwargs = {"foo": "bar"}
    f = mock.Mock()

    wrapped = wrap_stub(f, mock.Mock())
    wrapped(*args, retry=Retry(), **kwargs)
    f.assert_called_once_with(*args, **kwargs)


def test_wrap_stub_args_kwargs():
    args = (0,)
    kwargs = {"foo": "bar"}
    f = mock.Mock()

    wrapped = wrap_stub(f, Retry())
    wrapped(*args, **kwargs)
    f.assert_called_once_with(*args, **kwargs)


def test_close():
    mock_channel = mock.Mock()
    client = Client()
    client._channel = mock_channel

    # close if channel open
    client.close()
    mock_channel.close.assert_called_once_with()
    assert client._channel is None

    # close if no channel open
    client.close()
    assert client._channel is None


def test_context_manager():
    client = Client()
    with mock.patch.object(client, "close") as close:
        with client as client_:
            assert client_ is client
        close.assert_called_once_with()
