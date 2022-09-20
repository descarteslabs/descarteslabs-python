import unittest
from unittest import mock

import grpc
import pytest
from descarteslabs.common.http.proxy import ProxyAuthentication

from ....common.proto.health import health_pb2_grpc
from ....common.retry import Retry
from ..client import USER_AGENT_HEADER, GrpcClient


class FakeRpcError(grpc.RpcError):
    def __init__(self, message, code):
        self._code = code

    def code(self):
        return self._code


@mock.patch.object(GrpcClient, "_populate_api")
@mock.patch.object(health_pb2_grpc, "HealthStub")
def test_client_health(stub, _):
    stub.return_value.Check.return_value = True
    client = GrpcClient("host", auth=mock.Mock())

    assert client.health(timeout=5) == stub.return_value.Check.return_value
    assert stub.return_value.Check.call_args_list[0][1]["timeout"] == 5


@mock.patch.object(GrpcClient, "_populate_api")
@mock.patch.object(health_pb2_grpc, "HealthStub")
def test_client_health_called_with_user_agent_metadata(stub, _):
    stub.return_value.Check.return_value = True
    client = GrpcClient("host", auth=mock.Mock())

    assert client.health() == stub.return_value.Check.return_value
    assert USER_AGENT_HEADER in stub.return_value.Check.call_args_list[0][1]["metadata"]


@mock.patch.object(GrpcClient, "_populate_api")
@mock.patch.object(health_pb2_grpc, "HealthStub")
def test_client_health_called_with_default_metadata(stub, _):
    stub.return_value.Check.return_value = True
    client = GrpcClient(
        "host", auth=mock.Mock(), default_metadata=(("x-test-header", "foo"),)
    )

    assert client.health() == stub.return_value.Check.return_value
    assert ("x-test-header", "foo") in stub.return_value.Check.call_args_list[0][1][
        "metadata"
    ]
    # Confirm that user-agent header is still included.
    assert USER_AGENT_HEADER in stub.return_value.Check.call_args_list[0][1]["metadata"]


@mock.patch.object(GrpcClient, "_populate_api")
@mock.patch.object(health_pb2_grpc, "HealthStub")
def test_client_health_default_retry_false_predicate(stub, _):
    stub.return_value.Check.side_effect = [TypeError(), True]
    client = GrpcClient("host", auth=mock.Mock())

    with pytest.raises(TypeError):
        client.health()

    assert len(stub.return_value.Check.call_args_list) == 1


@mock.patch.object(GrpcClient, "_populate_api")
def test_wrap_stub_with_default_retry(_):
    def f(*args, **kwargs):
        return args, kwargs

    retry = mock.Mock()
    client = GrpcClient("host", auth=mock.Mock())
    wrapped = client._wrap_stub(f, retry, ())
    wrapped()
    retry.assert_called_once_with(f)


@mock.patch.object(GrpcClient, "_populate_api")
def test_wrap_stub_with_kwarg(_):
    args = (0,)
    kwargs = {
        "foo": "bar",
        "metadata": (("x-wf-channel", "foo"),),
    }

    f = mock.Mock()

    client = GrpcClient("host", auth=mock.Mock())
    wrapped = client._wrap_stub(f, mock.Mock(), ())
    wrapped(*args, retry=Retry(), **kwargs)
    f.assert_called_once_with(*args, **kwargs)


@mock.patch.object(GrpcClient, "_populate_api")
def test_wrap_stub_args_kwargs(_):
    args = (0,)
    kwargs = {
        "foo": "bar",
        "metadata": (("x-wf-channel", "foo"),),
    }

    f = mock.Mock()

    client = GrpcClient("host", auth=mock.Mock())
    wrapped = client._wrap_stub(f, Retry(), ())
    wrapped(*args, **kwargs)
    f.assert_called_once_with(*args, **kwargs)


@mock.patch.object(GrpcClient, "_populate_api")
def test_metadata_header(_):
    # Test that channel is added as a header
    args = (0,)
    kwargs = {
        "foo": "bar",
    }

    f = mock.Mock()

    client = GrpcClient("host", auth=mock.Mock())

    default_metadata = (("x-wf-channel", "foo"),)
    wrapped = client._wrap_stub(f, Retry(), default_metadata)
    wrapped(*args, **kwargs)

    kwargs_w_header = kwargs.copy()
    kwargs_w_header["metadata"] = (("x-wf-channel", "foo"),)

    f.assert_called_once_with(*args, **kwargs_w_header)

    # Test header can be shadowed when function is called
    f = mock.Mock()

    wrapped = client._wrap_stub(f, Retry(), default_metadata)
    wrapped(*args, metadata=(("x-wf-channel", "override_value"),), **kwargs)

    kwargs_w_header = kwargs.copy()
    kwargs_w_header["metadata"] = (("x-wf-channel", "override_value"),)

    f.assert_called_once_with(*args, **kwargs_w_header)

    # Test headrs can be merged
    f = mock.Mock()

    wrapped = client._wrap_stub(f, Retry(), default_metadata)
    wrapped(*args, metadata=(("key", "val"),), **kwargs)

    kwargs_w_header = kwargs.copy()
    kwargs_w_header["metadata"] = (("x-wf-channel", "foo"), ("key", "val"))

    f.assert_called_once_with(*args, **kwargs_w_header)


@mock.patch.object(GrpcClient, "_populate_api")
def test_add_api_metadata_header_merge(_):
    client = GrpcClient(
        "host", auth=mock.Mock(), default_metadata=(("foo", "bar"), ("baz", "qux"))
    )
    client._initialize()

    stub_callable = mock.Mock()
    stub = mock.Mock()
    stub_callable.return_value = stub

    client._add_stub("Foo", stub_callable)
    client._add_api(
        "Foo",
        "Bar",
        # ("baz", "quux") should override ("baz", "qux")
        default_metadata=(("baz", "quux"), ("corge", "grault"), ("garply", "waldo")),
    )
    # ("garply", "fred") should override ("garply", "waldo")
    client.api["Bar"]("thud", metadata=(("garply", "fred"), ("plugh", "xyzzy")))
    stub.Bar.assert_called_once_with(
        "thud",
        metadata=(
            ("foo", "bar"),
            ("baz", "quux"),
            USER_AGENT_HEADER,
            ("corge", "grault"),
            ("garply", "fred"),
            ("plugh", "xyzzy"),
        ),
    )


@mock.patch.object(GrpcClient, "_populate_api")
def test_close(_):
    mock_channel = mock.Mock()
    client = GrpcClient("host")
    client._channel = mock_channel

    # close if channel open
    client.close()
    mock_channel.close.assert_called_once_with()
    assert client._channel is None

    # close if no channel open
    client.close()
    assert client._channel is None


@mock.patch.object(GrpcClient, "_populate_api")
def test_context_manager(_):
    client = GrpcClient("host")
    with mock.patch.object(client, "close") as close:
        with client as client_:
            assert client_ is client
        close.assert_called_once_with()


class TestProxyAuth(unittest.TestCase):
    class MyGrpcClient(GrpcClient):
        def _populate_api(self):
            pass

    def tearDown(self):
        ProxyAuthentication.unregister()
        ProxyAuthentication.clear_proxy()

    def test_no_options(self):
        client = TestProxyAuth.MyGrpcClient("some-service")
        options = client._build_channel_options()
        assert options == []

    def test_registered_no_proxy_set(self):
        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                return {
                    "Proxy-Authentication": "some-token",
                    "x-another-header": "some-other-header",
                }

        ProxyAuthentication.register(MyProxyAuth)
        client = TestProxyAuth.MyGrpcClient("some-service")
        options = client._build_channel_options()
        assert options == []

    def test_determines_proxy(self):
        ProxyAuthentication.set_proxy("http://some-proxy.test:8888")

        client = TestProxyAuth.MyGrpcClient("some-service")
        options = client._build_channel_options()
        assert options == [("grpc.http_proxy", "http://some-proxy.test:8888")]

        ProxyAuthentication.set_proxy(
            "http://grpc-specific-override:1234", ProxyAuthentication.Protocol.GRPC
        )
        options = client._build_channel_options()
        assert options == [("grpc.http_proxy", "http://grpc-specific-override:1234")]

    def test_validates_authorize(self):
        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                return 15

        ProxyAuthentication.register(MyProxyAuth)
        ProxyAuthentication.set_proxy("http://some-proxy.test:8888")

        with self.assertRaisesRegex(TypeError, "must return a dictionary"):
            client = TestProxyAuth.MyGrpcClient("some-service")
            client.channel

    def test_sends_connect_headers(self):
        class MyProxyAuth(ProxyAuthentication):
            def authorize(self, proxy: str, protocol: str) -> dict:
                return {
                    "Proxy-Authentication": "some-token",
                    "x-another-header": "some-other-header",
                }

        ProxyAuthentication.register(MyProxyAuth)
        ProxyAuthentication.set_proxy("http://some-proxy.test:8888")

        expected_options = [
            ("grpc.http_proxy", "http://some-proxy.test:8888"),
            (
                "grpc.http_connect_headers",
                "Proxy-Authentication: some-token\nx-another-header: some-other-header",
            ),
        ]

        client = TestProxyAuth.MyGrpcClient("some-service")
        options = client._build_channel_options()
        assert options == expected_options

        mock_chan_factory = mock.Mock()
        client.SECURE_CHANNEL_FACTORY = mock_chan_factory
        client.channel

        assert mock_chan_factory.called

        host, creds, headers, *other = mock_chan_factory.call_args[0]
        assert host == "some-service:443"
        assert type(creds) == grpc.ChannelCredentials
        assert headers == expected_options
