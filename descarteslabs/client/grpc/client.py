import os

import certifi
import grpc
from descarteslabs.client.auth import Auth
from descarteslabs.client.version import __version__
from descarteslabs.common.proto.health import health_pb2, health_pb2_grpc
from descarteslabs.common.retry import Retry, RetryError
from descarteslabs.common.retry.retry import _wraps


from .exceptions import from_grpc_error
from .auth import TokenProviderMetadataPlugin


_RETRYABLE_STATUS_CODES = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.UNKNOWN,
    grpc.StatusCode.DEADLINE_EXCEEDED,
}

USER_AGENT_HEADER = ("user-agent", "dl-python/{}".format(__version__))


def default_grpc_retry_predicate(e):
    try:
        code = e.code()
    except AttributeError:
        return False
    else:
        return code in _RETRYABLE_STATUS_CODES


class GrpcClient:
    """Low-level gRPC client for interacting with the gRPC backends.
    Not intended for users to use directly.

    Examples
    --------
    >>> from descarteslabs.client.grpc import GrpcClient
    >>> class MyClient(GrpcClient):
    ...     def __init__(self):
    ...         # derived classes should configure host
    ...         super().__init__("localhost")
    ...     def _populate_api(self):
    ...         # derived classes must add stubs and RPC methods
    ...         pass
    >>> client = MyClient()
    """

    SECURE_CHANNEL_FACTORY = staticmethod(grpc.secure_channel)
    INSECURE_CHANNEL_FACTORY = staticmethod(grpc.insecure_channel)
    DEFAULT_TIMEOUT = 5
    STREAM_TIMEOUT = 60 * 60 * 24

    def __init__(
        self,
        host,
        auth=None,
        certificate=None,
        port=None,
        default_retry=None,
        default_metadata=None,
        use_insecure_channel=False,
    ):
        if auth is None:
            auth = Auth()

        self.auth = auth
        self.host = host
        if not port:
            if use_insecure_channel:
                port = 8000
            else:
                port = 443
        self.port = port

        if default_metadata is None:
            default_metadata = ()
        self._default_metadata = default_metadata + (USER_AGENT_HEADER,)

        if default_retry is None:
            default_retry = Retry(predicate=default_grpc_retry_predicate, retries=5)
        self._default_retry = default_retry

        self._use_insecure_channel = use_insecure_channel
        self._channel = None
        self._interceptors = []
        self._certificate = certificate
        self._stubs = None
        self._api = None

    @property
    def token(self):
        "The Client token."
        return self.auth.token

    @property
    def channel(self):
        "The GRPC channel of the Client."
        if self._channel is None:
            self._channel = self._open_channel()
            if self._interceptors:
                self._channel = grpc.intercept_channel(
                    self._channel, *self._interceptors
                )
        return self._channel

    @property
    def certificate(self):
        "The Client SSL certificate."
        if self._certificate is None:
            cert_file = os.getenv("SSL_CERT_FILE", certifi.where())
            with open(cert_file, "rb") as f:
                self._certificate = f.read()

        return self._certificate

    @property
    def api(self):
        "The available Client operations, as a dict."
        if self._api is None:
            self._initialize()
        return self._api

    def health(self, timeout=None):
        """Check the health of the GRPC server (SERVING, NOT_SERVING, UNKNOWN).

        Example
        -------
        >>> from descarteslabs.client.grpc import GrpcClient
        >>> GrpcClient().health() # doctest: +SKIP
        SERVING
        """
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT

        return self.api["Check"](
            health_pb2.HealthCheckRequest(), timeout=self.DEFAULT_TIMEOUT
        )

    def close(self):
        "Close the GRPC channel associated with the Client."
        # NOTE: this may be a blocking operation
        if self._channel:
            self._channel.close()
            self._channel = None
        # stubs and apis hang on to channel
        if self._api is not None:
            del self._api
            self._api = None
        if self._stubs is not None:
            del self._stubs
            self._stubs = None

    def _add_stub(self, name, stub):
        self._stubs[name] = stub(self.channel)

    def _add_api(self, stub_name, func_name, default_retry=None, default_metadata=None):
        if default_retry is None:
            default_retry = self._default_retry
        if default_metadata is None:
            default_metadata = ()
        default_metadata = tuple(
            dict(self._default_metadata + default_metadata).items()
        )

        stub = self._stubs[stub_name]
        func = getattr(stub, func_name)

        self._api[func_name] = self._wrap_stub(
            func, default_retry=default_retry, default_metadata=default_metadata
        )

    def _initialize(self):
        self._stubs = {}
        self._add_stub("Health", health_pb2_grpc.HealthStub)

        self._api = {}
        self._add_api("Health", "Check")
        self._populate_api()

    def _populate_api(self):
        """Derived gRPC client classes should use this method to add stubs and RPC methods."""
        raise NotImplementedError

    def _register_interceptor(self, interceptor):
        """Register a client interceptor."""
        self._interceptors.append(interceptor)

    def _clear_interceptors(self):
        """Clear all registered client interceptors."""
        self._interceptors.clear()

    def _get_credentials(self):
        token_provider_plugin = TokenProviderMetadataPlugin(
            # NOTE: This property accessor will fetch a new token if need be.
            lambda: self.token
        )
        dl_auth_call_credentials = grpc.metadata_call_credentials(
            token_provider_plugin, "DL auth plugin"
        )
        ssl_channel_credentials = grpc.ssl_channel_credentials(self.certificate)

        composite_credentials = grpc.composite_channel_credentials(
            ssl_channel_credentials, dl_auth_call_credentials
        )

        return composite_credentials

    def _open_channel(self):
        if self._use_insecure_channel:
            return self.INSECURE_CHANNEL_FACTORY("{}:{}".format(self.host, self.port))
        else:
            return self.SECURE_CHANNEL_FACTORY(
                "{}:{}".format(self.host, self.port), self._get_credentials()
            )

    def _wrap_stub(self, func, default_retry, default_metadata):
        @_wraps(func)
        def wrapper(*args, **kwargs):
            retry, kwargs = self._prepare_stub_kwargs(
                default_retry, default_metadata, kwargs
            )

            try:
                return retry(func)(*args, **kwargs)
            except grpc.RpcError as e:
                raise from_grpc_error(e) from None
            except RetryError as e:
                e._exceptions = [
                    from_grpc_error(exc) if isinstance(exc, grpc.RpcError) else exc
                    for exc in e._exceptions
                ]
                raise e from e._exceptions[-1]

        return wrapper

    @staticmethod
    def _prepare_stub_kwargs(default_retry, default_metadata, kwargs):
        retry = kwargs.pop("retry", default_retry)

        # If retry is none, use identity function.
        if retry is None:

            def retry(f):
                return f

        # Merge and set default request headers
        # example: https://github.com/grpc/grpc/blob/master/examples/python/metadata/metadata_client.py
        merged_metadata = dict(default_metadata + kwargs.get("metadata", ()))

        kwargs["metadata"] = tuple(merged_metadata.items())

        return retry, kwargs

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        self.close()

    def __del__(self):
        self.close()
