from descarteslabs.config import get_settings

from ...client.grpc import GrpcClient
from ..proto.discover import discover_pb2_grpc


class DiscoverGrpcClient(GrpcClient):
    """Low-level gRPC client for interacting with the Discover backend. Not intended for users to use directly.

    Parameters
    ----------
    host : str, optional
        The backend host to connect to.
    port : int, optional
        The backend port to connect to.
    auth : Auth, optional
        The authentication instance to use.
    certificate : bytes, optional
        The certificate to use when connecting to the backend service.
    default_retry : Retry, int, optional
        The retry instance or number of times to retry a connection before giving up.
    default_metadata : tuple, optional
        Metadata (headers) to send to every RPC when called.
    use_insecure_channel : bool, optional
        If set, an insecure channel will be used.
    **grpc_client_kwargs : dict, optional
        Additional arguments to use when creating the client instance.
        Refer to :py:meth:`GrpcClient.__init__` to see available options.
    """

    def __init__(
        self,
        host=None,
        port=None,
        auth=None,
        certificate=None,
        default_retry=None,
        default_metadata=None,
        use_insecure_channel=False,
        **kwargs,
    ):
        if host is None:
            host = get_settings().discover_host

        if port is None:
            port = int(get_settings().discover_port)

        super().__init__(
            host,
            auth,
            certificate,
            port,
            default_retry,
            default_metadata,
            use_insecure_channel,
            **kwargs,
        )

    def _populate_api(self):
        self._add_stub("DiscoverAccessGrant", discover_pb2_grpc.AccessGrantApiStub)
        self._add_stub("DiscoverAsset", discover_pb2_grpc.AssetApiStub)

        self._add_api("DiscoverAccessGrant", "CreateAccessGrant")
        self._add_api("DiscoverAccessGrant", "DeleteAccessGrant")
        self._add_api("DiscoverAccessGrant", "ListAccessGrants")
        self._add_api("DiscoverAccessGrant", "ListAccessGrantsStream")
        self._add_api("DiscoverAccessGrant", "ReplaceAccessGrant")
        self._add_api("DiscoverAsset", "CreateAsset")
        self._add_api("DiscoverAsset", "GetAsset")
        self._add_api("DiscoverAsset", "ListAssets")
        self._add_api("DiscoverAsset", "MoveAsset")
        self._add_api("DiscoverAsset", "UpdateAsset")
        self._add_api("DiscoverAsset", "DeleteAsset")
