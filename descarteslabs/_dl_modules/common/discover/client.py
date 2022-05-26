from ...client.grpc import GrpcClient
from ..proto.discover import discover_pb2_grpc


class DiscoverGrpcClient(GrpcClient):
    """TODO: add docstring"""

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

    def __getattr__(self, attr):
        try:
            return self.api[attr]
        except KeyError:
            raise AttributeError(attr)
