import grpc
from grpc._auth import _sign_request


class TokenProviderMetadataPlugin(grpc.AuthMetadataPlugin):
    """
    A gRPC authentication plugin that supplies `grpc.CallCredentials` via a refreshable
    access token, provided by the given token provider. The __call__ method is invoked
    for every RPC, which invokes the token provider and adds the returned access token
    as a Bearer token in the HTTP authorization header.
    """

    def __init__(self, token_provider):
        """
        Constructs a token provider metadata plugin.

        Parameters
        ----------
        token_provider: callable
            A token provider that returns a valid access token on each invocation.
        """
        self._token_provider = token_provider

    def __call__(self, context, callback):
        """
        Implements authentication by passing metadata to a callback.
        This method will be invoked asynchronously in a separate thread.

        This particular implementation adds the access token returned by the token
        provider as a Bearer token in the HTTP authorization header.

        Parameters
        ----------
        context: `AuthMetadataContext`
            An AuthMetadataContext providing information on the RPC that the plugin is
            being called to authenticate.
        callback: `AuthMetadataPluginCallback`
            An AuthMetadataPluginCallback to be invoked either synchronously or
            asynchronously.
        """
        # NOTE: This is invoked for every RPC
        _sign_request(callback, self._token_provider(), None)
