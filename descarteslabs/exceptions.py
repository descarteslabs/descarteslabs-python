# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Exceptions raised by HTTP clients."""


class ClientError(Exception):
    """Base class for all client exceptions."""

    pass


class AuthError(ClientError):
    """Authentication error, improperly supplied credentials."""

    pass


class OauthError(AuthError):
    """Authentication error, failure from OAuth authentication service."""

    pass


class ConfigError(Exception):
    """Configuration error during initial configuration of the library."""

    pass


class ServerError(Exception):
    """Server or service failure."""

    status = 500


class BadRequestError(ClientError):
    """Client request with incorrect parameters."""

    status = 400


class ValidationError(BadRequestError):
    """Client request with invalid parameters."""

    status = 422


class NotFoundError(ClientError):
    """Resource not found."""

    status = 404


class ProxyAuthenticationRequiredError(ClientError):
    """Client request needs proxy authentication.

    Attributes
    ==========
    status : int
        The status code of the error response.
    proxy_authenticate : Optional[str]
        A `ProxyAuthenticate <https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Proxy-Authenticate>`_
        header if found in the response.
    """

    status = 407

    def __init__(self, message, proxy_authenticate=None) -> None:
        super(ProxyAuthenticationRequiredError, self).__init__(message)

        self.proxy_authenticate = proxy_authenticate


class ConflictError(ClientError):
    """Client request conflicts with existing state."""

    status = 409


class GoneError(ClientError):
    """Client request to a URL which has been permanently removed."""

    status = 410


class RateLimitError(ClientError):
    """
    Client request exceeds rate limits.

    The retry_after member will contain any time limit returned
    in the response.
    """

    status = 429

    def __init__(self, message, retry_after=None):
        """
        Construct a new instance.

        :param str message: The error message.
        :type retry_after: str or None
        :param retry_after: An indication of a
            ``retry-after`` timeout specified by the error response.
        """
        super(RateLimitError, self).__init__(message)
        self.retry_after = retry_after


class RetryWithError(ClientError):
    """Vector service query request timed out."""

    status = 449


class GatewayTimeoutError(ServerError):
    """Timeout from the gateway after failing to route request to destination service."""

    status = 504


class RequestCancellationError(ClientError):
    """Client cancelled the request and no status or response was received."""
