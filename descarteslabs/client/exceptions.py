# Copyright 2018-2020 Descartes Labs.
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


class ClientError(Exception):
    """ Base class for all client exceptions."""

    pass


class AuthError(ClientError):
    """Authentication error, improperly supplied credentials."""

    pass


class OauthError(AuthError):
    """Authentication error, failure from OAuth authentication service."""

    pass


class ServerError(Exception):
    """Server or service failure."""

    status = 500


class BadRequestError(ClientError):
    """Client request with invalid or incorrect parameters."""

    status = 400


class NotFoundError(ClientError):
    """Resource not found."""

    status = 404


class ProxyAuthenticationRequiredError(ClientError):
    """Client request needs proxy authentication."""

    status = 407


class ConflictError(ClientError):
    """Client request conflicts with existing state."""

    status = 409


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
