# Copyright 2018-2024 Descartes Labs.
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

import logging
import uuid
from http import HTTPStatus
import warnings

import requests
import requests.adapters
import requests.compat
import requests.utils
import urllib3
import urllib3.exceptions

try:
    from urllib3.contrib.socks import SOCKSProxyManager
except ImportError:

    def SOCKSProxyManager(*args, **kwargs):
        raise requests.exceptions.InvalidSchema(
            "Missing dependencies for SOCKS support."
        )


from descarteslabs.exceptions import (
    BadRequestError,
    ClientError,
    ConflictError,
    ForbiddenError,
    GatewayTimeoutError,
    GoneError,
    MethodNotAllowedError,
    NotFoundError,
    ProxyAuthenticationRequiredError,
    RateLimitError,
    RequestCancellationError,
    ServerError,
    UnauthorizedError,
    ValidationError,
)

from .proxy import ProxyAuthentication

# Disable warnings for retries etc
logging.getLogger("urllib3").setLevel(logging.ERROR)
logging.getLogger("urllib3").propagate = False


class HttpHeaderKeys:
    RequestGroup = "X-Request-Group"
    RetryAfter = "Retry-After"
    ProxyAuthenticate = "Proxy-Authenticate"


class HTTPAdapter(requests.adapters.HTTPAdapter):
    """Custom HTTPAdapter to integrate ProxyAuthentication with requests."""

    def proxy_headers(self, proxy: str, url: str = None):
        """Sets headers used for the connection to the proxy.

        Parameters
        ==========
        proxy : str
            The proxy URL.
        url : Optional[str]
            The request URL.

        Note
        ====
        If the URL starts with `http://`, the headers are merged with the request
        headers.
        """

        if url is None:
            protocol = ProxyAuthentication.Protocol.HTTPS
        else:
            if url.startswith(ProxyAuthentication.Protocol.HTTPS):
                protocol = ProxyAuthentication.Protocol.HTTPS
            elif url.startswith(ProxyAuthentication.Protocol.HTTP):
                protocol = ProxyAuthentication.Protocol.HTTP
            else:
                raise ValueError(
                    "Protocol must be either {} or {}".format(
                        ProxyAuthentication.Protocol.HTTP,
                        ProxyAuthentication.Protocol.HTTPS,
                    )
                )

        proxy_auth = ProxyAuthentication.get_registered_instance()
        if proxy_auth is None:
            return super().proxy_headers(proxy)

        return proxy_auth.get_verified_headers(proxy, protocol)

    def send(
        self, request: requests.PreparedRequest, proxies=None, **kwargs
    ) -> requests.Response:
        """Override the base send method of the adapter to be able to specify proxies from ProxyAuthentication.

        Parameters
        ==========
        request: requests.PreparedRequest
            The prepared request to send.
        proxies : dict
            The proxies to use for the request.
        kwargs : dict
            Additional request arguments see
            :py:meth:`requests.adapter.HTTPAdapter.send`.
        """

        # proxies defaults to {} by the time it gets here
        if not proxies:
            protocol = request.url.split(":")[0]
            proxy = ProxyAuthentication.get_proxy(protocol)

            if proxy:
                proxies = {protocol: proxy}

        try:
            return super().send(request, proxies=proxies, **kwargs)
        except urllib3.exceptions.ProxyError as ex:
            # Unfortunately response headers are pretty low level and require a lot of overriding.
            # If this is really desired look at https://stackoverflow.com/questions/39068998/reading-connect-headers
            # or https://gist.github.com/bpartridge/9c758c5e70222bac6ce6e1db7bb4d8ea

            # Since we use retry, MaxRetryException converts the underlying exception
            # into a string giving us no choice but to search for the string
            if "407 Proxy Authentication Required" in ex:
                response = requests.Response()
                response.status_code = HTTPStatus.PROXY_AUTHENTICATION_REQUIRED
                return response
            else:
                raise

    def proxy_manager_for(self, proxy, url=None, **proxy_kwargs):
        """Copied from requests.adapters.HTTPAdapter to pass request url to proxy_headers()

        Parameters
        ==========
        proxy : str
            The selected proxy for the request.
        url : str
            The URL of the request.
        proxy_kwargs : dict
            Additional request arguments see
            :py:meth:`requests.adapter.HTTPAdapter.proxy_manager_for`.
        """

        if proxy in self.proxy_manager:
            manager = self.proxy_manager[proxy]
        elif proxy.lower().startswith("socks"):
            username, password = requests.utils.get_auth_from_url(proxy)
            manager = self.proxy_manager[proxy] = SOCKSProxyManager(
                proxy,
                username=username,
                password=password,
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                block=self._pool_block,
                **proxy_kwargs,
            )
        else:
            proxy_headers = self.proxy_headers(proxy, url)
            manager = self.proxy_manager[proxy] = urllib3.poolmanager.proxy_from_url(
                proxy,
                proxy_headers=proxy_headers,
                num_pools=self._pool_connections,
                maxsize=self._pool_maxsize,
                block=self._pool_block,
                **proxy_kwargs,
            )

        return manager

    def get_connection_with_tls_context(self, request, verify, proxies=None, cert=None):
        """Returns a urllib3 connection for the given request and TLS settings.
        This should not be called from user code, and is only exposed for use
        when subclassing the :class:`HTTPAdapter <requests.adapters.HTTPAdapter>`.

        :param request:
            The :class:`PreparedRequest <PreparedRequest>` object to be sent
            over the connection.
        :param verify:
            Either a boolean, in which case it controls whether we verify the
            server's TLS certificate, or a string, in which case it must be a
            path to a CA bundle to use.
        :param proxies:
            (optional) The proxies dictionary to apply to the request.
        :param cert:
            (optional) Any user-provided SSL certificate to be used for client
            authentication (a.k.a., mTLS).
        :rtype:
            urllib3.ConnectionPool
        """
        proxy = requests.utils.select_proxy(request.url, proxies)
        try:
            host_params, pool_kwargs = self.build_connection_pool_key_attributes(
                request,
                verify,
                cert,
            )
        except ValueError as e:
            raise requests.exceptions.InvalidURL(e, request=request)
        if proxy:
            proxy = requests.utils.prepend_scheme_if_needed(proxy, "http")
            proxy_url = urllib3.util.parse_url(proxy)
            if not proxy_url.host:
                raise requests.exceptions.InvalidProxyURL(
                    "Please check proxy URL. It is malformed "
                    "and could be missing the host."
                )
            proxy_manager = self.proxy_manager_for(proxy, request.url)
            conn = proxy_manager.connection_from_host(
                **host_params, pool_kwargs=pool_kwargs
            )
        else:
            # Only scheme should be lower case
            conn = self.poolmanager.connection_from_host(
                **host_params, pool_kwargs=pool_kwargs
            )

        return conn

    def get_connection(self, url, proxies=None):
        """Copied from requests.adapters.HTTPAdapter to pass request url to proxy_manager_for()

        Parameters
        ==========
        url : str
            The request URL.
        proxies : Optional[dict]
            The proxies configured for this request.
        """

        warnings.warn(
            (
                "`get_connection` has been deprecated in favor of "
                "`get_connection_with_tls_context`. Custom HTTPAdapter subclasses "
                "will need to migrate for Requests>=2.32.2. Please see "
                "https://github.com/psf/requests/pull/6710 for more details."
            ),
            DeprecationWarning,
        )
        proxy = requests.utils.select_proxy(url, proxies)

        if proxy:
            proxy = requests.utils.prepend_scheme_if_needed(proxy, "http")
            proxy_url = urllib3.util.parse_url(proxy)
            if not proxy_url.host:
                raise requests.exceptions.InvalidProxyURL(
                    "Please check proxy URL. It is malformed"
                    " and could be missing the host."
                )
            proxy_manager = self.proxy_manager_for(proxy, url)
            conn = proxy_manager.connection_from_url(url)
        else:
            # Only scheme should be lower case
            parsed = requests.compat.urlparse(url)
            url = parsed.geturl()
            conn = self.poolmanager.connection_from_url(url)

        return conn


class Session(requests.Session):
    """The HTTP Session that performs the actual HTTP request.

    This is the base session that is used for all Descartes Labs HTTP calls which
    itself is derived from `requests.Session
    <https://requests.readthedocs.io/en/master/api/#requests.Session>`_.

    You cannot control its instantiation, but you can derive from this class
    and pass it as the class to use when you instantiate a
    :py:class:`~descarteslabs.client.services.service.Service` or register it as the
    default session class using
    :py:meth:`~descarteslabs.client.services.service.Service.set_default_session_class`.

    Notes
    =====
    Session is not thread safe due to the Adapter and the connection pool which it uses.
    Instead, you should ensure that each thread is using it's own session instead of
    trying to share one.

    Parameters
    ----------
    base_url: str
        The URL prefix to use for communication with the Descartes Labs servers.
    timeout: int or tuple(int, int)
        See `requests timeouts
        <https://requests.readthedocs.io/en/master/user/advanced/#timeouts>`_.
    """

    ATTR_BASE_URL = "base_url"
    ATTR_TIMEOUT = "timeout"

    # Adapts the custom pickling protocol of requests.Session
    __attrs__ = requests.Session.__attrs__ + [ATTR_BASE_URL, ATTR_TIMEOUT]

    def __init__(self, base_url="", timeout=None, retries=None):
        self.base_url = base_url
        self.timeout = timeout

        super(Session, self).__init__()

        self.mount("http://", HTTPAdapter(max_retries=retries))
        self.mount("https://", HTTPAdapter(max_retries=retries))

    def initialize(self):
        """Initialize the :py:class:`Session` instance

        You can override this method in a derived class to add your own initialization.
        This method does nothing in the base class.
        """

        pass

    def request(self, method, url, headers=None, **kwargs):
        """Sends an HTTP request and emits Descartes Labs specific errors.

        Parameters
        ----------
        method: str
            The HTTP method to use.
        url: str
            The URL to send the request to.
        headers: dict
            The Headers to set on the request.
        kwargs: dict
            Additional arguments.  See `requests.request
            <https://requests.readthedocs.io/en/master/api/#requests.request>`_.

        Returns
        -------
        Response
            A :py:class:`request.Response` object.

        Raises
        ------
        BadRequestError
            Either a 400 or 422 HTTP response status code was encountered.
        NotFoundError
            A 404 HTTP response status code was encountered.
        ProxyAuthenticationRequiredError
            A 407 HTTP response status code was encountered indicating proxy
            authentication was not handled or was invalid.
        ConflictError
            A 409 HTTP response status code was encountered.
        GoneError
            A 410 HTTP response status code was encountered.
        ValidationError
            A 422 HTTP response status code was encountered.
            ValidationError extends BadRequestError for backward compatibility.
        RateLimitError
            A 429 HTTP response status code was encountered.
        GatewayTimeoutError
            A 504 HTTP response status code was encountered.
        ~descarteslabs.exceptions.ServerError
            Any HTTP response status code larger than 400 that was not covered above
            is returned as a ServerError.  The original HTTP response status code
            can be found in the attribute :py:attr:`original_status`.
        """

        if self.timeout and self.ATTR_TIMEOUT not in kwargs:
            kwargs[self.ATTR_TIMEOUT] = self.timeout

        if headers is None:
            headers = {}

        headers[HttpHeaderKeys.RequestGroup] = uuid.uuid4().hex
        request_url = self.base_url + url

        try:
            resp = super(Session, self).request(
                method,
                request_url,
                headers=headers,
                **kwargs,
            )
        except IndexError:
            # self._read_status() in http/client returns an IndexError when the request
            # is cancelled.
            raise RequestCancellationError()

        if (
            resp.status_code >= HTTPStatus.OK
            and resp.status_code < HTTPStatus.BAD_REQUEST
        ):
            return resp
        elif resp.status_code == HTTPStatus.BAD_REQUEST:
            raise BadRequestError(resp.text)
        elif resp.status_code == HTTPStatus.UNAUTHORIZED:
            raise UnauthorizedError(resp.text)
        elif resp.status_code == HTTPStatus.FORBIDDEN:
            raise ForbiddenError(resp.text)
        elif resp.status_code == HTTPStatus.NOT_FOUND:
            text = resp.text
            if not text:
                text = "{} {} {}".format(HTTPStatus.NOT_FOUND, method, url)
            raise NotFoundError(text)
        elif resp.status_code == HTTPStatus.METHOD_NOT_ALLOWED:
            raise MethodNotAllowedError(resp.text)
        elif resp.status_code == HTTPStatus.PROXY_AUTHENTICATION_REQUIRED:
            raise ProxyAuthenticationRequiredError(
                resp.text,
                proxy_authenticate=resp.headers.get(HttpHeaderKeys.ProxyAuthenticate),
            )
        elif resp.status_code == HTTPStatus.CONFLICT:
            raise ConflictError(resp.text)
        elif resp.status_code == HTTPStatus.GONE:
            raise GoneError(resp.text)
        elif resp.status_code == HTTPStatus.UNPROCESSABLE_ENTITY:
            # For backward compatibility, ValidationError extends BadRequestError
            raise ValidationError(resp.text)
        elif resp.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            raise RateLimitError(
                resp.text, retry_after=resp.headers.get(HttpHeaderKeys.RetryAfter)
            )
        elif resp.status_code < HTTPStatus.INTERNAL_SERVER_ERROR:
            ex = ClientError(resp.text)
            ex.status = resp.status_code.value
            raise ex
        elif resp.status_code == HTTPStatus.GATEWAY_TIMEOUT:
            raise GatewayTimeoutError(
                "Your request timed out on the server. "
                "Consider reducing the complexity of your request."
            )
        else:
            # The whole error hierarchy has some problems.  Originally a ClientError
            # could be thrown by our client libraries, but any HTTP error was a
            # ServerError.  That changed and HTTP errors below 500 became ClientErrors.
            # That means that this actually should be split in ClientError for
            # status < 500 and ServerError for status >= 500, but that might break
            # things.  So instead, we'll add the original status.
            server_error = ServerError(resp.text)
            server_error.original_status = resp.status_code
            raise server_error
