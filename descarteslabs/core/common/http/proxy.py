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

import abc
import os
from typing import Dict, Union

from strenum import StrEnum


class ProxyAuthentication(abc.ABC):
    """Provides a common interface to handle proxies and authentication between HTTP and GRPC.

    See :py:meth:`ProxyAuthentication.authorize` for more information.
    """

    class Protocol(StrEnum):
        """Protocols supported by ProxyAuthentication.

        Attributes
        ----------
        GRPC : enum
            gRPC protocol.
        HTTP : enum
            HTTP Protocol.
        HTTPS : enum
            HTTPS Protocol.
        """

        GRPC = "grpc"
        HTTP = "http"
        HTTPS = "https"

    _instance: "ProxyAuthentication" = None
    _proxies: Dict[str, str] = {}

    @classmethod
    def get_registered_instance(cls) -> "ProxyAuthentication":
        return cls._instance

    @classmethod
    def register(cls, implementation: Union[type, "ProxyAuthentication"]):
        """Registers a proxy authentication implementation.

        Parameters
        ==========
        implementation : Union[type, ProxyAuthentication]
            An instance or subclass type of :py:class:`ProxyAuthentication`.
        """

        if isinstance(implementation, ProxyAuthentication):
            cls._instance = implementation
        elif issubclass(implementation, ProxyAuthentication):
            cls._instance = implementation()
        else:
            raise TypeError(
                "ProxyAuthentication implementation must be of type ProxyAuthentication"
            )

    @classmethod
    def unregister(cls):
        cls._instance = None

    @classmethod
    def get_proxies(cls):
        """Returns a dictionary of the configured proxy for each protocol.

        User defined proxies will take precedence over default environment vars.
        """
        return {**cls._get_proxies_from_env(), **cls._proxies}

    @classmethod
    def set_proxy(cls, proxy: str, protocol: Protocol = None):
        """Configures a proxy for a given protocol.

        If no protocol is specified, all known protocols will be configured to use the
        specified proxy.

        Parameters
        ==========
        proxy : str
            The URL of the proxy.
        protocol : :py:class:`Protocol`
            The Protocol that should be modified to use the specified proxy.
        """

        if protocol is None:
            cls._proxies = {k: proxy for k in cls.get_proxies().keys()}
        else:
            cls._proxies[protocol] = proxy

    @classmethod
    def get_proxy(cls, protocol: Protocol) -> str:
        """Determines the proxy to use for a given protocol.

        Attempts to use user defined proxies and fallsback to proxies defined by
        environment variables.

        Parameters
        ==========
        protocol : :py:class:`Protocol`
            The Protocol for which to retrieve the proxy URL.
        """

        return cls.get_proxies().get(protocol, None)

    @classmethod
    def clear_proxy(cls, protocol: Protocol = None):
        """Clears a proxy that was defined by ``set_proxy``.

        If no protocol is specified, all programmatically specified proxies will be
        cleared. This will result in environment variables being used instead.

        Parameters
        ==========
        protocol : :py:class:`Protocol`
            The Protocol for which to clear the proxy configuration.
        """

        if protocol:
            cls._proxies.pop(protocol, None)
        else:
            cls._proxies.clear()

    @classmethod
    def _get_proxies_from_env(cls):
        """Retrieves proxies from environment variables.

        We preserve gRPC behavior by falling back to HTTPS then HTTP.
        """

        return {
            cls.Protocol.HTTP: os.environ.get("HTTP_PROXY"),
            cls.Protocol.HTTPS: os.environ.get("HTTPS_PROXY"),
            cls.Protocol.GRPC: os.environ.get(
                "GRPC_PROXY",
                os.environ.get("HTTPS_PROXY", os.environ.get("HTTP_PROXY")),
            ),
        }

    def get_verified_headers(self, proxy: str, protocol: Protocol) -> dict:
        """Calls `authorize` and verifies the returned headers.

        Intended to be used to retrieve the headers instead of calling
        py:meth:`ProxyAuthentication.authorize` directly.

        Parameters
        ==========
        proxy : str
            The URL of the proxy that was selected for the connection.
        protocol : :py:class:`Protocol`
            The Protocol that will be used for requests across the proxy after the
            connection is established.

        Raises
        ======
        TypeError
            When :py:meth:`ProxyAuthentication.authorize` returns a type that is not a
            dictionary.
        """

        headers = self.authorize(proxy, protocol)

        if not isinstance(headers, dict):
            raise TypeError("ProxyAuthentication.authorize must return a dictionary")

        return headers

    # User Implementable methods below this line

    @abc.abstractmethod
    def authorize(self, proxy: str, protocol: Protocol) -> dict:
        """This method is used to authorize an HTTP, HTTPS, or GRPC connection to a service.

        If you are attempting to use basic auth, you should include your
        username and password in the proxy URL. In this case, you do not need to
        implement this interface.

        .. code::

            proxy = "http://user:pass@someproxy:8080"
            os.environ["HTTPS_PROXY"] = proxy
            # OR
            ProxyAuthentication.set_proxy(proxy, ProxyAuthentication.Protocol.HTTPS)
            ProxyAuthentication.set_proxy(proxy, ProxyAuthentication.Protocol.GRPC)
            # OR
            ProxyAuthentication.set_proxy(proxy) # sets all known protocols

        If you are implementing this interface, you should return a dictionary with the
        headers as the keys and any string values that should be sent.

        .. code::

            class MyProxyAuth(ProxyAuthentication)
                def __init__(self, client_id: str = None, secret: str = None):
                    self.client_id = client_id
                    self.secret = secret

                def authorize(self, proxy: str, protocol: str) -> dict:
                    # Here you could use the instance variables to fetch a new key.
                    # Or whatever implementation you desire.

                    return {
                        "some-header": "some value",
                        "x-api-key": "123456",
                    }

            ProxyAuthentication.register(MyProxyAuth)
            # OR
            ProxyAuthentication.register(MyProxyAuth("some-client-id", "some-secret"))

        Parameters
        ==========
        proxy : str
            The URL of the proxy that was selected for the connection.
        protocol : :py:class:`Protocol`
            The Protocol that will be used for requests across the proxy after the
            connection is established.

        Notes
        =====
        For HTTP (urls starting with `http://`):
            Authorize is called for every HTTP request.

            The returned headers are merged with the original request headers.

        For HTTPS (urls starting with `https://`) and GRPC:
            Authorize is called for the initial CONNECT request for the underlying
            socket to the proxy server.

            The returned headers will not be present on any requests through the
            established connection to prevent data leaking.

        For HTTPS Proxies (proxy urls starting with `https://`) and GRPC:
            Unless your proxy is signed by a third party Certificate Authority, you will
            need to configure a CA certificate.

            This can be done at the system level or through the `CURL_CA_BUNDLE` or
            `REQUESTS_CA_BUNDLE` environment variable.
        """

        pass
