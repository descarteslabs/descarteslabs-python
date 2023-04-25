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

try:
    import builtins
except ImportError:
    # Until we get rid of Python2 tests...
    builtins = __builtins__

import itertools
import json
import os
import platform
import random
import sys
import uuid
from http import HTTPStatus
from warnings import warn

from descarteslabs.auth import Auth
from descarteslabs.exceptions import ClientError, ServerError

from ....common.http import Retry, Session
from ....common.http.authorization import add_bearer
from ....common.threading.local import ThreadLocalWrapper
from ...version import __version__


class HttpMountProtocol:
    HTTP = "http://"
    HTTPS = "https://"


class HttpRequestMethod:
    DELETE = "DELETE"
    GET = "GET"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"
    PATCH = "PATCH"
    POST = "POST"
    PUT = "PUT"
    TRACE = "TRACE"


class HttpHeaderKeys:
    Accept = "Accept"
    Authorization = "Authorization"
    ClientSession = "X-Client-Session"
    Conda = "X-Conda"
    ContentType = "Content-Type"
    Notebook = "X-Notebook"
    Platform = "X-Platform"
    Python = "X-Python"
    RequestGroup = "X-Request-Group"
    RetryAfter = "Retry-After"
    UserAgent = "User-Agent"


class HttpHeaderValues:
    ApplicationJson = "application/json"
    ApplicationVndApiJson = "application/vnd.api+json"
    ApplicationOctetStream = "application/octet-stream"
    DlPython = "dl-python"


# For backward compatibility
WrappedSession = Session


class Service:
    """The default Descartes Labs HTTP Service used to communicate with its servers.

    This service has a default timeout and retry policy that retries HTTP requests
    depending on the timeout and HTTP status code that was returned.  This is based
    on the `requests timeouts
    <https://requests.readthedocs.io/en/master/user/advanced/#timeouts>`_
    and the `urllib3 retry object
    <https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.retry.Retry>`_.

    The default timeouts are set to 9.5 seconds for establishing a connection (slightly
    larger than a multiple of 3, which is the TCP default packet retransmission window),
    and 30 seconds for reading a response.

    The default retry logic retries up to 3 times total, a maximum of 2 for establishing
    a connection, 2 for reading a response, and 2 for unexpected HTTP status codes.
    The backoff_factor is a random number between 1 and 3, but will never be more
    than 2 minutes.  The unexpected HTTP status codes that will be retried are ``500``,
    ``502``, ``503``, and ``504`` for any of the HTTP requests. Note that 429s are
    retried automatically according to their retry-after headers without us
    specifying anything here (see the source for urllib3.util.Retry as the API
    documentation doesn't make this clear).

    Parameters
    ----------
    url: str
        The URL prefix to use for communication with the Descartes Labs server.
    token: str, optional
        Deprecated.
    auth: Auth, optional
        A Descartes Labs :py:class:`~descarteslabs.auth.Auth` instance.  If not
        provided, a default one will be instantiated.
    retries: int or urllib3.util.retry.Retry
        If a number, it's the number of retries that will be attempted.  If a
        :py:class:`urllib3.util.retry.Retry` instance, it will determine the retry
        behavior.  If not provided, the default retry policy as described above will
        be used.
    session_class: class
        The session class to use when instantiating the session.  This must be a derived
        class from :py:class:`Session`.  If not provided, the default session class
        is used.  You can register a default session class with
        :py:meth:`Service.set_default_session_class`.

    Raises
    ------
    TypeError
        If you try to use a session class that is not derived from :py:class:`Session`.
    """

    # https://requests.readthedocs.io/en/master/user/advanced/#timeouts
    CONNECT_TIMEOUT = 9.5
    READ_TIMEOUT = 30

    TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

    RETRY_CONFIG = Retry(
        total=3,
        connect=2,
        read=2,
        status=2,
        backoff_factor=random.uniform(1, 3),
        allowed_methods=frozenset(
            [
                HttpRequestMethod.HEAD,
                HttpRequestMethod.TRACE,
                HttpRequestMethod.GET,
                HttpRequestMethod.POST,
                HttpRequestMethod.PUT,
                HttpRequestMethod.PATCH,
                HttpRequestMethod.OPTIONS,
                HttpRequestMethod.DELETE,
            ]
        ),
        status_forcelist=[
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        ],
        remove_headers_on_redirect=[],
    )

    _session_class = Session

    # List of attributes that will be included in state for pickling.
    # Subclasses can extend this attribute list.
    __attrs__ = ["auth", "base_url", "_session_class", "RETRY_CONFIG"]

    @classmethod
    def set_default_session_class(cls, session_class):
        """Set the default session class for :py:class:`Service`.

        The default session is used for any :py:class:`Service` that is instantiated
        without specifying the session class.

        Parameters
        ----------
        session_class: class
            The session class to use when instantiating the session.  This must be the
            class :py:class:`Session` itself or a derived class from
            :py:class:`Session`.
        """

        if not issubclass(session_class, Session):
            raise TypeError(
                "The session class must be a subclass of {}.".format(Session)
            )

        cls._session_class = session_class

    @classmethod
    def get_default_session_class(cls):
        """Get the default session class for :py:class:`Service`.

        Returns
        -------
        Session
            The default session class, which is :py:class:`Session` itself or a derived
            class from :py:class:`Session`.
        """

        return cls._session_class

    def __init__(self, url, token=None, auth=None, retries=None, session_class=None):
        if auth is None:
            auth = Auth.get_default_auth()

        if token is not None:
            warn(
                "setting token at service level will be removed in future",
                FutureWarning,
            )
            auth._token = token

        self.auth = auth
        self.base_url = url

        if retries is None:
            retries = Service.RETRY_CONFIG
        self._retry_config = retries

        if session_class is not None:
            # Overwrite the default session class
            if not issubclass(session_class, Session):
                raise TypeError(
                    "The session class must be a subclass of {}.".format(Session)
                )

            self._session_class = session_class

        self._init_session()

    def _init_session(self):
        # Sessions can't be shared across threads or processes because the underlying
        # SSL connection pool can't be shared. We create them thread-local to avoid
        # intractable exceptions when users naively share clients e.g. when using
        # multiprocessing.
        self._session = ThreadLocalWrapper(self._build_session)

    @property
    def token(self):
        """str: The bearer token used in the requests."""
        return self.auth.token

    @token.setter
    def token(self, token):
        """str: Deprecated"""
        self.auth._token = token

    @property
    def session(self) -> Session:
        """Session: The session instance used by this service."""
        session = self._session.get()
        auth = add_bearer(self.token)
        if session.headers.get(HttpHeaderKeys.Authorization) != auth:
            session.headers[HttpHeaderKeys.Authorization] = auth

        return session

    def _build_session(self):
        session = self._session_class(
            self.base_url, timeout=self.TIMEOUT, retries=self._retry_config
        )
        session.initialize()
        session.headers.update(
            {
                HttpHeaderKeys.ContentType: HttpHeaderValues.ApplicationJson,
                HttpHeaderKeys.UserAgent: "{}/{}".format(
                    HttpHeaderValues.DlPython, __version__
                ),
            }
        )

        try:
            session.headers.update(
                {
                    # https://github.com/easybuilders/easybuild/wiki/OS_flavor_name_version
                    HttpHeaderKeys.Platform: platform.platform(),
                    HttpHeaderKeys.Python: platform.python_version(),
                    # https://stackoverflow.com/questions/47608532/how-to-detect-from-within-python-whether-packages-are-managed-with-conda
                    HttpHeaderKeys.Conda: str(
                        os.path.exists(
                            os.path.join(sys.prefix, "conda-meta", "history")
                        )
                    ),
                    # https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
                    HttpHeaderKeys.Notebook: str("ipykernel" in sys.modules),
                    HttpHeaderKeys.ClientSession: uuid.uuid4().hex,
                }
            )
        except Exception:
            pass

        return session

    def __getstate__(self):
        return dict((attr, getattr(self, attr)) for attr in self.__attrs__)

    def __setstate__(self, state):
        for name, value in state.items():
            setattr(self, name, value)

        self._init_session()


class JsonApiSession(Session):
    """The HTTP Session that performs the actual JSONAPI HTTP request.

    You cannot control its instantiation, but you can derive from this class
    and pass it as the class to use when you instantiate a :py:class:`JsonApiService`
    or register it as the default session class using
    :py:meth:`JsonApiService.set_default_session_class`.

    Parameters
    ----------
    base_url: str
        The URL prefix to use for communication with the Descartes Labs servers.
    timeout: int or tuple(int, int)
        See `requests timeouts
        <https://requests.readthedocs.io/en/master/user/advanced/#timeouts>`_.
    """

    # Warning keys
    KEY_CATEGORY = "category"
    KEY_MESSAGE = "message"
    KEY_META = "meta"
    KEY_WARNINGS = "warnings"

    # Error keys
    KEY_ABOUT = "about"
    KEY_DETAIL = "detail"
    KEY_ERRORS = "errors"
    KEY_HREF = "href"
    KEY_ID = "id"
    KEY_LINKS = "links"
    KEY_PARAMETER = "parameter"
    KEY_POINTER = "pointer"
    KEY_SOURCE = "source"
    KEY_STATUS = "status"
    KEY_TITLE = "title"

    def __init__(self, *args, **kwargs):
        self.rewrite_errors = False  # This may be changed by the JsonApiService
        super(JsonApiSession, self).__init__(*args, **kwargs)

    def initialize(self):
        """Initialize the :py:class:`Session` instance

        You can override this method in a derived class to add your own initialization.
        This method does nothing in the base class.
        """

        pass

    def request(self, *args, **kwargs):
        """Sends an HTTP request and emits Descartes Labs specific errors.

        Parameters
        ----------
        method: str
            The HTTP method to use.
        url: str
            The URL to send the request to.
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
        ~descarteslabs.exceptions.NotFoundError
            A 404 HTTP response status code was encountered.
        ProxyAuthenticationRequiredError
            A 407 HTTP response status code was encountered indicating proxy
            authentication was not handled or was invalid.
        ConflictError
            A 409 HTTP response status code was encountered.
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

        Note
        ----
        If :py:attr:`rewrite_errors` was set to ``True`` in the corresponding
        :py:class:`JsonApiService`, the JSONAPI errors will be rewritten in a more
        human readable format.
        """

        try:
            resp = super(JsonApiSession, self).request(*args, **kwargs)
        except (ClientError, ServerError) as error:
            if self.rewrite_errors:
                self._rewrite_error(error)
            raise

        try:
            self._emit_warnings(resp.json())
        except Exception:
            # Really don't want to raise anything here
            pass

        return resp

    def _emit_warnings(self, json_response):
        if (
            self.KEY_META not in json_response
            or self.KEY_WARNINGS not in json_response[self.KEY_META]
        ):
            return

        for warning in json_response[self.KEY_META][self.KEY_WARNINGS]:
            if self.KEY_MESSAGE not in warning:  # Mandatory
                continue

            message = warning[self.KEY_MESSAGE]
            category = UserWarning

            if self.KEY_CATEGORY in warning:
                category = getattr(builtins, warning[self.KEY_CATEGORY], None)

                if category is None:
                    # Couldn't find this category; add it to the message instead
                    category = UserWarning
                    message = "{}: {}".format(warning[self.KEY_CATEGORY], message)

            warn(message, category)

    def _rewrite_error(self, client_error):
        """Rewrite JSON ClientErrors that are returned to make them easier to read"""
        message = ""

        for arg in client_error.args:
            try:
                errors = json.loads(arg)[self.KEY_ERRORS]

                for error in errors:
                    line = ""
                    separator = ""

                    if self.KEY_TITLE in error:
                        line += error[self.KEY_TITLE]
                        separator = ": "
                    elif self.KEY_STATUS in error:
                        line += error[self.KEY_STATUS]
                        separator = ": "

                    if self.KEY_DETAIL in error:
                        line += separator + error[self.KEY_DETAIL].strip(".")
                        separator = ": "

                    if self.KEY_SOURCE in error:
                        source = error[self.KEY_SOURCE]
                        if self.KEY_POINTER in source:
                            source = source[self.KEY_POINTER].split("/")[-1]
                        elif self.KEY_PARAMETER in source:
                            source = source[self.KEY_PARAMETER]
                        line += separator + source

                    if self.KEY_ID in error:
                        line += " ({})".format(error[self.KEY_ID])

                    if line:
                        message += "\n    " + line

                    if self.KEY_LINKS in error:
                        links = error[self.KEY_LINKS]

                        if self.KEY_ABOUT in links:
                            link = links[self.KEY_ABOUT]

                            if isinstance(link, str):
                                message += "\n        {}".format(link)
                            elif isinstance(link, dict) and self.KEY_HREF in link:
                                message += "\n        {}".format(link[self.KEY_HREF])
            except Exception:
                return

        if message:
            client_error.args = (message,)


class JsonApiService(Service):
    """A JsonApi oriented default Descartes Labs HTTP Service.

    For details see the :py:class:`Service`.  This service adheres to the `JsonApi
    standard <https://jsonapi.org/format/>`_ and interprets responses as needed.

    This service uses the :py:class:`JsonApiSession` which provides some optional
    functionality.

    Parameters
    ----------
    url: str
        The URL prefix to use for communication with the Descartes Labs servers.
    session_class: class
        The session class to use when instantiating the session.  This must be a derived
        class from :py:class:`JsonApiSession`.  If not provided, the default session
        class is used.  You can register a default session class with
        :py:meth:`JsonApiService.set_default_session_class`.
    rewrite_errors: bool
        When set to ``True``, errors are rewritten to be more readable.  Each JsonApi
        error becomes a single line of error information without tags.
    auth: Auth, optional
        A Descartes Labs :py:class:`~descarteslabs.auth.Auth` instance.  If not
        provided, a default one will be instantiated.
    retries: int or urllib3.util.retry.Retry If a number, it's the number of retries
        that will be attempted.  If a :py:class:`urllib3.util.retry.Retry` instance,
        it will determine the retry behavior.  If not provided, the default retry
        policy as described above will be used.

    Raises
    ------
    TypeError
        If you try to use a session class that is not derived from
        :py:class:`JsonApiSession`.
    """

    KEY_ATTRIBUTES = "attributes"
    KEY_DATA = "data"
    KEY_ID = "id"
    KEY_TYPE = "type"

    _session_class = JsonApiSession

    @classmethod
    def set_default_session_class(cls, session_class):
        """Set the default session class for :py:class:`JsonApiService`.

        The default session is used for any :py:class:`JsonApiService` that is
        instantiated without specifying the session class.

        Parameters
        ----------
        session_class: class
            The session class to use when instantiating the session.  This must be the
            class :py:class:`JsonApiSession` itself or a derived class from
            :py:class:`JsonApiSession`.
        """

        if not issubclass(session_class, JsonApiSession):
            raise TypeError(
                "The session class must be a subclass of {}.".format(JsonApiSession)
            )

        cls._session_class = session_class

    @classmethod
    def get_default_session_class(cls):
        """Get the default session class for :py:class:`JsonApiService`.

        Returns
        -------
        JsonApiService
            The default session class, which is :py:class:`JsonApiService` itself or
            a derived class from :py:class:`JsonApiService`.
        """

        return cls._session_class

    def __init__(self, url, session_class=None, rewrite_errors=False, **kwargs):
        if not (session_class is None or issubclass(session_class, JsonApiSession)):
            raise TypeError(
                "The session class must be a subclass of {}.".format(JsonApiSession)
            )

        self.rewrite_errors = rewrite_errors
        super(JsonApiService, self).__init__(url, session_class=session_class, **kwargs)

    def _build_session(self):
        session = super(JsonApiService, self)._build_session()

        session.rewrite_errors = self.rewrite_errors
        session.headers.update(
            {
                HttpHeaderKeys.ContentType: HttpHeaderValues.ApplicationVndApiJson,
                HttpHeaderKeys.Accept: HttpHeaderValues.ApplicationVndApiJson,
            }
        )
        return session

    @staticmethod
    def jsonapi_document(type, attributes, id=None):
        """Return a JsonApi document with a single resource.

        A JsonApi document has the following structure:

        .. code::

            {
                "data": {
                    "type": "...",
                    "id": "...",  // Optional
                    "attributes": {
                        "...": "...",
                        ...
                    }
                }
            }

        Parameters
        ----------
        type: str
            The type of resource; this becomes the ``type`` key in the ``data`` element.
        attributes: dict
            The attributes for this resource; this becomes the ``attributes`` key in
            the ``data`` element.
        id: str, optional
            The optional id for the resource; if provided this becomes the ``id`` key
            in the ``data`` element.

        Returns
        -------
        dict
            A dictionary representing the JsonApi document with ``data`` as the
            top-level key, which itself contains a single resource.
        """

        resource = {
            JsonApiService.KEY_DATA: {
                JsonApiService.KEY_TYPE: type,
                JsonApiService.KEY_ATTRIBUTES: attributes,
            }
        }
        if id is not None:
            resource[JsonApiService.KEY_DATA][JsonApiService.KEY_ID] = id
        return resource

    @staticmethod
    def jsonapi_collection(type, attributes_list, ids_list=None):
        """Return a JsonApi document with a collection of resources.

        The number of elements in the ``attributes_list`` must be identical to the
        number of elements in the ``ids_list``.

        A JsonApi collection has the following structure:

        .. code::

            {
                "data": [
                    {
                        "type": "...",
                        "id": "...",  // Optional
                        "attributes": {
                            "...": "...",
                            ...
                        }
                    }, {
                        ...
                    }, {
                    ...
                ]
            }

        Parameters
        ----------
        type: str
            The type of resource; this becomes the ``type`` key for each resource in
            the collection.  The JsonApi collection contains resources of the same
            type.
        attributes: list(dict)
            A list of attributes for each resource; this becomes the ``attributes``
            key for each resource in the collection.
        id: list(str), optional
            The optional id for the resource; if provided this becomes the ``id`` key
            for each resource in the collection.

        Returns
        -------
        dict
            A dictionary representing the JsonApi document with ``data`` as the
            top-level key, which itself contains a list of resources.

        Raises
        ------
        ValueError
            If the number of elements in ``attributes_list`` differs from the number
            of elements in ``ids_list``.
        """

        if ids_list is None:
            ids_list = itertools.repeat(None)
        else:
            if len(ids_list) != len(attributes_list):
                raise ValueError(
                    "Different number of resources given than IDs: {} vs {}".format(
                        len(attributes_list), len(ids_list)
                    )
                )
        resources = []
        for attributes, id in zip(attributes_list, ids_list):
            resource = {
                JsonApiService.KEY_TYPE: type,
                JsonApiService.KEY_ATTRIBUTES: attributes,
            }
            if id is not None:
                resource[JsonApiService.KEY_ID] = id
            resources.append(resource)
        return {JsonApiService.KEY_DATA: resources}


class ThirdPartyService:
    """The default Descartes Labs HTTP Service used for 3rd party servers.

    This service has a default timeout and retry policy that retries HTTP requests
    depending on the timeout and HTTP status code that was returned.  This is based
    on the `requests timeouts
    <https://requests.readthedocs.io/en/master/user/advanced/#timeouts>`_
    and the `urllib3 retry object
    <https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#urllib3.util.retry.Retry>`_.

    The default timeouts are set to 9.5 seconds for establishing a connection (slightly
    larger than a multiple of 3, which is the TCP default packet retransmission window),
    and 30 seconds for reading a response.

    The default retry logic retries up to 10 times total, a maximum of 2 for
    establishing a connection.  The backoff_factor is a random number between 1 and
    3, but will never be more than 2 minutes.  The unexpected HTTP status codes that
    will be retried are ``429``, ``500``, ``502``, ``503``, and ``504`` for any of the
    HTTP requests. Here we specify 429s explicitly (unlike for the Service class)
    because we have no guarantee that third party services are consistent about
    providing a retry-after header.

    Parameters
    ----------
    url: str
        The URL prefix to use for communication with the 3rd party server.
    session_class: class
        The session class to use when instantiating the session.  This must be a derived
        class from :py:class:`Session`.  If not provided, the default session class
        is used.  You can register a default session class with
        :py:meth:`ThirdPartyService.set_default_session_class`.

    Raises
    ------
    TypeError
        If you try to use a session class that is not derived from :py:class:`Session`.
    """

    CONNECT_TIMEOUT = 9.5
    READ_TIMEOUT = 30
    TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

    RETRY_CONFIG = Retry(
        total=10,
        read=2,
        backoff_factor=random.uniform(1, 3),
        allowed_methods=frozenset(
            [
                HttpRequestMethod.HEAD,
                HttpRequestMethod.TRACE,
                HttpRequestMethod.GET,
                HttpRequestMethod.POST,
                HttpRequestMethod.PUT,
                HttpRequestMethod.OPTIONS,
                HttpRequestMethod.DELETE,
            ]
        ),
        status_forcelist=[
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        ],
    )

    _session_class = Session

    @classmethod
    def set_default_session_class(cls, session_class=None):
        """Set the default session class for :py:class:`ThirdPartyService`.

        The default session is used for any :py:meth:`ThirdPartyService` that is
        instantiated without specifying the session class.

        Parameters
        ----------
        session_class: class
            The session class to use when instantiating the session.  This must be the
            class :py:class:`Session` itself or a derived class from
            :py:class:`Session`.
        """

        if not issubclass(session_class, Session):
            raise TypeError(
                "The session class must be a subclass of {}.".format(Session)
            )

        cls._session_class = session_class

    @classmethod
    def get_default_session_class(cls):
        """Get the default session class for the :py:class:`ThirdPartyService`.

        Returns
        -------
        Session
            The default session class, which is :py:class:`Session` itself or a derived
            class from :py:class:`Session`.
        """

        return cls._session_class

    def __init__(self, url="", session_class=None):
        self.base_url = url

        if session_class is not None:
            if not issubclass(session_class, Session):
                raise TypeError(
                    "The session class must be a subclass of {}.".format(Session)
                )

            self._session_class = session_class

        self._session = ThreadLocalWrapper(self._build_session)

    @property
    def session(self) -> Session:
        return self._session.get()

    def _build_session(self):
        session = self._session_class(self.base_url, timeout=self.TIMEOUT)
        session.initialize()
        session.headers.update(
            {
                # HttpHeaderKeys.ContentType: HttpHeaderValues.ApplicationOctetStream,
                HttpHeaderKeys.UserAgent: "{}/{}".format(
                    HttpHeaderValues.DlPython, __version__
                ),
            }
        )

        return session
