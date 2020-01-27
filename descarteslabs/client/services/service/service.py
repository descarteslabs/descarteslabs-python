# Copyright 2018-2019 Descartes Labs.
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
import os
import platform
import random
import requests
import sys
import uuid

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from warnings import warn

from descarteslabs.client.auth import Auth
from descarteslabs.client.exceptions import (
    ServerError,
    BadRequestError,
    NotFoundError,
    RateLimitError,
    GatewayTimeoutError,
    ConflictError,
)
from descarteslabs.client.version import __version__
from descarteslabs.common.http.authorization import add_bearer
from descarteslabs.common.threading.local import ThreadLocalWrapper


class HttpMountProtocol(object):
    HTTP = "http://"
    HTTPS = "https://"


class HttpRequestMethod(object):
    DELETE = "DELETE"
    GET = "GET"
    HEAD = "HEAD"
    OPTIONS = "OPTIONS"
    PATCH = "PATCH"
    POST = "POST"
    PUT = "PUT"
    TRACE = "TRACE"


class HttpStatusCode(object):
    TooManyRequests = 429
    InternalServerError = 500
    BadGateway = 502
    ServiceUnavailable = 503
    GatewayTimeout = 504


class HttpHeaderKeys(object):
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


class HttpHeaderValues(object):
    ApplicationJson = "application/json"
    ApplicationVndApiJson = "application/vnd.api+json"
    ApplicationOctetStream = "application/octet-stream"
    DlPython = "dl-python"


class WrappedSession(requests.Session):
    ATTR_BASE_URL = "base_url"
    ATTR_HEADERS = "headers"
    ATTR_TIMEOUT = "timeout"

    # Adapts the custom pickling protocol of requests.Session
    __attrs__ = requests.Session.__attrs__ + [ATTR_BASE_URL, ATTR_TIMEOUT]

    def __init__(self, base_url, timeout=None):
        self.base_url = base_url
        self.timeout = timeout
        super(WrappedSession, self).__init__()

    def request(self, method, url, **kwargs):
        if self.timeout and self.ATTR_TIMEOUT not in kwargs:
            kwargs[self.ATTR_TIMEOUT] = self.timeout

        if self.ATTR_HEADERS not in kwargs:
            kwargs[self.ATTR_HEADERS] = {}

        kwargs[self.ATTR_HEADERS][HttpHeaderKeys.RequestGroup] = uuid.uuid4().hex

        resp = super(WrappedSession, self).request(
            method, self.base_url + url, **kwargs
        )

        if resp.status_code >= 200 and resp.status_code < 400:
            return resp
        elif resp.status_code == 400:
            raise BadRequestError(resp.text)
        elif resp.status_code == 404:
            text = resp.text
            if not text:
                text = "404 {} {}".format(method, url)
            raise NotFoundError(text)
        elif resp.status_code == 409:
            raise ConflictError(resp.text)
        elif resp.status_code == 422:
            raise BadRequestError(resp.text)
        elif resp.status_code == 429:
            raise RateLimitError(
                resp.text, retry_after=resp.headers.get(HttpHeaderKeys.RetryAfter)
            )
        elif resp.status_code == 504:
            raise GatewayTimeoutError(
                "Your request timed out on the server. "
                "Consider reducing the complexity of your request."
            )
        else:
            raise ServerError(resp.text)


class Service(object):
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
        method_whitelist=frozenset(
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
            HttpStatusCode.InternalServerError,
            HttpStatusCode.BadGateway,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.GatewayTimeout,
        ],
    )

    # We share an adapter (one per thread/process) among all clients to take advantage
    # of the single underlying connection pool.
    ADAPTER = ThreadLocalWrapper(lambda: HTTPAdapter(max_retries=Service.RETRY_CONFIG))

    def __init__(self, url, token=None, auth=None, retries=None, session_class=None):
        if auth is None:
            auth = Auth()

        if token is not None:
            warn(
                "setting token at service level will be removed in future",
                DeprecationWarning,
            )
            auth._token = token

        self.auth = auth
        self.base_url = url

        if retries is None:
            self._adapter = Service.ADAPTER
        else:
            self._adapter = ThreadLocalWrapper(lambda: HTTPAdapter(max_retries=retries))

        if session_class is None:
            self._session_class = WrappedSession
        else:
            self._session_class = session_class

        # Sessions can't be shared across threads or processes because the underlying
        # SSL connection pool can't be shared. We create them thread-local to avoid
        # intractable exceptions when users naively share clients e.g. when using
        # multiprocessing.
        self._session = ThreadLocalWrapper(self.build_session)

    @property
    def token(self):
        return self.auth.token

    @token.setter
    def token(self, token):
        self.auth._token = token

    @property
    def session(self):
        session = self._session.get()
        auth = add_bearer(self.token)
        if session.headers.get(HttpHeaderKeys.Authorization) != auth:
            session.headers[HttpHeaderKeys.Authorization] = auth

        return session

    def build_session(self):
        s = self._session_class(self.base_url, timeout=self.TIMEOUT)
        adapter = self._adapter.get()
        s.mount(HttpMountProtocol.HTTPS, adapter)
        s.mount(HttpMountProtocol.HTTP, adapter)

        s.headers.update(
            {
                HttpHeaderKeys.ContentType: HttpHeaderValues.ApplicationJson,
                HttpHeaderKeys.UserAgent: "{}/{}".format(
                    HttpHeaderValues.DlPython, __version__
                ),
            }
        )

        try:
            s.headers.update(
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

        return s


class JsonApiSession(WrappedSession):
    KEY_CATEGORY = "category"
    KEY_MESSAGE = "message"
    KEY_META = "meta"
    KEY_WARNINGS = "warnings"

    def request(self, *args, **kwargs):
        resp = super(JsonApiSession, self).request(*args, **kwargs)

        try:
            json_response = resp.json()
        except ValueError:
            pass
        else:
            if (
                self.KEY_META not in json_response
                or self.KEY_WARNINGS not in json_response[self.KEY_META]
            ):
                return  # This activates the `finally` clause...

            for warning in json_response[self.KEY_META][self.KEY_WARNINGS]:
                if self.KEY_MESSAGE not in warning:  # Mandatory
                    continue

                message = warning[self.KEY_MESSAGE]
                category = UserWarning

                if self.KEY_CATEGORY in warning:
                    category = getattr(builtins, warning[self.KEY_CATEGORY], None)

                    if category is None:
                        category = UserWarning
                        message = "{}: {}".format(warning[self.KEY_CATEGORY], message)

                warn(message, category)
        finally:
            return resp


class JsonApiService(Service):
    KEY_ATTRIBUTES = "attributes"
    KEY_DATA = "data"
    KEY_ID = "id"
    KEY_TYPE = "type"

    def __init__(self, url, session_class=None, **kwargs):
        if session_class is None:
            session_class = JsonApiSession

        super(JsonApiService, self).__init__(url, session_class=session_class, **kwargs)

    def build_session(self):
        s = super(JsonApiService, self).build_session()
        s.headers.update(
            {
                HttpHeaderKeys.ContentType: HttpHeaderValues.ApplicationVndApiJson,
                HttpHeaderKeys.Accept: HttpHeaderValues.ApplicationVndApiJson,
            }
        )
        return s

    @staticmethod
    def jsonapi_document(type, attributes, id=None):
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
        if ids_list is None:
            ids_list = itertools.repeat(None)
        else:
            if len(ids_list) != len(attributes_list):
                raise ValueError(
                    "Different number of resources given than IDs: {} vs {}".foramt(
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


class ThirdPartyService(object):
    CONNECT_TIMEOUT = 9.5
    READ_TIMEOUT = 30
    TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

    RETRY_CONFIG = Retry(
        total=10,
        read=2,
        backoff_factor=random.uniform(1, 3),
        method_whitelist=frozenset(
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
            HttpStatusCode.TooManyRequests,
            HttpStatusCode.InternalServerError,
            HttpStatusCode.BadGateway,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.GatewayTimeout,
        ],
    )

    ADAPTER = ThreadLocalWrapper(
        lambda: HTTPAdapter(max_retries=ThirdPartyService.RETRY_CONFIG)
    )

    def __init__(self, url=""):
        self.base_url = url
        self._session = ThreadLocalWrapper(self.build_session)

    @property
    def session(self):
        return self._session.get()

    def build_session(self):
        s = WrappedSession(self.base_url, timeout=self.TIMEOUT)
        s.mount(HttpMountProtocol.HTTPS, self.ADAPTER.get())

        s.headers.update(
            {
                HttpHeaderKeys.ContentType: HttpHeaderValues.ApplicationOctetStream,
                HttpHeaderKeys.UserAgent: "{}/{}".format(
                    HttpHeaderValues.DlPython, __version__
                ),
            }
        )

        return s
