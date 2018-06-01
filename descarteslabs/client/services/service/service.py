# Copyright 2018 Descartes Labs.
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

import random

import requests
from requests.adapters import HTTPAdapter
from warnings import warn
from urllib3.util.retry import Retry
from descarteslabs.client.auth import Auth
from descarteslabs.client.version import __version__
from descarteslabs.client.exceptions import ServerError, BadRequestError, NotFoundError, RateLimitError, \
    GatewayTimeoutError, ConflictError


class WrappedSession(requests.Session):
    def __init__(self, base_url, timeout=None):
        self.base_url = base_url
        self.timeout = timeout
        super(WrappedSession, self).__init__()

    def request(self, method, url, **kwargs):
        if self.timeout and 'timeout' not in kwargs:
            kwargs['timeout'] = self.timeout

        resp = super(WrappedSession, self).request(method, self.base_url + url, **kwargs)

        if resp.status_code >= 200 and resp.status_code < 400:
            return resp
        elif resp.status_code == 400:
            raise BadRequestError(resp.text)
        elif resp.status_code == 404:
            raise NotFoundError("404 %s %s" % (method, url))
        elif resp.status_code == 409:
            raise ConflictError(resp.text)
        elif resp.status_code == 429:
            raise RateLimitError(resp.text)
        elif resp.status_code == 504:
            raise GatewayTimeoutError(
                "Your request timed out on the server. "
                "Consider reducing the complexity of your request.")
        else:
            raise ServerError(resp.text)


class Service(object):
    TIMEOUT = (9.5, 30)

    RETRY_CONFIG = Retry(total=5,
                         read=2,
                         backoff_factor=random.uniform(1, 3),
                         method_whitelist=frozenset([
                             'HEAD', 'TRACE', 'GET', 'POST',
                             'PUT', 'OPTIONS', 'DELETE'
                         ]),
                         status_forcelist=[500, 502, 503, 504])

    ADAPTER = HTTPAdapter(max_retries=RETRY_CONFIG)

    def __init__(self, url, token=None, auth=None):
        if auth is None:
            auth = Auth()

        if token is not None:
            warn("setting token at service level will be removed in future", DeprecationWarning)
            auth._token = token

        self.auth = auth

        self.base_url = url

        self._session = self.build_session()

    @property
    def token(self):
        return self.auth.token

    @token.setter
    def token(self, token):
        self.auth._token = token

    @property
    def session(self):
        if self._session.headers.get('Authorization') != self.token:
            self._session.headers['Authorization'] = self.token

        return self._session

    def build_session(self):
        s = WrappedSession(self.base_url, timeout=self.TIMEOUT)
        s.mount('https://', self.ADAPTER)

        s.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "dl-python/{}".format(__version__)
        })

        return s


class JsonApiService(Service):
    def build_session(self):
        s = super(JsonApiService, self).build_session()
        s.headers.update({
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json"
        })


class ThirdPartyService(object):
    TIMEOUT = (9.5, 30)

    RETRY_CONFIG = Retry(total=10,
                         read=2,
                         backoff_factor=random.uniform(1, 3),
                         method_whitelist=frozenset([
                             'HEAD', 'TRACE', 'GET', 'POST',
                             'PUT', 'OPTIONS', 'DELETE'
                         ]),
                         status_forcelist=[429, 500, 502, 503, 504])

    ADAPTER = HTTPAdapter(max_retries=RETRY_CONFIG)

    def __init__(self, url=''):
        self.base_url = url

        self._session = self.build_session()

    @property
    def session(self):
        return self._session

    def build_session(self):
        s = WrappedSession(self.base_url, timeout=self.TIMEOUT)
        s.mount('https://', self.ADAPTER)

        s.headers.update({
            "Content-Type": "application/octet-stream",
            "User-Agent": "dl-python/{}".format(__version__)
        })

        return s
