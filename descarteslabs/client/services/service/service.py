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

import itertools
import random
import os
import platform
import sys
from warnings import warn

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from descarteslabs.client.auth import Auth
from descarteslabs.client.version import __version__
from descarteslabs.client.exceptions import ServerError, BadRequestError, NotFoundError, RateLimitError, \
    GatewayTimeoutError, ConflictError
from descarteslabs.common.threading.local import ThreadLocalWrapper


class WrappedSession(requests.Session):

    # Adapts the custom pickling protocol of requests.Session
    __attrs__ = requests.Session.__attrs__ + ["base_url", "timeout"]

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
            raise NotFoundError(resp.text if 'text' in resp else '404 {} {}'.format(method, url))
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

    RETRY_CONFIG = Retry(total=3,
                         connect=2,
                         read=2,
                         status=2,
                         backoff_factor=random.uniform(1, 3),
                         method_whitelist=frozenset([
                             'HEAD', 'TRACE', 'GET', 'POST',
                             'PUT', 'OPTIONS', 'DELETE'
                         ]),
                         status_forcelist=[500, 502, 503, 504])

    # We share an adapter (one per thread/process) among all clients to take advantage
    # of the single underlying connection pool.
    ADAPTER = ThreadLocalWrapper(lambda: HTTPAdapter(max_retries=Service.RETRY_CONFIG))

    def __init__(self, url, token=None, auth=None):
        if auth is None:
            auth = Auth()

        if token is not None:
            warn("setting token at service level will be removed in future", DeprecationWarning)
            auth._token = token

        self.auth = auth

        self.base_url = url

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
        if session.headers.get('Authorization') != self.token:
            session.headers['Authorization'] = self.token

        return session

    def build_session(self):
        s = WrappedSession(self.base_url, timeout=self.TIMEOUT)
        s.mount('https://', self.ADAPTER.get())

        s.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "dl-python/{}".format(__version__),
        })

        try:
            s.headers.update({
                # https://github.com/easybuilders/easybuild/wiki/OS_flavor_name_version
                "X-Platform": platform.platform(),
                "X-Python": platform.python_version(),
                # https://stackoverflow.com/questions/47608532/how-to-detect-from-within-python-whether-packages-are-managed-with-conda
                "X-Conda": str(os.path.exists(os.path.join(sys.prefix, 'conda-meta', 'history'))),
                # https://stackoverflow.com/questions/15411967/how-can-i-check-if-code-is-executed-in-the-ipython-notebook
                "X-Notebook": str('ipykernel' in sys.modules),
            })
        except Exception:
            pass

        return s


class JsonApiService(Service):
    def build_session(self):
        s = super(JsonApiService, self).build_session()
        s.headers.update({
            "Content-Type": "application/vnd.api+json",
            "Accept": "application/vnd.api+json"
        })
        return s

    @staticmethod
    def jsonapi_document(type, attributes, id=None):
        resource = {
            "data": {
                "type": type,
                "attributes": attributes
            }
        }
        if id is not None:
            resource["data"]["id"] = id
        return resource

    @staticmethod
    def jsonapi_collection(type, attributes_list, ids_list=None):
        if ids_list is None:
            ids_list = itertools.repeat(None)
        else:
            if len(ids_list) != len(attributes_list):
                raise ValueError(
                    "Different number of resources given than IDs: {} vs {}".foramt(len(attributes_list), len(ids_list))
                )
        resources = []
        for attributes, id in zip(attributes_list, ids_list):
            resource = {
                "type": type,
                "attributes": attributes
            }
            if id is not None:
                resource["id"] = id
            resources.append(resource)
        return {
            "data": resources
        }


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

    ADAPTER = ThreadLocalWrapper(lambda: HTTPAdapter(max_retries=ThirdPartyService.RETRY_CONFIG))

    def __init__(self, url=''):
        self.base_url = url

        self._session = ThreadLocalWrapper(self.build_session)

    @property
    def session(self):
        return self._session.get()

    def build_session(self):
        s = WrappedSession(self.base_url, timeout=self.TIMEOUT)
        s.mount('https://', self.ADAPTER.get())

        s.headers.update({
            "Content-Type": "application/octet-stream",
            "User-Agent": "dl-python/{}".format(__version__)
        })

        return s
