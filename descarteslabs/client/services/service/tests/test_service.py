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

import pickle
import unittest

import mock

from descarteslabs.client.services.service import JsonApiService, Service
from descarteslabs.client.services.service.service import WrappedSession, requests
from descarteslabs.common.http.authorization import add_bearer

FAKE_URL = "http://localhost"
FAKE_TOKEN = "foo.bar.sig"


class TestService(unittest.TestCase):
    def test_session_token(self):
        service = Service("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        self.assertEqual(
            service.session.headers.get("Authorization"), add_bearer(FAKE_TOKEN)
        )

    def test_client_session_header(self):
        service = Service("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        assert "X-Client-Session" in service.session.headers


class TestJsonApiService(unittest.TestCase):
    def test_session_token(self):
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        self.assertEqual(
            service.session.headers.get("Authorization"), add_bearer(FAKE_TOKEN)
        )

    def test_client_session_header(self):
        service = JsonApiService("foo", auth=mock.MagicMock(token=FAKE_TOKEN))
        assert "X-Client-Session" in service.session.headers


class TestWrappedSession(unittest.TestCase):
    def test_pickling(self):
        session = WrappedSession(FAKE_URL, timeout=10)
        self.assertEqual(10, session.timeout)
        unpickled = pickle.loads(pickle.dumps(session))
        self.assertEqual(10, unpickled.timeout)

    @mock.patch.object(requests.Session, "request")
    def test_request_group_header_none(self, request):
        request.return_value.status_code = 200

        session = WrappedSession("")
        session.request("POST", FAKE_URL)

        request.assert_called_once()
        assert "X-Request-Group" in request.call_args[1]["headers"]

    @mock.patch.object(requests.Session, "request")
    def test_request_group_header_conflict(self, request):
        request.return_value.status_code = 200

        args = "POST", FAKE_URL
        kwargs = dict(headers={"X-Request-Group": "f00"})

        session = WrappedSession("")
        session.request(*args, **kwargs)
        request.assert_called_once_with(*args, **kwargs)  # we do nothing here

    @mock.patch.object(requests.Session, "request")
    def test_request_group_header_no_conflict(self, request):
        request.return_value.status_code = 200

        session = WrappedSession("")
        session.request("POST", FAKE_URL, headers={"foo": "bar"})

        request.assert_called_once()
        assert "X-Request-Group" in request.call_args[1]["headers"]
