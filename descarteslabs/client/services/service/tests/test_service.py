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

import pickle
import unittest

from mock import MagicMock
from descarteslabs.client.services.service import Service, JsonApiService
from descarteslabs.client.services.service.service import WrappedSession


class TestService(unittest.TestCase):
    def test_session_token(self):
        token = "foo.bar.sig"
        service = Service('foo', auth=MagicMock(token=token))
        self.assertEqual(service.session.headers.get('Authorization'), token)


class TestJsonApiService(unittest.TestCase):
    def test_session_token(self):
        token = "foo.bar.sig"
        service = JsonApiService('foo', auth=MagicMock(token=token))
        self.assertEqual(service.session.headers.get('Authorization'), token)


class TestWrappedSession(unittest.TestCase):
    def test_pickling(self):
        session = WrappedSession('http://example.com', timeout=10)
        self.assertEqual(10, session.timeout)
        unpickled = pickle.loads(pickle.dumps(session))
        self.assertEqual(10, unpickled.timeout)


if __name__ == '__main__':
    unittest.main()
