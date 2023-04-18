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

import unittest

from ..service import DefaultClientMixin


class TestDefaultClient(unittest.TestCase):
    class BaseClient(DefaultClientMixin):
        """Test base client to make sure instance type is correct when subclassing"""

        pass

    class TestClient(BaseClient):
        """Test client to make sure the instance type is correct"""

        def __init__(self, url="default"):
            self.url = url

    def test_get_default_client(self):
        TestDefaultClient.TestClient.set_default_client(
            TestDefaultClient.TestClient(url="blah")
        )
        default_client = TestDefaultClient.TestClient.get_default_client()
        assert isinstance(default_client, TestDefaultClient.TestClient)
        assert TestDefaultClient.TestClient.get_default_client() == default_client

    def test_set_default_client(self):
        url = "something"
        TestDefaultClient.TestClient.set_default_client(
            TestDefaultClient.TestClient(url=url)
        )
        assert TestDefaultClient.TestClient.get_default_client().url == url

    def test_set_validates_type(self):
        with self.assertRaisesRegex(ValueError, "client must be an instance of"):
            TestDefaultClient.TestClient.set_default_client("Should Fail")

        with self.assertRaisesRegex(ValueError, "client must be an instance of"):
            TestDefaultClient.TestClient.set_default_client(
                TestDefaultClient.BaseClient()
            )
