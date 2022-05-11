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

import pytest
import unittest
import responses
from tempfile import NamedTemporaryFile
import re

from descarteslabs.auth import Auth
from .. import Storage
from descarteslabs.exceptions import ServerError


public_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTExMzg1NTY1MjQ4MTIzOTU3MTIiLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.sbSzD9ACNZvaxSgClZCnZMpee_p5MBaKV9uHZQonD6Q"  # noqa


class StorageClientTestCase(unittest.TestCase):
    def setUp(self):
        url = "http://example.com"
        self.url = url
        self.client = Storage(
            url=url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.match_url = re.compile(url)

    def mock_response(self, method, body, status=200):
        responses.add(method, self.match_url, body=body, status=status)


class TestStorage(StorageClientTestCase):
    @responses.activate
    def test_set_file(self):
        upload_url = "http://example.com/"
        self.mock_response(responses.GET, upload_url)
        self.mock_response(responses.PUT, "")

        with NamedTemporaryFile() as tmp:
            tmp.write(b"hello world")

            self.client.set_file("foo", tmp)
            assert upload_url == responses.calls[1].request.url

    @responses.activate
    def test_get_file(self):
        data = "hello world"
        self.mock_response(responses.GET, data)

        with NamedTemporaryFile() as tmp:
            self.client.get_file("foo", tmp)
            assert len(data) == tmp.tell()

    @responses.activate
    def test_exists_true(self):
        self.mock_response(responses.HEAD, None, 200)
        assert self.client.exists("foo")

    @responses.activate
    def test_exists_false(self):
        self.mock_response(responses.HEAD, None, 404)
        assert not self.client.exists("foo")

    @responses.activate
    def test_exists_bad_req(self):
        self.mock_response(responses.HEAD, None, 500)
        with pytest.raises(ServerError):
            self.client.exists("foo")


if __name__ == "__main__":
    unittest.main()
