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

import re
import unittest

import responses

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.places import Places

public_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZ29vZ2xlLW9hdXRoMnwxMTExMzg1NTY1MjQ4MTIzOTU3MTIiLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.sbSzD9ACNZvaxSgClZCnZMpee_p5MBaKV9uHZQonD6Q"  # noqa


class TestPlaces(unittest.TestCase):
    def setUp(self):
        self.url = "http://example.com"
        self.instance = Places(
            url=self.url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.match_url = re.compile(self.url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)

    @responses.activate
    def test_placetypes(self):
        self.mock_response(responses.GET, ["continent", "country"])
        data = self.instance.placetypes()
        assert 2 == len(data)

    @responses.activate
    def test_find(self):
        self.mock_response(responses.GET, [{"id": 85632693}])
        r = self.instance.find("united-states_iowa")
        assert 1 == len(r)

    @responses.activate
    def test_search(self):
        self.mock_response(responses.GET, [{"id": 85632693}])
        r = self.instance.search("texas")
        assert 1 == len(r)

        r = self.instance.search("texas", country="united-states")
        assert 1 == len(r)

        r = self.instance.search("texas", country="united-states", placetype="county")
        assert 1 == len(r)

        r = self.instance.search(
            "texas", country="united-states", region="oklahoma", placetype="county"
        )
        assert 1 == len(r)

    @responses.activate
    def test_shape(self):
        self.mock_response(responses.GET, {"id": 85632693})
        r = self.instance.shape("north-america_united-states_iowa")
        assert 85632693 == r["id"]

    @responses.activate
    def test_prefix(self):
        # counties by default
        self.mock_response(
            responses.GET, {"type": "FeatureCollection", "features": [{"id": 85632693}]}
        )
        r = self.instance.prefix("north-america_united-states_iowa")
        assert 1 == len(r["features"])

        r = self.instance.prefix(
            "north-america_united-states_iowa", placetype="district"
        )
        assert 1 == len(r["features"])


if __name__ == "__main__":
    unittest.main()
