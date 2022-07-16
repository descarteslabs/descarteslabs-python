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

import base64
import json
import re
import time
import unittest
import warnings

import responses
from descarteslabs.auth import Auth
from shapely.geometry import shape

from .. import Metadata


class MetadataTest(unittest.TestCase):
    def setUp(self):
        payload = (
            base64.b64encode(
                json.dumps(
                    {
                        "aud": "ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
                        "exp": time.time() + 3600,
                    }
                ).encode()
            )
            .decode()
            .strip("=")
        )
        public_token = f"header.{payload}.signature"

        self.url = "http://example.com/metadata"
        self.instance = Metadata(
            url=self.url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.match_url = re.compile(self.url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)

    def test_expr_serialization(self):
        p = self.instance.properties
        q = ((0.1 < p.cloud_fraction <= 0.2) & (p.sat_id == "f00b")) | (
            p.sat_id == "usa-245"
        )
        expected_q = {
            "or": [
                {
                    "and": [
                        {"range": {"cloud_fraction": {"gt": 0.1, "lte": 0.2}}},
                        {"eq": {"sat_id": "f00b"}},
                    ]
                },
                {"eq": {"sat_id": "usa-245"}},
            ]
        }

        assert q.serialize() == expected_q

    def test_expr_contains(self):
        p = self.instance.properties
        q = p.sat_id.in_(("usa-245", "terra"))
        expected_q = {
            "or": [{"eq": {"sat_id": "usa-245"}}, {"eq": {"sat_id": "terra"}}]
        }

        assert q.serialize() == expected_q

    @responses.activate
    def test_paged_search(self):
        features = [{"id": "foo"}]
        token = "token"
        self.mock_response(
            responses.POST, json=features, headers={"x-continuation-token": token}
        )
        collection = self.instance.paged_search(limit=100)
        assert features == collection.features
        assert token == collection.properties.continuation_token

    @responses.activate
    def test_paged_search_deprecated_args(self):
        features = [{"id": "foo"}]
        self.mock_response(responses.POST, json=features)
        with warnings.catch_warnings(record=True) as w:
            collection = self.instance.paged_search(limit=100, start_time="2017-07-08")
            assert 1 == len(w)
            assert w[0].category == FutureWarning
        assert features == collection.features

    @responses.activate
    def test_paged_search_dltile(self):
        features = [{"id": "foo"}]
        tile_geom = {
            "coordinates": [
                [
                    [-94.01008346640455, 40.992358024242606],
                    [-93.90737611136569, 40.99321227969176],
                    [-93.908445279927, 41.0710332380541],
                    [-94.01127360818097, 41.070176651899104],
                    [-94.01008346640455, 40.992358024242606],
                ]
            ],
            "type": "Polygon",
        }
        self.mock_response(responses.GET, json={"geometry": tile_geom})
        self.mock_response(responses.POST, json=features)
        collection = self.instance.paged_search(
            limit=100, dltile="256:16:30.0:15:-11:591"
        )
        assert features == collection.features

    @responses.activate
    def test_paged_search_shapely(self):
        features = [{"id": "foo"}]
        geom = {
            "coordinates": [
                [
                    [-94.01008346640455, 40.992358024242606],
                    [-93.90737611136569, 40.99321227969176],
                    [-93.908445279927, 41.0710332380541],
                    [-94.01127360818097, 41.070176651899104],
                    [-94.01008346640455, 40.992358024242606],
                ]
            ],
            "type": "Polygon",
        }
        shape_geom = shape(geom)
        self.mock_response(responses.GET, json={"geometry": geom})
        self.mock_response(responses.POST, json=features)
        collection = self.instance.paged_search(limit=100, geom=shape_geom)
        assert features == collection.features

    @responses.activate
    def test_search(self):
        features = [{"id": "foo"}, {"id": "bar"}, {"id": "baz"}]
        self.mock_response(responses.POST, json=features)
        collection = self.instance.search(limit=2)
        req = responses.calls[0].request
        assert "storage_state" not in json.loads(req.body.decode("utf-8"))
        assert features[:2] == collection.features

    @responses.activate
    def test_search_storage_state(self):
        features = [{"id": "foo"}, {"id": "bar"}, {"id": "baz"}]
        self.mock_response(responses.POST, json=features)
        collection = self.instance.search(limit=2, storage_state="available")
        assert features[:2] == collection.features
        req = responses.calls[0].request
        assert "storage_state" in json.loads(req.body.decode("utf-8"))

    @responses.activate
    def test_features(self):
        features = [{"id": "foo"}, {"id": "bar"}, {"id": "baz"}]
        self.mock_response(
            responses.POST, json=features[:2], headers={"x-continuation-token": "token"}
        )
        self.mock_response(
            responses.POST,
            json=features[2:],
            headers={"x-continuation-token": "token2"},
        )
        # Note: Unfortunately the client has historically been written in such a way that it always
        # expects a token header, even if the end of the search was reached, so an extra request
        # with 0 results happens in practice.
        self.mock_response(
            responses.POST, json=[], headers={"x-continuation-token": "token3"}
        )
        assert features == list(self.instance.features())
        req = responses.calls[0].request
        assert "storage_state" not in json.loads(req.body.decode("utf-8"))

    @responses.activate
    def test_summary_default(self):
        summary = {"count": 42}
        self.mock_response(responses.POST, json=summary)
        assert summary == self.instance.summary()
        req = responses.calls[0].request
        assert "storage_state" not in json.loads(req.body.decode("utf-8"))

    @responses.activate
    def test_summary_storage_state(self):
        summary = {"count": 42}
        self.mock_response(responses.POST, json=summary)
        assert summary == self.instance.summary(storage_state="available")
        expected_req = {"date": "acquired", "storage_state": "available"}
        req = responses.calls[0].request
        assert json.loads(req.body.decode("utf-8")) == expected_req

    @responses.activate
    def test_summary_dltile(self):
        summary = {"count": 42}
        tile_geom = {
            "coordinates": [
                [
                    [-94.01008346640455, 40.992358024242606],
                    [-93.90737611136569, 40.99321227969176],
                    [-93.908445279927, 41.0710332380541],
                    [-94.01127360818097, 41.070176651899104],
                    [-94.01008346640455, 40.992358024242606],
                ]
            ],
            "type": "Polygon",
        }
        self.mock_response(responses.GET, json={"geometry": tile_geom})
        self.mock_response(responses.POST, json=summary)
        assert summary == self.instance.summary(dltile="256:16:30.0:15:-11:591")

    @responses.activate
    def test_summary_shapely(self):
        summary = {"count": 42}
        geom = {
            "coordinates": [
                [
                    [-94.01008346640455, 40.992358024242606],
                    [-93.90737611136569, 40.99321227969176],
                    [-93.908445279927, 41.0710332380541],
                    [-94.01127360818097, 41.070176651899104],
                    [-94.01008346640455, 40.992358024242606],
                ]
            ],
            "type": "Polygon",
        }
        shape_geom = shape(geom)
        self.mock_response(responses.GET, json={"geometry": geom})
        self.mock_response(responses.POST, json=summary)
        assert summary == self.instance.summary(geom=shape_geom)


if __name__ == "__main__":
    unittest.main()
