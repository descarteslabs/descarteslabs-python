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
import io
import json
import re
import time
import unittest

import pytest
import responses
from descarteslabs.auth import Auth
from descarteslabs.exceptions import BadRequestError, NotFoundError
from shapely.geometry import shape

from .. import Vector


class ClientTestCase(unittest.TestCase):
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

        self.url = "http://example.vector.com"
        self.gcs_url = "http://example.gcs.com"

        self.client = Vector(
            url=self.url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )

        self.match_url = re.compile(self.url)
        self.match_gcs_url = re.compile(self.gcs_url)

        self.attrs = {
            "geometry": {
                "coordinates": [
                    [
                        [-113.40087890624999, 40.069664523297774],
                        [-111.434326171875, 40.069664523297774],
                        [-111.434326171875, 41.918628865183045],
                        [-113.40087890624999, 41.918628865183045],
                        [-113.40087890624999, 40.069664523297774],
                    ]
                ],
                "type": "Polygon",
            },
            "properties": {"baz": 1.0, "foo": "bar"},
        }

        self.product_response = {
            "data": {
                "attributes": {
                    "description": "bar",
                    "name": "new-test-product",
                    "owners": [
                        "org:descarteslabs",
                        "user:3d7bf4b0b1f4e6283e5cbeaadddbc6de6f16dea1",
                    ],
                    "readers": [],
                    "title": "Test Product",
                    "writers": [],
                },
                "id": "2b4552ff4b8a4bb5bb278c94005db50",
                "meta": {"created": "2018-12-27T17:01:16.197369"},
                "type": "product",
            }
        }

        self.status_response = {
            "data": {
                "attributes": {
                    "created": "2019-01-03T20:07:51.720000+00:00",
                    "started": "2019-01-03T20:07:51.903000+00:00",
                    "status": "RUNNING",
                },
                "id": "c589d688-3230-4caf-9f9d-18854f71e91d",
                "type": "copy_query",
            }
        }

        self.feature_response = {
            "data": [
                {
                    "attributes": {
                        "created": "2019-03-28T23:08:24.991729+00:00",
                        "geometry": {
                            "coordinates": [
                                [[-95, 42], [-95, 41], [-93, 40], [-93, 42], [-95, 42]]
                            ],
                            "type": "Polygon",
                        },
                        "properties": {},
                    },
                    "id": "7d724ae48d1fab595bc95b6091b005c920327",
                    "type": "feature",
                }
            ]
        }

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)

    def mock_gcs(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_gcs_url, json=json, status=status, **kwargs)


class VectorsTest(ClientTestCase):
    @responses.activate
    def test_upload_bytesio(self):
        self.mock_response(responses.POST, {"upload_id": "xyz", "url": self.gcs_url})

        self.mock_gcs(responses.PUT, {})

        s = io.BytesIO()

        for i in range(10):
            s.write(b"{")
            s.write("{}".format(self.attrs).encode("utf-8"))
            s.write(b"}\n")

        self.client.upload_features(s, "test")

    @responses.activate
    def test_upload_stringio(self):
        self.mock_response(responses.POST, {"upload_id": "xyz", "url": self.gcs_url})

        self.mock_gcs(responses.PUT, {})

        s = io.StringIO()

        for i in range(10):
            s.write("{")
            s.write("{}".format(self.attrs))
            s.write("}\n")

        self.client.upload_features(s, "test")

    @responses.activate
    def test_bad_upload(self):
        self.mock_response(responses.POST, {"upload_id": "xyz", "url": self.gcs_url})

        self.mock_gcs(responses.PUT, {})

        s = ""

        for i in range(10):
            s += "{"
            s += "{}".format(self.attrs)
            s += "}\n"

        with pytest.raises(Exception):
            self.client.upload_features(s, "test")

    @responses.activate
    def test_search_features(self):
        self.mock_response(
            responses.POST,
            {"meta": {"continuation_token": 1, "total_results": 10}, "data": []},
        )

        self.client.search_features("test-product-id", geometry=self.attrs["geometry"])

        assert len(responses.calls) == 1
        request = responses.calls[0].request
        assert (
            json.loads(request.body.decode("utf-8"))["geometry"]
            == self.attrs["geometry"]
        )

    @responses.activate
    def test_search_features_shapely(self):
        self.mock_response(
            responses.POST,
            {"meta": {"continuation_token": 1, "total_results": 10}, "data": []},
        )

        self.client.search_features(
            "test-product-id", geometry=shape(self.attrs["geometry"])
        )

        assert len(responses.calls) == 1
        request = responses.calls[0].request
        assert (
            json.loads(request.body.decode("utf-8"))["geometry"]
            == self.attrs["geometry"]
        )

    @responses.activate
    def test_create_product_from_query(self):
        self.mock_response(responses.POST, self.product_response, status=201)

        r = self.client.create_product_from_query("foo", "Foo", "Foo is a bar", "baz")

        assert "2b4552ff4b8a4bb5bb278c94005db50" == r.data.id

    @responses.activate
    def test_create_product_from_query_exception(self):
        self.mock_response(responses.POST, {}, status=400)

        with pytest.raises(BadRequestError):
            self.client.create_product_from_query("foo", "Foo", "Foo is a bar", "baz")

    @responses.activate
    def test_get_product_from_query_status(self):
        self.mock_response(responses.GET, self.status_response, status=200)

        r = self.client.get_product_from_query_status("2b4552ff4b8a4bb5bb278c94005db50")

        assert r.data.id == "c589d688-3230-4caf-9f9d-18854f71e91d"

    @responses.activate
    def test_get_product_from_query_status_not_found(self):
        self.mock_response(responses.GET, self.status_response, status=404)

        with pytest.raises(NotFoundError):
            self.client.get_product_from_query_status("2b4552ff4b8a4bb5bb278c94005db50")

    @responses.activate
    def delete_features_from_query(self):
        self.mock_response(responses.DELETE, self.product_response, status=202)

        r = self.client.delete_features_from_query("foo", "bar", "baz")

        assert "2b4552ff4b8a4bb5bb278c94005db50" == r.data.id

    @responses.activate
    def delete_features_from_query_bad_request(self):
        self.mock_response(responses.DELETE, self.product_response, status=400)

        with pytest.raises(BadRequestError):
            self.client.delete_features_from_query("foo", "bar", "baz")

    @responses.activate
    def test_get_delete_features_status(self):
        self.mock_response(responses.GET, self.status_response, status=200)

        r = self.client.get_product_from_query_status("2b4552ff4b8a4bb5bb278c94005db50")
        assert r.data.id == "c589d688-3230-4caf-9f9d-18854f71e91d"

    @responses.activate
    def test_get_delete_features_status_not_found(self):
        self.mock_response(responses.GET, self.status_response, status=404)

        with pytest.raises(NotFoundError):
            self.client.get_product_from_query_status("2b4552ff4b8a4bb5bb278c94005db50")

    @responses.activate
    def test_create_feature_correct_wo(self):
        self.mock_response(responses.POST, self.feature_response, status=200)
        non_ccw = {
            "type": "Polygon",
            "coordinates": [[[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]],
        }
        expected_req_body = {
            "data": {
                "attributes": {
                    "fix_geometry": "fix",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]
                        ],
                    },
                    "properties": None,
                },
                "type": "feature",
            }
        }

        self.client.create_feature(
            "2b4552ff4b8a4bb5bb278c94005db50", non_ccw, fix_geometry="fix"
        )

        request = responses.calls[0].request
        assert json.loads(request.body.decode("utf-8")) == expected_req_body

    @responses.activate
    def test_create_feature_error(self):
        self.mock_response(responses.POST, self.feature_response, status=400)
        non_ccw = {
            "type": "Polygon",
            "coordinates": [[[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]],
        }

        with pytest.raises(BadRequestError):
            self.client.create_feature(
                "2b4552ff4b8a4bb5bb278c94005db50", non_ccw, fix_geometry="reject"
            )

        expected_req_body = {
            "data": {
                "type": "feature",
                "attributes": {
                    "fix_geometry": "reject",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]
                        ],
                    },
                    "properties": None,
                },
            }
        }

        request = responses.calls[0].request
        assert json.loads(request.body.decode("utf-8")) == expected_req_body

    @responses.activate
    def test_create_features_correct_wo(self):
        self.mock_response(responses.POST, self.feature_response, status=200)
        non_ccw_list = [
            {
                "type": "Polygon",
                "coordinates": [
                    [[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]
                ],
            },
            {
                "type": "MultiPolygon",
                "coordinates": [
                    [[[-95, 42], [-95, 41], [-93, 40], [-93, 42], [-95, 42]]],
                    [[[-91, 44], [-92, 43], [-91, 42], [-89, 43], [-91, 44]]],
                    [
                        [
                            [-97, 44],
                            [-96, 42],
                            [-95, 43],
                            [-94, 43],
                            [-95, 44],
                            [-97, 44],
                        ]
                    ],
                ],
            },
            {
                "type": "MultiLineString",
                "coordinates": [
                    [[-91, 44], [-89, 43], [-91, 42], [-92, 43]],
                    [[-95, 42], [-93, 42], [-93, 40], [-95, 41]],
                ],
            },
        ]

        self.client.create_features(
            "2b4552ff4b8a4bb5bb278c94005db50", non_ccw_list, fix_geometry="fix"
        )
        expected_req_body = {
            "data": [
                {
                    "type": "feature",
                    "attributes": {
                        "fix_geometry": "fix",
                        "type": "Polygon",
                        "coordinates": [
                            [[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]
                        ],
                    },
                },
                {
                    "type": "feature",
                    "attributes": {
                        "fix_geometry": "fix",
                        "type": "MultiPolygon",
                        "coordinates": [
                            [[[-95, 42], [-95, 41], [-93, 40], [-93, 42], [-95, 42]]],
                            [[[-91, 44], [-92, 43], [-91, 42], [-89, 43], [-91, 44]]],
                            [
                                [
                                    [-97, 44],
                                    [-96, 42],
                                    [-95, 43],
                                    [-94, 43],
                                    [-95, 44],
                                    [-97, 44],
                                ]
                            ],
                        ],
                    },
                },
                {
                    "type": "feature",
                    "attributes": {
                        "fix_geometry": "fix",
                        "type": "MultiLineString",
                        "coordinates": [
                            [[-91, 44], [-89, 43], [-91, 42], [-92, 43]],
                            [[-95, 42], [-93, 42], [-93, 40], [-95, 41]],
                        ],
                    },
                },
            ]
        }

        request = responses.calls[0].request
        assert json.loads(request.body.decode("utf-8")) == expected_req_body

    @responses.activate
    def test_create_features_error(self):
        self.mock_response(responses.POST, self.feature_response, status=400)
        non_ccw_list = [
            {
                "type": "Polygon",
                "coordinates": [
                    [[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]
                ],
            },
            {
                "type": "MultiPolygon",
                "coordinates": [
                    [[[-95, 42], [-95, 41], [-93, 40], [-93, 42], [-95, 42]]],
                    [[[-91, 44], [-92, 43], [-91, 42], [-89, 43], [-91, 44]]],
                    [
                        [
                            [-97, 44],
                            [-96, 42],
                            [-95, 43],
                            [-94, 43],
                            [-95, 44],
                            [-97, 44],
                        ]
                    ],
                ],
            },
            {
                "type": "MultiLineString",
                "coordinates": [
                    [[-91, 44], [-89, 43], [-91, 42], [-92, 43]],
                    [[-95, 42], [-93, 42], [-93, 40], [-95, 41]],
                ],
            },
        ]

        with pytest.raises(BadRequestError):
            self.client.create_features(
                "2b4552ff4b8a4bb5bb278c94005db50", non_ccw_list, fix_geometry="reject"
            )

        expected_req_body = {
            "data": [
                {
                    "type": "feature",
                    "attributes": {
                        "fix_geometry": "reject",
                        "type": "Polygon",
                        "coordinates": [
                            [[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]
                        ],
                    },
                },
                {
                    "type": "feature",
                    "attributes": {
                        "fix_geometry": "reject",
                        "type": "MultiPolygon",
                        "coordinates": [
                            [[[-95, 42], [-95, 41], [-93, 40], [-93, 42], [-95, 42]]],
                            [[[-91, 44], [-92, 43], [-91, 42], [-89, 43], [-91, 44]]],
                            [
                                [
                                    [-97, 44],
                                    [-96, 42],
                                    [-95, 43],
                                    [-94, 43],
                                    [-95, 44],
                                    [-97, 44],
                                ]
                            ],
                        ],
                    },
                },
                {
                    "type": "feature",
                    "attributes": {
                        "fix_geometry": "reject",
                        "type": "MultiLineString",
                        "coordinates": [
                            [[-91, 44], [-89, 43], [-91, 42], [-92, 43]],
                            [[-95, 42], [-93, 42], [-93, 40], [-95, 41]],
                        ],
                    },
                },
            ]
        }
        request = responses.calls[0].request
        assert json.loads(request.body.decode("utf-8")) == expected_req_body


if __name__ == "__main__":
    unittest.main()
