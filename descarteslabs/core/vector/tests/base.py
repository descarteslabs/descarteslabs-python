# Copyright 2018-2024 Descartes Labs.

import base64
import json
import json as jsonlib
import time
import urllib.parse
import uuid
from datetime import datetime, timezone
from unittest import TestCase

import geopandas as gpd
import pandas as pd

import responses
from requests import PreparedRequest

from descarteslabs.auth import Auth
from descarteslabs.config import get_settings

from ..vector_client import VectorClient


def make_uuid():
    return str(uuid.uuid4())


class BaseTestCase(TestCase):
    vector_url = get_settings().vector_url

    spatial_test_dataframe = gpd.GeoDataFrame.from_features(
        [
            {
                "geometry": {"coordinates": [4.473067, 52.119339], "type": "Point"},
                "properties": {"color": "red", "num": 1},
                "type": "Feature",
            },
            {
                "geometry": {"coordinates": [4.686934, 52.116113], "type": "Point"},
                "properties": {"color": "blue", "num": 4},
                "type": "Feature",
            },
        ],
        crs="EPSG:4326",
    )

    nonspatial_test_dataframe = pd.DataFrame(
        [
            {"color": "red", "num": 1},
            {"color": "blue", "num": 4},
        ],
    )

    spatial_product_id = "some-org:snappy-spatial-vector-product"

    nonspatial_product_id = "some-org:snappy-nonspatial-vector-product"

    spatial_feature_id = "snappy-spatial-vector-feature"

    nonspatial_feature_id = "snappy-nonspatial-vector-feature"

    def setUp(self):
        responses.mock.assert_all_requests_are_fired = True
        self.now = datetime.now(timezone.utc).replace(tzinfo=None)

        payload = (
            base64.b64encode(
                json.dumps(
                    {
                        "aud": "client-id",
                        "exp": time.time() + 3600,
                    }
                ).encode()
            )
            .decode()
            .strip("=")
        )
        token = f"header.{payload}.signature"
        auth = Auth(jwt_token=token, token_info_path=None)
        VectorClient.set_default_client(VectorClient(auth=auth))

    def tearDown(self):
        responses.mock.assert_all_requests_are_fired = False

    def mock_response(self, method, uri, status=200, **kwargs):
        responses.add(
            method,
            f"{self.vector_url}{uri}",
            status=status,
            **kwargs,
        )

    def assert_url_called(
        self, method, uri, times=1, json=None, body=None, params=None, headers=None
    ):
        if json and body:
            raise ValueError("Using json and body together does not make sense")

        url = f"{self.vector_url}{uri}"
        calls = [call for call in responses.calls if call.request.url.startswith(url)]
        assert calls, f"No requests were made to uri: {uri}"

        data = json or body
        matches = []
        calls_with_data = []
        calls_with_params = set()
        calls_with_headers = set()

        for call in calls:
            request: PreparedRequest = call.request

            if json is not None:
                request_data = jsonlib.loads(request.body)
            else:
                request_data = request.body

            if request_data:
                calls_with_data.append(repr(request.body))

            if params is not None:
                request_params = {}

                for key, value in urllib.parse.parse_qsl(
                    urllib.parse.urlsplit(request.url).query
                ):
                    try:
                        value = jsonlib.loads(value)
                    except jsonlib.JSONDecodeError:
                        value = value

                    if key in request_params:
                        values = request_params[key]

                        if not isinstance(values, list):
                            values = [values]

                        values.append(value)
                    else:
                        values = value

                    request_params[key] = values

                if request_params:
                    calls_with_params.add(jsonlib.dumps(request_params))
            else:
                request_params = None

            if headers is not None:
                request_headers = {}
                for key, value in request.headers.items():
                    if key in headers:
                        request_headers[key] = value
                if request_headers:
                    calls_with_headers.add(jsonlib.dumps(request_headers))
            else:
                request_headers = None

            if (
                (method == request.method)
                and (data is None or request_data == data)
                and (params is None or request_params == params)
                and (headers is None or request_headers == headers)
            ):
                matches.append(call)

        count = len(matches)
        msg = f"Expected {times} calls found {count} for {method} {uri}"

        if data is not None:
            msg += f" with data: {data}"

            if calls_with_data:
                msg += "\n\nData:\n" + "\n".join(calls_with_data)

        if params is not None:
            msg += f" with params: {params}"

            if calls_with_params:
                msg += "\n\nParams:\n" + "\n".join(calls_with_params)

        if headers is not None:
            msg += f" with headers: {headers}"

            if headers:
                msg += "\n\nHeaders:\n" + "\n".join(calls_with_headers)

        assert count == times, msg
