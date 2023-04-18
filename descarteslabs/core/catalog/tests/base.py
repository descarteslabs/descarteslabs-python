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

import base64
import json
import re
import time
import unittest

import responses
from descarteslabs.auth import Auth

from ..catalog_client import CatalogClient


class ClientTestCase(unittest.TestCase):
    not_found_json = {
        "errors": [
            {
                "detail": "Object not found: foo",
                "status": "404",
                "title": "Object not found",
            }
        ],
        "jsonapi": {"version": "1.0"},
    }

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

        self.url = "https://example.com/catalog/v2"
        self.client = CatalogClient(
            url=self.url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.match_url = re.compile(self.url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)

    def get_request(self, index):
        r = responses.calls[index].request
        if r.body is None:
            r.body = ""
        elif isinstance(r.body, bytes):
            r.body = r.body.decode()
        return r

    def get_request_body(self, index):
        body = responses.calls[index].request.body
        if body is None:
            body = ""
        elif isinstance(body, bytes):
            body = body.decode()
        return json.loads(body)
