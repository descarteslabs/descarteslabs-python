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

import unittest
import mock
import os
from io import StringIO
from ..cli import parser, handle
import base64
import json
import pytest


class TestScripts(unittest.TestCase):
    old_token = None
    token_path = os.path.join(
        os.path.expanduser("~"), ".descarteslabs", "token_info.json"
    )

    @classmethod
    def setUpClass(cls):
        client_id = os.environ.get("CLIENT_ID")
        client_secret = os.environ.get("CLIENT_SECRET")
        refresh_token = os.environ.get("DESCARTESLABS_REFRESH_TOKEN")
        if os.path.exists(cls.token_path):
            cls.old_token = json.load(open(cls.token_path))
            client_id = client_id or cls.old_token.get("client_id")
            client_secret = client_secret or cls.old_token.get("client_secret")
            refresh_token = refresh_token or cls.old_token.get("refresh_token")

        cls.token = base64.b64encode(
            json.dumps(
                {
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "refresh_token": refresh_token,
                }
            ).encode("utf-8")
        )

    @classmethod
    def tearDownClass(cls):
        # Put old token back if it existed.
        if cls.old_token:
            with open(cls.token_path, "w+") as f:
                json.dump(cls.old_token, f)

    @pytest.mark.skip("requires creds")
    def test_auth_login(self):
        with mock.patch("descarteslabs.client.auth.cli.input", return_value=self.token):
            handle(parser.parse_args(["auth", "login"]))

        with mock.patch("sys.stdout", new_callable=StringIO) as out:
            handle(parser.parse_args(["auth", "groups"]))
            assert out.getvalue().strip() == '["public"]'

    @pytest.mark.skip("requires creds")
    def test_places_find(self):
        with mock.patch("sys.stdout", new_callable=StringIO) as out:
            handle(parser.parse_args(["places", "find", "iowa"]))
            iowa = [
                {
                    "name": "Iowa",
                    "bbox": [-96.639468, 40.37544, -90.140061, 43.501128],
                    "id": 85688713,
                    "path": "continent:north-america_country:united-states_region:iowa",
                    "slug": "north-america_united-states_iowa",
                    "placetype": "region",
                }
            ]
            assert json.loads(out.getvalue().strip()) == iowa
