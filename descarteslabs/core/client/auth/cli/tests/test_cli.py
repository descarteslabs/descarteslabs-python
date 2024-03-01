# 2018-2023 Descartes Labs.
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
import os
import unittest
from unittest.mock import patch

import click.testing
import responses

from .. import cli


REFRESH = {
    "id": "some id",
    "client_id": "ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
    "name": "API token",
    "revoke_url": "https://iam.descarteslabs.com/auth/credentials/revoke/revoke.me",
    "client_secret": os.environ.get(
        "CLIENT_SECRET", os.environ.get("DESCARTESLABS_CLIENT_SECRET")
    ),
}
REFRESH_TOKEN = base64.urlsafe_b64encode(
    json.dumps(REFRESH, separators=(",", ":")).encode("utf-8")
).decode("utf-8")

PAYLOAD = {
    "name": "Some Body",
    "groups": ["public"],
    "org": "someorg",
    "email": "some_body@someorg.com",
    "email_verified": True,
    "iss": "https://descarteslabs.auth0.com/",
    "sub": "google-oauth2|202801449858648638555",
    "aud": "ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
    "exp": 1610770917,
    "iat": 1610734917,
    "azp": "ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
}
PAYLOAD_JSON = json.dumps(PAYLOAD, separators=(",", ":"))


class Open:
    """Emulate `open()` statement and return pre-defined strings

    When there was no written string, return the initial string.
    Once a string is written, return that instead
    """

    def __init__(self, initial_payload):
        self._initial_payload = initial_payload
        self._payload = ""

    def __call__(self, *args, **kwargs):
        if len(args) > 1 and "w" in args[1]:
            self._payload = ""

        return self

    def __enter__(self, *args):
        return self

    def __exit__(self, *args):
        pass

    def read(self, *args):
        if self._payload:
            print("Returing {}".format(self._payload))
            return self._payload
        else:
            return self._initial_payload

    def write(self, payload):
        self._payload += payload


#
# Note that the `open()` patch cannot be shared across tests.
# Note that the environment must be cleaned in order to get
# expected behavior (i.e. no credentials present).
#
@patch("descarteslabs.auth.auth.makedirs_if_not_exists")
@patch(
    "descarteslabs.auth.auth.get_default_domain",
    return_value="https://descarteslabs.auth0.com",
)
@patch("descarteslabs.auth.auth.DEFAULT_TOKEN_INFO_PATH", None)
class TestAuth(unittest.TestCase):
    def setUp(self):
        self.runner = click.testing.CliRunner()

    @responses.activate
    @patch("builtins.open", Open(PAYLOAD_JSON))
    def test_login(self, *mocks):
        payload = base64.urlsafe_b64encode(PAYLOAD_JSON.encode("utf-8")).decode("utf-8")
        responses.add(
            responses.POST,
            "https://descarteslabs.auth0.com/token",
            json={
                "access_token": f".{payload}.",
            },
        )

        result = self.runner.invoke(cli, ["login"], input=REFRESH_TOKEN + "\n")
        assert result.exit_code == 0
        assert "Welcome, Some Body!" in result.output

    @responses.activate
    @patch("builtins.open", Open(PAYLOAD_JSON))
    def test_payload(self, *mocks):
        payload = base64.urlsafe_b64encode(PAYLOAD_JSON.encode("utf-8")).decode("utf-8")
        responses.add(
            responses.POST,
            "https://descarteslabs.auth0.com/token",
            json={
                "access_token": f".{payload}.",
            },
        )

        result = self.runner.invoke(cli, ["payload"])
        assert result.exit_code == 0
        assert json.loads(result.output) == PAYLOAD
