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

import json
import six
import unittest

import responses
from mock import patch

from descarteslabs.client.auth import Auth


def token_response_callback(request):
    body = request.body
    if not isinstance(body, six.text_type):
        body = body.decode('utf-8')

    data = json.loads(body)

    required_fields = ['client_id', 'grant_type', 'refresh_token']
    legacy_required_fields = ["api_type", "target"]

    if not all(field in data for field in required_fields):
        return 400, {"Content-Type": "application/json"}, json.dumps("missing fields")

    if data['grant_type'] == "urn:ietf:params:oauth:grant-type:jwt-bearer" \
            and all(field in data for field in legacy_required_fields):
        return 200, {"Content-Type": "application/json"}, json.dumps(dict(id_token="id_token"))

    if data['grant_type'] == "refresh_token" \
            and all(field not in data for field in legacy_required_fields):
        return 200, {"Content-Type": "application/json"}, json.dumps(dict(access_token="access_token",
                                                                          id_token="id_token"))
    return 400, {"Content-Type": "application/json"}, json.dumps(data)


class TestAuth(unittest.TestCase):
    @responses.activate
    def test_get_token(self):
        responses.add(responses.POST, 'https://accounts.descarteslabs.com/token',
                      json=dict(access_token="access_token"),
                      status=200)
        auth = Auth(token_info_path=None, client_secret="client_secret", client_id="client_id")
        auth._get_token()

        self.assertEqual("access_token", auth._token)

    @responses.activate
    def test_get_token_legacy(self):
        responses.add(responses.POST, 'https://accounts.descarteslabs.com/token',
                      json=dict(id_token="id_token"), status=200)
        auth = Auth(token_info_path=None, client_secret="client_secret", client_id="client_id")
        auth._get_token()

        self.assertEqual("id_token", auth._token)

    @patch("descarteslabs.client.auth.Auth.payload", new=dict(sub="asdf"))
    def test_get_namespace(self):
        auth = Auth(token_info_path=None, client_secret="client_secret", client_id="client_id")
        self.assertEqual(auth.namespace, "3da541559918a808c2402bba5012f6c60b27661c")

    def test_init_token_no_path(self):
        auth = Auth(jwt_token="token", token_info_path=None, client_id="foo")
        self.assertEqual("token", auth._token)

    @responses.activate
    def test_get_token_schema_internal_only(self):
        responses.add_callback(responses.POST, 'https://accounts.descarteslabs.com/token',
                               callback=token_response_callback)
        auth = Auth(token_info_path=None, refresh_token="refresh_token", client_id="client_id")
        auth._get_token()

        self.assertEqual("access_token", auth._token)

        auth = Auth(token_info_path=None, client_secret="refresh_token", client_id="client_id")
        auth._get_token()

        self.assertEqual("access_token", auth._token)

    @responses.activate
    def test_get_token_schema_legacy_internal_only(self):
        responses.add_callback(responses.POST, 'https://accounts.descarteslabs.com/token',
                               callback=token_response_callback)
        auth = Auth(token_info_path=None, client_secret="client_secret", client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c")
        auth._get_token()
        self.assertEqual("id_token", auth._token)


if __name__ == '__main__':
    unittest.main()
