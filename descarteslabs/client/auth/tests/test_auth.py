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

import base64
import datetime
import json
import unittest

import six

import responses
from descarteslabs.client.auth import Auth
from descarteslabs.client.exceptions import AuthError
from mock import patch


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


def to_bytes(s):
    if isinstance(s, six.text_type):
        s = s.encode('utf-8')
    return s


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

    @patch("descarteslabs.client.auth.Auth._get_token")
    def test_token(self, _get_token):
        auth = Auth(token_info_path=None, client_secret="client_secret", client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c")
        token = b".".join((base64.b64encode(to_bytes(p)) for p in ["header", json.dumps(dict(exp=9999999999)), "sig"]))
        auth._token = token

        self.assertEqual(auth.token, token)
        _get_token.assert_not_called()

    @patch("descarteslabs.client.auth.Auth._get_token")
    def test_token_expired(self, _get_token):
        auth = Auth(token_info_path=None, client_secret="client_secret", client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c")
        token = b".".join((base64.b64encode(to_bytes(p)) for p in ["header", json.dumps(dict(exp=0)), "sig"]))
        auth._token = token

        self.assertEqual(auth.token, token)
        _get_token.assert_called_once()

    @patch("descarteslabs.client.auth.Auth._get_token", side_effect=AuthError('error'))
    def test_token_expired_autherror(self, _get_token):
        auth = Auth(token_info_path=None, client_secret="client_secret", client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c")
        token = b".".join((base64.b64encode(to_bytes(p)) for p in ["header", json.dumps(dict(exp=0)), "sig"]))
        auth._token = token

        with self.assertRaises(AuthError):
            auth.token
        _get_token.assert_called_once()

    @patch("descarteslabs.client.auth.Auth._get_token", side_effect=AuthError('error'))
    def test_token_in_leeway_autherror(self, _get_token):
        auth = Auth(token_info_path=None, client_secret="client_secret", client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c")
        exp = (datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds() + auth.leeway / 2
        token = b".".join((base64.b64encode(to_bytes(p)) for p in ["header", json.dumps(dict(exp=exp)), "sig"]))
        auth._token = token

        self.assertEqual(auth.token, token)
        _get_token.assert_called_once()

    def test_auth_init_env_vars(self):
        environ = dict(CLIENT_SECRET='secret_bar',
                       CLIENT_ID="id_bar",
                       DESCARTESLABS_CLIENT_SECRET='secret_foo',
                       DESCARTESLABS_CLIENT_ID="id_foo")

        # should work with direct var
        with patch.dict('descarteslabs.client.auth.auth.os.environ', environ):
            auth = Auth(client_id='client_id', client_secret='client_secret', jwt_token='jwt_token')
            self.assertEqual(auth.client_secret, 'client_secret')
            self.assertEqual(auth.client_id, 'client_id')

        # should work with namespaced env vars
        with patch.dict('descarteslabs.client.auth.auth.os.environ', environ):
            auth = Auth()
            self.assertEqual(auth.client_secret, environ.get('DESCARTESLABS_CLIENT_SECRET'))
            self.assertEqual(auth.client_id, environ.get('DESCARTESLABS_CLIENT_ID'))

        # remove the namespaced ones
        environ.pop('DESCARTESLABS_CLIENT_SECRET')
        environ.pop('DESCARTESLABS_CLIENT_ID')

        # should fallback to legacy env vars
        with patch.dict('descarteslabs.client.auth.auth.os.environ', environ):
            auth = Auth()
            self.assertEqual(auth.client_secret, environ.get('CLIENT_SECRET'))
            self.assertEqual(auth.client_id, environ.get('CLIENT_ID'))


if __name__ == '__main__':
    unittest.main()
