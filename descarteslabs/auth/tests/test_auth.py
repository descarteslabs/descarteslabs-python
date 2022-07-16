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
import datetime
import json
import pytest
import unittest
import warnings
import os
import tempfile

import responses
from .. import auth as auth_module
from ..auth import Auth
from descarteslabs.exceptions import AuthError
from mock import patch


def token_response_callback(request):
    body = request.body
    if not isinstance(body, str):
        body = body.decode("utf-8")

    data = json.loads(body)

    required_fields = ["client_id", "grant_type", "refresh_token"]
    legacy_required_fields = ["api_type", "target"]

    if not all(field in data for field in required_fields):
        return 400, {"Content-Type": "application/json"}, json.dumps("missing fields")

    if data["grant_type"] == "urn:ietf:params:oauth:grant-type:jwt-bearer" and all(
        field in data for field in legacy_required_fields
    ):
        return (
            200,
            {"Content-Type": "application/json"},
            json.dumps(dict(id_token="id_token")),
        )

    if data["grant_type"] == "refresh_token" and all(
        field not in data for field in legacy_required_fields
    ):
        return (
            200,
            {"Content-Type": "application/json"},
            json.dumps(dict(access_token="access_token", id_token="id_token")),
        )
    return 400, {"Content-Type": "application/json"}, json.dumps(data)


def to_bytes(s):
    if isinstance(s, str):
        s = s.encode("utf-8")
    return s


class TestAuth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.env = dict(os.environ)
        os.environ.clear()

        # Make sure we're not picking up credentials from anywhere
        auth_module.DEFAULT_TOKEN_INFO_DIR = "/tmp"
        auth_module.DEFAULT_TOKEN_INFO_PATH = "/dev/null"

    @classmethod
    def tearDownClass(cls):
        os.environ.update(cls.env)

    def tearDown(self):
        warnings.resetwarnings()

    def test_auth_client_refresh_match(self):
        with warnings.catch_warnings(record=True) as caught_warnings:
            auth = Auth(
                client_id="client_id",
                client_secret="secret",
                refresh_token="mismatched_refresh_token",
            )
            assert "mismatched_refresh_token" == auth.refresh_token
            assert "mismatched_refresh_token" == auth.client_secret

            assert len(caught_warnings) == 1
            assert caught_warnings[0].category == UserWarning
            assert "token mismatch" in str(caught_warnings[0].message)

    @responses.activate
    def test_get_token(self):
        responses.add(
            responses.POST,
            "https://accounts.descarteslabs.com/token",
            json=dict(access_token="access_token"),
            status=200,
        )
        auth = Auth(client_secret="client_secret", client_id="client_id")
        auth._get_token()

        assert "access_token" == auth._token

    @responses.activate
    def test_get_token_legacy(self):
        responses.add(
            responses.POST,
            "https://accounts.descarteslabs.com/token",
            json=dict(id_token="id_token"),
            status=200,
        )
        auth = Auth(client_secret="client_secret", client_id="client_id")
        auth._get_token()

        assert "id_token" == auth._token

    @patch.object(Auth, "payload", new=dict(sub="asdf"))
    def test_get_namespace(self):
        auth = Auth(client_secret="client_secret", client_id="client_id")
        assert auth.namespace == "3da541559918a808c2402bba5012f6c60b27661c"

    def test_init_token_no_path(self):
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=9999999999, aud="foo")), "sig"]
            )
        )
        auth = Auth(jwt_token=token, client_id="foo")
        assert token == auth._token

    @responses.activate
    def test_get_token_schema_internal_only(self):
        responses.add_callback(
            responses.POST,
            "https://accounts.descarteslabs.com/token",
            callback=token_response_callback,
        )
        auth = Auth(refresh_token="refresh_token", client_id="client_id")
        auth._get_token()

        assert "access_token" == auth._token

        auth = Auth(client_secret="refresh_token", client_id="client_id")
        auth._get_token()

        assert "access_token" == auth._token

    @responses.activate
    def test_get_token_schema_legacy_internal_only(self):
        responses.add_callback(
            responses.POST,
            "https://accounts.descarteslabs.com/token",
            callback=token_response_callback,
        )
        auth = Auth(
            client_secret="client_secret",
            client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
        )
        auth._get_token()
        assert "id_token" == auth._token

    @patch.object(Auth, "_get_token")
    def test_token(self, _get_token):
        auth = Auth(
            client_secret="client_secret",
            client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
        )
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in [
                    "header",
                    json.dumps(
                        dict(exp=9999999999, aud="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c")
                    ),
                    "sig",
                ]
            )
        )
        auth._token = token

        assert auth.token == token
        _get_token.assert_not_called()

    @patch.object(Auth, "_get_token")
    def test_token_expired(self, _get_token):
        auth = Auth(
            client_secret="client_secret",
            client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
        )
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=0)), "sig"]
            )
        )
        auth._token = token

        assert auth.token == token
        _get_token.assert_called_once()

    @patch.object(Auth, "_get_token", side_effect=AuthError("error"))
    def test_token_expired_autherror(self, _get_token):
        auth = Auth(
            client_secret="client_secret",
            client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
        )
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=0)), "sig"]
            )
        )
        auth._token = token

        with pytest.raises(AuthError):
            auth.token
        _get_token.assert_called_once()

    @patch.object(Auth, "_get_token", side_effect=AuthError("error"))
    def test_token_in_leeway_autherror(self, _get_token):
        auth = Auth(
            client_secret="client_secret",
            client_id="ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
        )
        exp = (
            datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)
        ).total_seconds() + auth.leeway / 2
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=exp)), "sig"]
            )
        )
        auth._token = token

        assert auth.token == token
        _get_token.assert_called_once()

    def test_auth_init_env_vars(self):
        warnings.simplefilter("ignore")

        environ = dict(
            CLIENT_SECRET="secret_bar",
            CLIENT_ID="id_bar",
            DESCARTESLABS_CLIENT_SECRET="secret_foo",
            DESCARTESLABS_CLIENT_ID="id_foo",
            DESCARTESLABS_REFRESH_TOKEN="refresh_foo",
        )

        # should work with direct var
        with patch.object(auth_module.os, "environ", environ):
            auth = Auth(
                client_id="client_id",
                client_secret="client_secret",
                refresh_token="client_secret",
            )
            assert auth.client_secret == "client_secret"
            assert auth.client_id == "client_id"

        # should work with namespaced env vars
        with patch.object(auth_module.os, "environ", environ):
            auth = Auth()
            # when refresh_token and client_secret do not match,
            # the Auth implementation sets both to the value of
            # refresh_token
            assert auth.client_secret == environ.get("DESCARTESLABS_REFRESH_TOKEN")
            assert auth.client_id == environ.get("DESCARTESLABS_CLIENT_ID")

        # remove the namespaced ones, except the refresh token because
        # Auth does not recognize a REFRESH_TOKEN environment variable
        # and removing it from the dictionary would result in non-deterministic
        # results based on the token_info.json file on the test runner disk
        environ.pop("DESCARTESLABS_CLIENT_SECRET")
        environ.pop("DESCARTESLABS_CLIENT_ID")

        # should fallback to legacy env vars
        with patch.object(auth_module.os, "environ", environ):
            auth = Auth()
            assert auth.client_secret == environ.get("DESCARTESLABS_REFRESH_TOKEN")
            assert auth.client_id == environ.get("CLIENT_ID")

    def test_set_token(self):
        environ = dict(DESCARTESLABS_TOKEN="token")

        with patch.object(auth_module.os, "environ", environ):
            with self.assertRaises(AuthError):
                auth = Auth()
                auth.payload

        with self.assertRaises(AuthError):
            auth = Auth(jwt_token="token")
            auth.payload

    def test_set_token_info_path(self):
        environ = dict(DESCARTESLABS_TOKEN_INFO_PATH="token_info_path")

        with patch.object(auth_module.os, "environ", environ):
            with self.assertRaises(AuthError):
                auth = Auth()
                assert auth.token_info_path == "token_info_path"
                auth.payload

        with patch.object(auth_module.os, "environ", dict()):
            with self.assertRaises(AuthError):
                auth = Auth(token_info_path="token_info_path")
                assert auth.token_info_path == "token_info_path"
                auth.payload

    def test_cache_jwt_token(self):
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=9999999999, aud="foo")), "sig"]
            )
        ).decode()
        with patch.object(auth_module, "DEFAULT_TOKEN_INFO_DIR", "/tmp"):
            # This instance should write out the jwt token to /tmp/...
            Auth(client_id="foo", client_secret="bar", jwt_token=token)
            # This instance should read it back in
            a = Auth(client_id="foo", client_secret="bar")
            assert a._token == token

    def test_clear_cached_jwt_token_expired(self):
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=0, aud="foo")), "sig"]
            )
        ).decode()
        with patch.object(auth_module, "DEFAULT_TOKEN_INFO_DIR", "/tmp"):
            # This instance should write out the jwt token to /tmp/...
            Auth(client_id="foo", client_secret="bar", jwt_token=token)
            # This instance should clear it because the token is expired
            a = Auth(client_id="foo", client_secret="bar")
            assert a._token is None

    def test_clear_cached_jwt_token_different_client(self):
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=9999999999, aud="foo")), "sig"]
            )
        ).decode()
        with patch.object(auth_module, "DEFAULT_TOKEN_INFO_DIR", "/tmp"):
            # This instance should write out the jwt token to /tmp/...
            Auth(client_id="foo", client_secret="bar", jwt_token=token)
            # This instance should clear it because the client_id differs
            a = Auth(client_id="bar", client_secret="bar")
            assert a._token is None

    def test_clear_cached_jwt_token_different_secret(self):
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=9999999999, aud="foo")), "sig"]
            )
        ).decode()
        with patch.object(auth_module, "DEFAULT_TOKEN_INFO_DIR", "/tmp"):
            # This instance should write out the jwt token to /tmp/...
            Auth(client_id="foo", client_secret="bar", jwt_token=token)
            # This instance should clear it because the client_secret differs
            a = Auth(client_id="foo", client_secret="foo")
            assert a._token is None

    def test_no_valid_auth_info(self):
        with warnings.catch_warnings(record=True) as caught_warnings:
            Auth(client_id="client_id")
            assert len(caught_warnings) == 1
            assert caught_warnings[0].category == UserWarning
            assert "No valid authentication info found" in str(
                caught_warnings[0].message
            )

    def test_token_info_file(self):
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=9999999999, aud="foo")), "sig"]
            )
        ).decode()
        with tempfile.NamedTemporaryFile(delete=False) as token_info_file:
            token_info_file.write(
                json.dumps(
                    {
                        "client_id": "foo",
                        "refresh_token": "bar",
                        "jwt_token": token,
                    }
                ).encode()
            )
            token_info_file.close()

            # This instance should read in that token
            a = Auth(
                client_id="foo",
                client_secret="bar",
                token_info_path=token_info_file.name,
            )
            assert a._token == token

    def test_clear_token_info_file(self):
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=9999999999, aud="foo")), "sig"]
            )
        ).decode()
        with tempfile.NamedTemporaryFile(delete=False) as token_info_file:
            token_info_file.write(
                json.dumps(
                    {
                        "client_id": "foo",
                        "refresh_token": "bar",
                        "jwt_token": token,
                    }
                ).encode()
            )
            token_info_file.close()

            # This instance should not read in that token
            a = Auth(
                client_id="bar",
                client_secret="bar",
                token_info_path=token_info_file.name,
            )
            assert a._token is None

    @responses.activate
    def test_write_token_info_file(self):
        token = b".".join(
            (
                base64.b64encode(to_bytes(p))
                for p in ["header", json.dumps(dict(exp=9999999999, aud="foo")), "sig"]
            )
        ).decode()
        responses.add(
            responses.POST,
            "https://accounts.descarteslabs.com/token",
            json=dict(client_id="foo", client_secret="bar", id_token=token),
            status=200,
        )

        with tempfile.NamedTemporaryFile(delete=False) as token_info_file:
            token_info_file.close()

            # This instance should write the token
            a = Auth(
                client_id="foo",
                client_secret="bar",
                token_info_path=token_info_file.name,
            )
            a.token

            # This instance should read the token
            a = Auth(
                client_id="foo",
                client_secret="bar",
                token_info_path=token_info_file.name,
            )
            assert a._token == token

            # This instance should clear the token because the client_id doesn't match
            a = Auth(
                client_id="bar",
                client_secret="bar",
                token_info_path=token_info_file.name,
            )
            assert a._token is None


if __name__ == "__main__":
    unittest.main()
