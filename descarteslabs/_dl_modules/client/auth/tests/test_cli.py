# 2018-2020 Descartes Labs.
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

import binascii
import builtins
import json
import os
import sys
import unittest

from descarteslabs.exceptions import AuthError
from ..cli import auth_handler
from .. import cli
from mock import patch


class Args:
    command = "login"


class Input:
    """Emulate `input()` and return the given strings.

    Return the given strings in sequence when an input() statement is executed.
    Return KeyboardError when no strings are left
    """

    def __init__(self, *return_values):
        self._return_values = list(return_values)

    def __call__(self, *args, **kwargs):
        if self._return_values:
            return self._return_values.pop(0)

        raise KeyboardInterrupt()


class Print:
    """Emulate the `print()` statement and check exceptions.

    Compare the given exception when a print() statement is executed.
    When the comparison fails, an assert is raised. Use `None` to indicate no exception.
    When there are no exceptions left to compare, the comparison is skipped.
    """

    EXCEPTION_TYPE = 0
    print = builtins.print

    def __init__(self, *exception_types):
        self._exception_types = list(exception_types)

    def __call__(self, *args, **kwargs):
        if self._exception_types:
            assert sys.exc_info()[self.EXCEPTION_TYPE] == self._exception_types.pop(0)

        self.print(*args, **kwargs)


PAYLOAD = "{'name': 'Some Body', 'groups': ['public'], 'org': 'someorg', 'email': 'some_body@someorg.com', 'email_verified': True, 'iss': 'https://descarteslabs.auth0.com/', 'sub': 'google-oauth2|202801449858648638555', 'aud': 'ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c', 'exp': 1610770917, 'iat': 1610734917, 'azp': 'ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c'}"  # noqa: E501


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
@patch.object(cli, "os")
class TestAuth(unittest.TestCase):
    def setUp(self):
        # Clean up the environment
        CLIENT_ID = "CLIENT_ID"
        CLIENT_SECRET = "CLIENT_SECRET"

        if CLIENT_ID in os.environ:
            self.client_id = os.environ["CLIENT_ID"]
            del os.environ["CLIENT_ID"]

        if CLIENT_SECRET in os.environ:
            self.client_secret = os.environ["CLIENT_SECRET"]
            del os.environ["CLIENT_SECRET"]

    def tearDown(self):
        if hasattr(self, "client_id"):
            os.environ["CLIENT_ID"] = self.client_id

        if hasattr(self, "client_secret"):
            os.environ["CLIENT_SECRET"] = self.client_secret

    # Test simple bad input
    @patch.object(cli, "input", Input("foo", "foo.bar"))
    @patch("builtins.print", Print(None, UnicodeDecodeError, binascii.Error))
    @patch("builtins.open", Open(PAYLOAD))
    def test_invalid_token(self, *mocks):
        auth_handler(Args)

    # Test incorrect json
    @patch.object(cli, "input", Input("VGhpcyBpcyBhIHRlc3Q="))
    @patch("builtins.print", Print(None, json.JSONDecodeError))
    @patch("builtins.open", Open(PAYLOAD))
    def test_invalid_json(self, *mocks):
        auth_handler(Args)

    # Test incorrect character set. Base64encoded CP51932: This is ∞ test
    @patch.object(cli, "input", Input("VGhpcyBpcyCh5yB0ZXN0"))
    @patch("builtins.print", Print(None, UnicodeDecodeError))
    @patch("builtins.open", Open(PAYLOAD))
    def test_invalid_character(self, *mocks):
        auth_handler(Args)

    # Test incomplete json: {"test": "test"}
    @patch.object(cli, "input", Input("eyJ0ZXN0IjogInRlc3QifQ=="))
    @patch("builtins.print", Print(None, None, None))
    @patch("builtins.open", Open(""))
    def test_incomplete_json(self, *mocks):
        with self.assertRaises(AuthError):
            auth_handler(Args)
