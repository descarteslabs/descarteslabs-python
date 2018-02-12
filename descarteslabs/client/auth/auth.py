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

import requests
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import base64
import datetime
import errno
import json
import os
import random
import six
import stat
from hashlib import sha1
import warnings

from descarteslabs.client.exceptions import AuthError, OauthError

DEFAULT_TOKEN_INFO_PATH = os.path.join(
    os.path.expanduser("~"), '.descarteslabs', 'token_info.json')


def base64url_decode(input):
    """Helper method to base64url_decode a string.
    Args:
        input (str): A base64url_encoded string to decode.
    """
    rem = len(input) % 4
    if rem > 0:
        input += b'=' * (4 - rem)

    return base64.urlsafe_b64decode(input)


def makedirs_if_not_exists(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
        except OSError as ex:
            if ex.errno == errno.EEXIST:
                pass
            else:
                raise


class Auth:
    def __init__(self, domain="https://iam.descarteslabs.com",
                 scope=None, leeway=500, token_info_path=DEFAULT_TOKEN_INFO_PATH,
                 client_id=None, client_secret=None, jwt_token=None):
        """
        Helps retrieve JWT from a client id and refresh token for cli usage.
        :param domain: endpoint for auth0
        :param scope: the JWT fields to be included
        :param leeway: JWT expiration leeway
        :param token_info_path: path to a JSON file optionally holding auth information
        :param client_id: JWT client id
        :param client_secret: JWT client secret
        :param jwt_token: the JWT token, if we already have one
        """
        self.token_info_path = token_info_path

        token_info = {}
        if self.token_info_path:
            try:
                with open(self.token_info_path) as fp:
                    token_info = json.load(fp)
            except (IOError, ValueError):
                pass

        self.client_id = client_id if client_id else os.environ.get('CLIENT_ID', token_info.get('client_id', None))
        self.client_secret = client_secret if client_secret else os.environ.get('CLIENT_SECRET', token_info.get(
            'client_secret', None))
        self._token = jwt_token if jwt_token else os.environ.get('JWT_TOKEN', token_info.get('jwt_token', None))

        if token_info:
            # If the token was read from a path but environment variables were set, we may need
            # to reset the token.
            client_id_changed = token_info.get('client_id', None) != self.client_id
            client_secret_changed = token_info.get('client_secret', None) != self.client_secret

            if client_id_changed or client_secret_changed:
                self._token = None

        self._namespace = None

        self.domain = domain
        self.scope = scope
        self.leeway = leeway

        if self.scope is None:
            self.scope = ['openid', 'name', 'groups']

    @classmethod
    def from_environment_or_token_json(cls, **kwargs):
        """
        Creates an Auth object from environment variables CLIENT_ID, CLIENT_SECRET,
        JWT_TOKEN if they are set, or else from a JSON file at the given path.
        :param domain: endpoint for auth0
        :param scope: the JWT fields to be included
        :param leeway: JWT expiration leeway
        :param token_info_path: path to a JSON file optionally holding auth information
        """
        return Auth(**kwargs)

    @property
    def token(self):
        if self._token is None:
            self._get_token()

        exp = self.payload.get('exp')

        if exp is not None:
            now = (datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds()
            if now + self.leeway > exp:
                try:
                    self._get_token()
                except AuthError as e:
                    # Unable to refresh, raise if now > exp
                    if now > exp:
                        raise e

        return self._token

    @property
    def payload(self):
        if self._token is None:
            self._get_token()

        if isinstance(self._token, six.text_type):
            token = self._token.encode('utf-8')
        else:
            token = self._token

        claims = token.split(b'.')[1]
        return json.loads(base64url_decode(claims).decode('utf-8'))

    def _get_token(self, timeout=100):
        if self.client_id is None:
            raise AuthError("Could not find CLIENT_ID")

        if self.client_secret is None:
            raise AuthError("Could not find CLIENT_SECRET")

        s = requests.Session()
        retries = Retry(total=5,
                        backoff_factor=random.uniform(1, 10),
                        method_whitelist=frozenset(['GET', 'POST']),
                        status_forcelist=[429, 500, 502, 503, 504])

        s.mount('https://', HTTPAdapter(max_retries=retries))

        headers = {"content-type": "application/json"}
        params = {
            "scope": " ".join(self.scope),
            "client_id": self.client_id,
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "target": self.client_id,
            "api_type": "app",
            "refresh_token": self.client_secret
        }
        r = s.post(self.domain + "/auth/delegation", headers=headers, data=json.dumps(params), timeout=timeout)

        if r.status_code != 200:
            raise OauthError("%s: %s" % (r.status_code, r.text))

        data = r.json()
        self._token = data['id_token']

        token_info = {}

        if self.token_info_path:
            try:
                with open(self.token_info_path) as fp:
                    token_info = json.load(fp)
            except (IOError, ValueError):
                pass

        token_info['jwt_token'] = self._token

        if self.token_info_path:
            token_info_directory = os.path.dirname(self.token_info_path)
            makedirs_if_not_exists(token_info_directory)

            try:
                with open(self.token_info_path, 'w+') as fp:
                    json.dump(token_info, fp)

                os.chmod(self.token_info_path, stat.S_IRUSR | stat.S_IWUSR)
            except IOError as e:
                warnings.warn('failed to save token: {}'.format(e))

    @property
    def namespace(self):
        if self._namespace is None:
            self._namespace = sha1(self.payload['sub'].encode('utf-8')).hexdigest()
        return self._namespace


if __name__ == '__main__':
    auth = Auth()

    print(auth.token)
