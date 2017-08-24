# Copyright 2017 Descartes Labs.
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

from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

import random
import base64
import json
import datetime
import six
import os
import stat

from descarteslabs.exceptions import AuthError, OauthError

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
        self.client_id = client_id
        self.client_secret = client_secret
        self._token = jwt_token

        self.domain = domain
        self.scope = scope
        self.leeway = leeway

        if self.scope is None:
            self.scope = ['openid', 'name', 'groups']

    @classmethod
    def from_environment_or_token_json(cls, domain="https://iam.descarteslabs.com",
                                       scope=None, leeway=500,
                                       token_info_path=DEFAULT_TOKEN_INFO_PATH):
        """
        Creates an Auth object from environment variables CLIENT_ID, CLIENT_SECRET,
        JWT_TOKEN if they are set, or else from a JSON file at the given path.
        :param domain: endpoint for auth0
        :param scope: the JWT fields to be included
        :param leeway: JWT expiration leeway
        :param token_info_path: path to a JSON file optionally holding auth information
        """
        token_info = {}
        try:
            with open(token_info_path) as fp:
                token_info = json.load(fp)
        except:
            pass

        client_id = os.environ.get('CLIENT_ID', token_info.get('client_id', None))
        client_secret = os.environ.get('CLIENT_SECRET', token_info.get('client_secret', None))
        jwt_token = os.environ.get('JWT_TOKEN', token_info.get('jwt_token', None))

        return cls(domain=domain, scope=scope, leeway=leeway, token_info_path=token_info_path,
                   client_id=client_id, client_secret=client_secret, jwt_token=jwt_token)

    @property
    def token(self):
        if self._token is None:
            self._get_token()

        exp = self.payload.get('exp')

        if exp is not None:
            now = (datetime.datetime.utcnow() - datetime.datetime(1970, 1, 1)).total_seconds()
            if now + self.leeway > exp:
                self._get_token()

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

        try:
            with open(self.token_info_path) as fp:
                token_info = json.load(fp)
        except:
            pass

        token_info['jwt_token'] = self._token

        path = os.path.join(os.path.expanduser("~"), '.descarteslabs')

        if not os.path.exists(path):
            os.makedirs(path)

        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        with open(self.token_info_path, 'w+') as fp:
            json.dump(token_info, fp)

        os.chmod(self.token_info_path, stat.S_IRUSR | stat.S_IWUSR)


if __name__ == '__main__':
    auth = Auth.from_environment_or_token_json()

    print(auth.token)
