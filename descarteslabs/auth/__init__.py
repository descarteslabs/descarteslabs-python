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
    def __init__(self, domain="https://descarteslabs.auth0.com",
                 scope=None, leeway=500):
        """
        Helps retrieve JWT from a client id and refresh token for cli usage.
        :param client_id: str
        :param refresh_token: str generated through IAM interface
        :param url: endpoint for auth0
        :param scope: the JWT fields to be included
        :param leeway: JWT expiration leeway
        """

        token_info = {}

        try:
            with open(os.path.join(os.path.expanduser("~"), '.descarteslabs', 'token_info.json')) as fp:
                token_info = json.load(fp)
        except:
            pass

        self.client_id = os.environ.get('CLIENT_ID', token_info.get('client_id', None))
        self.client_secret = os.environ.get('CLIENT_SECRET', token_info.get('client_secret', None))
        self._token = os.environ.get('JWT_TOKEN', token_info.get('jwt_token', None))

        self.domain = domain
        self.scope = scope
        self.leeway = leeway

        if self.scope is None:
            self.scope = ['openid', 'name', 'groups']

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
            raise RuntimeError("Could not find CLIENT_ID")

        if self.client_secret is None:
            raise RuntimeError("Could not find CLIENT_SECRET")

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
        r = s.post(self.domain + "/delegation", headers=headers, data=json.dumps(params), timeout=timeout)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        data = r.json()

        self._token = data['id_token']

        token_info = {}

        try:
            with open(os.path.join(os.path.expanduser("~"), '.descarteslabs', 'token_info.json')) as fp:
                token_info = json.load(fp)
        except:
            pass

        token_info['jwt_token'] = self._token

        path = os.path.join(os.path.expanduser("~"), '.descarteslabs')

        if not os.path.exists(path):
            os.makedirs(path)

        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)

        file = os.path.join(os.path.expanduser("~"), '.descarteslabs', 'token_info.json')

        with open(file, 'w+') as fp:
            json.dump(token_info, fp)

        os.chmod(file, stat.S_IRUSR | stat.S_IWUSR)


if __name__ == '__main__':
    auth = Auth()

    print(auth.token)
