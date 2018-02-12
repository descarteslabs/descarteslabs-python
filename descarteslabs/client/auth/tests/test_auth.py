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

import unittest
from descarteslabs.client.auth import Auth
import requests
import json


# flake8: noqa
anon_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJncm91cHMiOlsicHVibGljIl0sImlzcyI6Imh0dHBzOi8vZGVzY2FydGVzbGFicy5hdXRoMC5jb20vIiwic3ViIjoiZGVzY2FydGVzfGFub24tdG9rZW4iLCJhdWQiOiJaT0JBaTRVUk9sNWdLWklweHhsd09FZng4S3BxWGYyYyIsImV4cCI6OTk5OTk5OTk5OSwiaWF0IjoxNDc4MjAxNDE5fQ.QL9zq5SkpO7skIy0niIxI0B92uOzZT5t1abuiJaspRI"


class TestAuth(unittest.TestCase):
    def test_get_token(self):
        # get a jwt
        auth = Auth.from_environment_or_token_json()
        self.assertIsNotNone(auth.token)

        # validate the jwt
        url = "https://descarteslabs.auth0.com" + "/tokeninfo"
        params = {"id_token": auth.token}
        headers = {"content-type": "application/json"}
        r = requests.post(url, data=json.dumps(params), headers=headers)
        self.assertEqual(200, r.status_code)

    def test_get_namespace(self):
        auth = Auth.from_environment_or_token_json()
        self.assertIsNotNone(auth.namespace)

    def test_init_token_no_path(self):
        auth = Auth(jwt_token=anon_token, token_info_path=None, client_id="foo")
        self.assertEquals(anon_token, auth._token)


if __name__ == '__main__':
    unittest.main()
