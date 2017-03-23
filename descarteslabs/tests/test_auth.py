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

import unittest
from descarteslabs.auth import Auth
import requests
import json


class TestAuth(unittest.TestCase):
    def test_get_token(self):
        # get a jwt
        auth = Auth()
        self.assertIsNotNone(auth.token)

        # validate the jwt
        url = auth.domain + "/tokeninfo"
        params = {"id_token": auth.token}
        headers = {"content-type": "application/json"}
        r = requests.post(url, data=json.dumps(params), headers=headers)
        self.assertEqual(200, r.status_code)


if __name__ == '__main__':
    unittest.main()
