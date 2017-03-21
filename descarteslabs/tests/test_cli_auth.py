import unittest
from descarteslabs.cli_auth import Auth
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
