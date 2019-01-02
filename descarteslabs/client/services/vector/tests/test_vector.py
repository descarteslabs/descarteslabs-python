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

import io
import re
import unittest

import responses

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.vector import Vector

public_token = "header.e30.signature"


class ClientTestCase(unittest.TestCase):

    def setUp(self):
        self.url = "http://example.vector.com"
        self.gcs_url = "http://example.gcs.com"

        self.client = Vector(url=self.url, auth=Auth(jwt_token=public_token, token_info_path=None))

        self.match_url = re.compile(self.url)
        self.match_gcs_url = re.compile(self.gcs_url)

        self.attrs = {
            'geometry': {'coordinates': [
                [
                    [-113.40087890624999, 40.069664523297774],
                    [-111.434326171875, 40.069664523297774],
                    [-111.434326171875, 41.918628865183045],
                    [-113.40087890624999, 41.918628865183045],
                    [-113.40087890624999, 40.069664523297774]
                ]
            ], 'type': 'Polygon'},
            'properties': {'baz': 1.0, 'foo': 'bar'}
        }

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)

    def mock_gcs(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_gcs_url, json=json, status=status, **kwargs)


class TasksTest(ClientTestCase):

    @responses.activate
    def test_upload_bytesio(self):
        self.mock_response(responses.POST, {
            'upload_id': 'xyz',
            'url': self.gcs_url
        })

        self.mock_gcs(responses.PUT, {})

        s = io.BytesIO()

        for i in range(10):
            s.write(b'{')
            s.write('{}'.format(self.attrs).encode('utf-8'))
            s.write(b'}\n')

        self.client.upload_features(s, 'test')

    @responses.activate
    def test_upload_stringio(self):
        self.mock_response(responses.POST, {
            'upload_id': 'xyz',
            'url': self.gcs_url
        })

        self.mock_gcs(responses.PUT, {})

        s = io.StringIO()

        for i in range(10):
            s.write(u'{')
            s.write(u'{}'.format(self.attrs))
            s.write(u'}\n')

        self.client.upload_features(s, 'test')

    @responses.activate
    def test_bad_upload(self):
        self.mock_response(responses.POST, {
            'upload_id': 'xyz',
            'url': self.gcs_url
        })

        self.mock_gcs(responses.PUT, {})

        s = ""

        for i in range(10):
            s += '{'
            s += '{}'.format(self.attrs)
            s += '}\n'

        with self.assertRaises(Exception):
            self.client.upload_features(s, 'test')


if __name__ == "__main__":
    unittest.main()
