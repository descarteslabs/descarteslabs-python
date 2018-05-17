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

import numpy as np
import unittest
import sys
from mock import patch
from tempfile import NamedTemporaryFile
import responses

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.catalog import Catalog


@patch.object(Auth, 'token', 'token')
@patch.object(Auth, 'namespace', 'foo')
class TestCatalog(unittest.TestCase):
    instance = None

    def setUp(self):
        self.instance = Catalog()

    @staticmethod
    def validate_ndarray_callback(request):
        np.load(request.body)
        return (200, {}, '')

    @patch('descarteslabs.client.services.catalog.Catalog._do_upload', return_value=(False,))
    def test_upload_ndarray_dtype(self, _do_upload):
        unsupported_dtypes = ['uint64']

        for dtype in unsupported_dtypes:
            self.assertRaises(TypeError,
                              self.instance.upload_ndarray,
                              np.zeros((10, 10), dtype=dtype), 'product', 'key')

        for dtype in Catalog.UPLOAD_NDARRAY_SUPPORTED_DTYPES:
            self.instance.upload_ndarray(np.zeros((10, 10), dtype=dtype), 'product', 'key')

    @unittest.skipIf(sys.version_info.major == 3, "Test only makes sense in py2")
    def test_upload_image_deprecated_file_type(self):
        # in py2 NamedTemporaryFile produces a file object not a IOBase object
        with NamedTemporaryFile() as tmp:
            self.assertRaises(
                Exception,
                self.instance.upload_image, tmp, 'product'
            )

    def test_upload_image_bad_path(self):
        name = None
        with NamedTemporaryFile() as tmp:
            name = tmp.file
        self.assertRaises(
            Exception,
            self.instance.upload_image, name, 'product'
        )

    def test_upload_image_multi_file_no_list(self):
        with NamedTemporaryFile() as tmp:
            self.assertRaises(
                ValueError,
                self.instance.upload_image, tmp.name, 'product', multi=True
            )

    def test_upload_image_multi_file_no_image_id(self):
        with NamedTemporaryFile() as tmp:
            self.assertRaises(
                ValueError,
                self.instance.upload_image, [tmp.name, tmp.name], 'product', multi=True
            )

    @responses.activate
    def test_upload_image(self):
        product = 'foo:product_id'
        gcs_upload_url = 'https://gcs_upload_url.com'
        upload_url = 'https://platform.descarteslabs.com/metadata/v1/catalog/products/{}/images/upload/{}'
        with NamedTemporaryFile() as tmp:
            tmp.write(b'foo')
            responses.add(
                responses.POST,
                upload_url.format(product, tmp.name.split('/')[-1]),
                body=gcs_upload_url
            )
            responses.add(responses.PUT, gcs_upload_url)
            self.instance.upload_image(tmp.name, product)

    @responses.activate
    def test_upload_ndarray(self):
        product = 'foo:product_id'
        gcs_upload_url = 'https://gcs_upload_url.com'
        upload_url = 'https://platform.descarteslabs.com/metadata/v1/catalog/products/{}/images/upload/key'
        responses.add(
            responses.POST,
            upload_url.format(product),
            body=gcs_upload_url,
        )
        responses.add_callback(responses.PUT, gcs_upload_url, callback=self.validate_ndarray_callback)
        self.instance.upload_ndarray(np.zeros((10, 10)), product, 'key')


if __name__ == '__main__':
    unittest.main()
