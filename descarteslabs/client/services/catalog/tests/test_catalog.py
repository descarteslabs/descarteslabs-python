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
from copy import deepcopy
from time import sleep
from random import randint
from hashlib import md5
from mock import patch
from tempfile import NamedTemporaryFile
import responses

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.catalog import Catalog

descartes_auth = Auth()


@patch.object(Auth, 'token', 'token')
@patch.object(Auth, 'namespace', 'foo')
class TestCatalog(unittest.TestCase):
    instance = None

    def setUp(self):
        self.instance = Catalog()
        # product_id = 'test_product:{}'.format(md5(str(randint(0, 2**32))).hexdigest())
        # self.instance = Catalog()
        # self.product = {
        #     'title': 'Test Product',
        #     'description': 'A test product',
        #     'native_bands': ['red'],
        #     'orbit': 'sun-synchronous',
        # }
        # self.band = {
        #     'name': 'red',
        #     'wavelength_min': 700,
        #     'wavelength_max': 750,
        #     'srcfile': 0,
        #     'srcband': 1,
        #     'jpx_layer': 1,
        #     'dtype': 'Byte',
        #     'data_range': [0, 255],
        #     'nbits': 8,
        #     'type': 'spectral',
        # }
        # self.image = {
        #     'bucket': 'dl-storage-{}-data'.format(descartes_auth.namespace),
        #     'directory': 'sub_path',
        #     'files': ['/path/to/file.jp2'],
        #     'geometry': {
        #         "type": "Polygon",
        #         "coordinates": [
        #             [
        #                 [
        #                     68.961181640625,
        #                     50.17689812200107,
        #                     0.0
        #                 ],
        #                 [
        #                     70.15869140625,
        #                     50.17689812200107,
        #                     0.0
        #                 ],
        #                 [
        #                     70.15869140625,
        #                     50.80593472676908,
        #                     0.0
        #                 ],
        #                 [
        #                     68.961181640625,
        #                     50.80593472676908,
        #                     0.0
        #                 ],
        #                 [
        #                     68.961181640625,
        #                     50.17689812200107,
        #                     0.0
        #                 ]
        #             ]
        #         ]
        #     }
        # }
        # r = self.instance.add_product(product_id, **self.product)
        # self.product_id = r['data']['id']
        pass

    def tearDown(self):
        # sleep(1)
        # self.instance.remove_product(self.product_id)
        pass

    @unittest.skip('integration test')
    def test_get_product(self):
        r = self.instance.get_product(self.product_id)
        self.assertEqual(r['data']['id'], self.product_id)

    @unittest.skip('integration test')
    def test_change_product(self):
        self.instance.change_product(self.product_id, **{'read': ['some_group']})

    @unittest.skip('integration test')
    def test_replace_product(self):
        product = deepcopy(self.product)
        product['description'] = 'A new description for this product'
        self.instance.replace_product(self.product_id, **product)

    @unittest.skip('integration test')
    def test_add_band(self):
        self.instance.add_band(self.product_id, 'red', **self.band)
        sleep(1)
        self.instance.get_band(self.product_id, 'red')
        self.instance.remove_band(self.product_id, 'red')

    @unittest.skip('integration test')
    def test_change_band(self):
        self.instance.add_band(self.product_id, 'red', **self.band)
        sleep(1)
        self.instance.change_band(self.product_id, 'red', read=['some_group'])
        self.instance.remove_band(self.product_id, 'red')

    @unittest.skip('integration test')
    def test_replace_band(self):
        self.instance.add_band(self.product_id, 'red', **self.band)
        sleep(1)

        band = deepcopy(self.band)
        band['srcfile'] = 1

        self.instance.replace_band(self.product_id, 'red', **band)
        self.instance.remove_band(self.product_id, 'red')

    @unittest.skip('integration test')
    def test_add_image(self):
        self.instance.add_image(self.product_id, 'some_meta_id', **self.image)
        sleep(1)
        self.instance.get_image(self.product_id, 'some_meta_id')
        self.instance.remove_image(self.product_id, 'some_meta_id')

    @unittest.skip('integration test')
    def test_change_image(self):
        self.instance.add_image(self.product_id, 'some_meta_id', **self.image)
        sleep(1)
        self.instance.change_image(self.product_id, 'some_meta_id', read=['some_group'])
        self.instance.remove_image(self.product_id, 'some_meta_id')

    @unittest.skip('integration test')
    def test_replace_image(self):
        self.instance.add_image(self.product_id, 'some_meta_id', **self.image)
        sleep(1)

        image = deepcopy(self.image)
        image['cloud_fraction'] = .5

        self.instance.replace_image(self.product_id, 'some_meta_id', **image)
        self.instance.remove_image(self.product_id, 'some_meta_id')

    @unittest.skip('integration test')
    def test_own_products(self):
        op = self.instance.own_products()
        self.assertGreater(len(op), 0)
        for p in op:
            self.assertEqual(p['owner']['uuid'], descartes_auth.payload['sub'])

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
            tmp.write('foo')
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
        responses.add(responses.PUT, gcs_upload_url)
        self.instance.upload_ndarray(np.zeros((10, 10)), product, 'key')

    @unittest.skip('integration test')
    def test_datetime_backwards_compatibile(self):
        product_id = 'test_product:{}'.format(md5(str(randint(0, 2**32))).hexdigest())
        product = {
            'title': 'Test Product',
            'description': 'A test product',
            'native_bands': ['red'],
            'orbit': 'sun-synchronous',
            'start_date': '2017-01-01',
            'end_date': '2017-12-30'
        }
        self.instance.add_product(product_id, **product)
        self.instance.change_product(product_id, **{'start_date': '2016-01-01', 'end_date': '2016-12-30'})
        product_copy = deepcopy(self.product)
        product_copy['start_date'] = '2015-01-01'
        product_copy['end_date'] = '2015-12-30'
        self.instance.replace_product(product_id, **product_copy)


if __name__ == '__main__':
    unittest.main()
