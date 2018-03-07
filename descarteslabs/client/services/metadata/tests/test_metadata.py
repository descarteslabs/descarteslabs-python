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

import itertools
import unittest

from descarteslabs.client.services.metadata import Metadata
from descarteslabs.client.exceptions import NotFoundError


class TestMetadata(unittest.TestCase):
    instance = None

    @classmethod
    def setUpClass(cls):
        cls.instance = Metadata()

    def test_sources(self):
        r = self.instance.sources()
        self.assertGreater(len(r), 0)

    def test_available_products(self):
        r = self.instance.available_products()
        self.assertGreater(len(r), 0)

    def test_search(self):
        r = self.instance.search()
        self.assertGreater(len(r['features']), 0)

    def test_search_dltile(self):
        dltile = "256:16:30.0:15:-11:591"
        r = self.instance.search(start_time='2016-07-06', end_time='2016-07-07',
                                 products=['landsat:LC08:PRE:TOAR'], dltile=dltile)
        keys = [f['key'] for f in r['features']]
        self.assertTrue('meta_LC80270312016188_v1' in keys)

    def test_sat_id(self):
        r = self.instance.search(start_time='2016-07-06', end_time='2016-07-07', sat_id='LANDSAT_8')
        self.assertGreater(len(r['features']), 0)

    def test_cloud_fraction(self):
        r = self.instance.search(start_time='2016-07-06', end_time='2016-07-07', sat_id='LANDSAT_8',
                                 cloud_fraction=0.5)
        for feature in r['features']:
            self.assertLess(feature['properties']['cloud_fraction'], 0.5)

        r = self.instance.search(start_time='2016-07-06', end_time='2016-07-07', sat_id='LANDSAT_8',
                                 cloud_fraction=0.0)
        for feature in r['features']:
            self.assertEqual(feature['properties']['cloud_fraction'], 0.0)

    def test_search_by_product(self):
        r = self.instance.search(start_time='2016-07-06', end_time='2016-07-07', products=['landsat:LC08:PRE:TOAR'])
        self.assertGreater(len(r['features']), 0)

    def test_fields(self):
        cases = [
            [],
            ["id", "key"],  # ["id", "key"]
            ["key"],  # ["id", "key"]
            ["geometry"],  # ["geometry", "id", "type"]
        ]

        for fields in cases:
            r = self.instance.search(
                start_time='2016-07-06',
                end_time='2016-07-07',
                products="landsat:LC08:PRE:TOAR",
                limit=1,
                fields=fields
            )
            for feature in r['features']:
                if "id" not in fields:
                    fields.append("id")

                if "geometry" in fields:
                    fields.append("type")

                if "key" in fields or "geometry" in fields:
                    fields.append("properties")

                self.assertEqual(sorted(feature.keys()), sorted(fields))

        fields = ["cloud_fraction", "fill_fraction"]
        r = self.instance.search(start_time='2016-07-06', end_time='2016-07-07', fields=fields)
        for feature in r['features']:
            self.assertEqual(sorted(feature['properties'].keys()), sorted(fields))

    def test_multiple_products_search(self):
        r = self.instance.search(
            start_time='2016-07-06',
            end_time='2016-07-07',
            products=['landsat:LE07:PRE:TOAR', 'landsat:LC08:PRE:TOAR']
        )
        self.assertGreater(len(r['features']), 0)

    def test_place(self):
        r = self.instance.search(products=['landsat:LC08:PRE:TOAR'], place='north-america_united-states_iowa', limit=1)
        self.assertEqual(1, len(r['features']))

    def test_summary(self):
        r = self.instance.summary(
            start_time='2016-07-06',
            end_time='2016-07-07',
            products=['landsat:LC08:PRE:TOAR'],
            pixels=True
        )
        self.assertIn('products', r)
        self.assertIn('count', r)
        self.assertIn('pixels', r)
        self.assertIn('bytes', r)
        self.assertGreater(r['count'], 0)

    def test_summary_part(self):
        r = self.instance.summary(
            start_time='2016-07-06',
            end_time='2016-07-07',
            products=['landsat:LC08:PRE:TOAR'],
            part='year',
            pixels=True
        )
        self.assertIn('count', r)
        self.assertIn('pixels', r)
        self.assertIn('bytes', r)
        self.assertIn('items', r)
        self.assertEqual(len(r['items']), 1)

    def test_features(self):
        r = self.instance.features(start_time='2016-07-06', end_time='2016-07-07', sat_id='LANDSAT_8', batch_size=10)
        first_21 = itertools.islice(r, 21)
        self.assertGreater(len(list(first_21)), 0)

    def test_products_search_sort(self):
        r = self.instance.search(start_time='2016-07-06', end_time='2016-07-07', products=['landsat:LC08:PRE:TOAR'],
                                 sort_field="cloud_fraction", sort_order="desc")
        self.assertGreater(len(r['features']), 0)

        last = None
        for feature in r['features']:
            current = feature['properties']['cloud_fraction']
            if last is None:
                last = current
                continue

            self.assertLessEqual(current, last)

    def test_search_products(self):
        r = self.instance.products(limit=1)
        self.assertEqual(len(r), 1)

    def test_products_get(self):
        product_id = 'landsat:LC08:PRE:TOAR'
        r = self.instance.get_product(product_id)
        self.assertEqual(r['product'], product_id)

    def test_bands_get(self):
        band_id = 'landsat:LC08:PRE:TOAR:red'
        try:
            band = self.instance.get_band(band_id)
            self.assertEqual(band_id, band['id'])
        except NotFoundError:
            pass

    def test_derived_bands_get(self):
        band_id = 'derived:ndvi'
        try:
            d_band = self.instance.get_derived_band(band_id)
            self.assertIn('bands', d_band)
        except NotFoundError:
            pass

    def test_derived_bands_search(self):
        bands = ['red', 'nir']
        bands = self.instance.derived_bands(bands=bands)

    def test_get_bands_by_key(self):
        self.instance.get_bands_by_key('meta_LC80270312016188_v1')

    def test_expr_serialization(self):
        p = self.instance.properties
        q = ((0.1 < p.cloud_fraction <= 0.2) & (p.sat_id == "f00b")) | (p.sat_id == "usa-245")
        expected_q = {"or": [
            {"and": [
                {"range": {"cloud_fraction": {"gt": 0.1, "lte": 0.2}}},
                {"eq": {"sat_id": "f00b"}}
            ]},
            {"eq": {"sat_id": "usa-245"}},
        ]}

        self.assertEqual(q.serialize(), expected_q)


if __name__ == '__main__':
    unittest.main()
