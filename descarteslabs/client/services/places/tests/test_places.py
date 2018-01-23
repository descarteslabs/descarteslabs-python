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

from descarteslabs.client.services.places import Places


class TestPlaces(unittest.TestCase):
    instance = None

    @classmethod
    def setUpClass(cls):
        cls.instance = Places()

    def test_placetypes(self):
        data = self.instance.placetypes()
        self.assertGreaterEqual(10, len(data))

    def test_find(self):
        r = self.instance.find('united-states_iowa')
        self.assertEqual(1, len(r))

    def test_search(self):
        r = self.instance.search('texas')
        self.assertEqual(8, len(r))

        r = self.instance.search('texas', country='united-states')
        self.assertEqual(7, len(r))

        r = self.instance.search('texas', country='united-states', placetype='county')
        self.assertEqual(2, len(r))

        r = self.instance.search('texas', country='united-states', region='oklahoma', placetype='county')
        self.assertEqual(1, len(r))

    def test_shape(self):
        r = self.instance.shape('north-america_united-states_iowa')
        self.assertEqual(85688713, r['id'])

    def test_prefix(self):
        # counties by default
        r = self.instance.prefix('north-america_united-states_iowa')
        self.assertEqual(99, len(r['features']))

        r = self.instance.prefix('north-america_united-states_iowa', placetype='district')
        self.assertEqual(9, len(r['features']))

    def test_sources(self):
        r = self.instance.sources()
        self.assertIn({'name': 'nass'}, r)

    def test_categories(self):
        r = self.instance.categories()
        self.assertIn({'name': 'corn'}, r)

    def test_metrics(self):
        r = self.instance.metrics()
        self.assertIn({'name': 'yield', 'units': 'bu/ac'}, r)

    def test_value(self):
        r = self.instance.value('north-america_united-states', source='nass', category='corn', metric='yield')
        self.assertEqual(1, len(r))

    def test_statistics(self):
        r = self.instance.statistics('north-america_united-states', source='nass', category='corn', metric='yield',
                                     units='bu/ac')
        self.assertEqual(36, len(r))

    def test_data(self):
        r = self.instance.data('north-america_united-states', source='nass', category='corn', metric='yield',
                               date='2015-01-01', units='bu/ac')
        self.assertEqual(1439, len(r))


if __name__ == '__main__':
    unittest.main()
