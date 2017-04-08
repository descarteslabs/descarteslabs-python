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

import descarteslabs as dl
from descarteslabs.tests.helpers import is_external_user


class TestMetadata(unittest.TestCase):
    instance = None

    @classmethod
    def setUpClass(cls):
        cls.instance = dl.metadata

    def test_sources(self):
        r = self.instance.sources()
        self.assertGreater(len(r), 0)

    def test_search(self):
        r = self.instance.search()
        self.assertGreater(len(r['features']), 0)

    @unittest.skip("search by sat_id not currently supported")
    def test_sat_id(self):
        r = self.instance.search(start_time='2016-09-01', end_time='2016-09-02', sat_id='LANDSAT_8')
        self.assertEqual(100, len(r['features']))

    @unittest.skipIf(is_external_user(), "currently requires internal user")
    def test_const_id(self):
        r = self.instance.search(start_time='2016-09-01', end_time='2016-09-02', const_id=['L8'])
        self.assertGreater(len(r['features']), 0)

    def test_multiple_const_id(self):
        r = self.instance.search(start_time='2016-09-01', end_time='2016-09-02', const_id=['L8', 'L7'])
        self.assertGreater(len(r['features']), 0)

    @unittest.skipIf(is_external_user(), "currently requires internal user")
    def test_place(self):
        r = self.instance.search(const_id=['L8'], place='north-america_united-states_iowa', limit=1)
        self.assertEqual(1, len(r['features']))

    def test_summary(self):
        r = self.instance.summary(start_time='2016-09-01', end_time='2016-09-02', const_id=['L8'])
        self.assertEqual(1, len(list(r['items'])))


if __name__ == '__main__':
    unittest.main()
