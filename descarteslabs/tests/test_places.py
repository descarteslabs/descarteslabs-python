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


class TestPlaces(unittest.TestCase):
    instance = None

    @classmethod
    def setUpClass(cls):
        cls.instance = dl.places

    def test_placetypes(self):
        data = self.instance.placetypes()
        self.assertEqual(9, len(data))

    def test_find(self):
        r = self.instance.find('united-states_iowa')
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


if __name__ == '__main__':
    unittest.main()
