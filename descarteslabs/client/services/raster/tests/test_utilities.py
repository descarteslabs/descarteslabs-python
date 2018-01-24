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

import json
import unittest

from descarteslabs.client.services.raster.raster import as_json_string


class TestUtilities(unittest.TestCase):

    def test_as_json_string(self):
        d = {'a': 'b'}
        truth = json.dumps(d)

        self.assertEqual(as_json_string(d), truth)
        s = '{"a": "b"}'
        self.assertEqual(as_json_string(s), truth)
        self.assertEqual(as_json_string(None), None)


if __name__ == "__main__":
    unittest.main()
