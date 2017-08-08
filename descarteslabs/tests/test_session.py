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


class TestSession(unittest.TestCase):
    def test_override_url(self):
        dl.raster.base_url = "http://www.descarteslabs.com"
        self.assertEqual(dl.raster.base_url, dl.raster.session.base_url)


if __name__ == '__main__':
    unittest.main()
