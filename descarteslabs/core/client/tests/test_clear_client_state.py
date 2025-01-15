# © 2025 EarthDaily Analytics Corp.
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

from .. import clear_client_state
from descarteslabs.auth import Auth
from ..services.raster import Raster


class ClientStateTests(unittest.TestCase):
    def test_clear_client_state(self):
        clear_client_state()

        auth = Auth.get_default_auth()
        assert auth
        assert Auth._instance is auth

        raster = Raster.get_default_client()
        assert raster
        assert Raster._instance is raster

        clear_client_state()

        assert Auth._instance is None
        assert Raster._instance is None
