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

import pytest
import responses

try:
    import ipyleaflet
except ImportError:
    ipyleaflet = None

from ...common.vector import models as vector_models
from .. import tiles
from .base import BaseTestCase


class TilesTestCase(BaseTestCase):
    @pytest.mark.skipif(ipyleaflet is None, reason="ipyleaflet not installed")
    @responses.activate
    def test_tiles(self):
        # mock table
        table_response = {
            "id": self.spatial_product_id,
            "name": self.spatial_product_id,
            "is_spatial": True,
            "model": vector_models.GenericFeatureBaseModel.model_json_schema(),
            "created": "2024-01-01T00:00:00.000000",
        }

        self.mock_response(
            "GET", f"/products/{self.spatial_product_id}", json=table_response
        )

        tiles.create_layer(self.spatial_product_id, self.spatial_product_id)
