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
