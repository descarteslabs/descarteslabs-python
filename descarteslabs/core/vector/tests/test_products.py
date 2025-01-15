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

from descarteslabs.exceptions import BadRequestError, ConflictError, NotFoundError

from .. import products
from ...common.vector.models import GenericFeatureBaseModel, VectorBaseModel

from .base import BaseTestCase


class ProductsTestCase(BaseTestCase):
    @responses.activate
    def test_create_spatial(self):
        expected = {
            "id": self.spatial_product_id,
            "name": self.spatial_product_id,
            "is_spatial": True,
            "model": GenericFeatureBaseModel.model_json_schema(),
        }

        self.mock_response("POST", "/products/", json=expected)

        result = products.create(self.spatial_product_id, self.spatial_product_id)
        assert result == expected
        self.assert_url_called("POST", "/products/", json=expected)

    @responses.activate
    def test_create_nonspatial(self):
        expected = {
            "id": self.nonspatial_product_id,
            "name": self.nonspatial_product_id,
            "is_spatial": False,
            "model": VectorBaseModel.model_json_schema(),
        }

        self.mock_response("POST", "/products/", json=expected)

        result = products.create(
            self.nonspatial_product_id,
            self.nonspatial_product_id,
            model=VectorBaseModel,
        )
        assert result == expected
        self.assert_url_called("POST", "/products/", json=expected)

    @responses.activate
    def test_create_conflict(self):
        expected = {
            "id": self.spatial_product_id,
            "name": self.spatial_product_id,
            "is_spatial": True,
            "model": GenericFeatureBaseModel.model_json_schema(),
        }

        self.mock_response("POST", "/products/", status=409)

        with pytest.raises(ConflictError):
            products.create(self.spatial_product_id, self.spatial_product_id)

        self.assert_url_called("POST", "/products/", json=expected)

    @responses.activate
    def test_create_bad_request(self):
        expected = {
            "id": self.spatial_product_id,
            "name": self.spatial_product_id,
            "is_spatial": True,
            "model": GenericFeatureBaseModel.model_json_schema(),
        }

        self.mock_response("POST", "/products/", status=400)

        with pytest.raises(BadRequestError):
            products.create(self.spatial_product_id, self.spatial_product_id)

        self.assert_url_called("POST", "/products/", json=expected)

    @responses.activate
    def test_list(self):
        expected = [
            {
                "id": self.spatial_product_id,
                "name": self.spatial_product_id,
            },
            {
                "id": self.nonspatial_product_id,
                "name": self.nonspatial_product_id,
            },
        ]

        self.mock_response("GET", "/products/", json=expected)

        result = products.list()
        assert result == expected

    @responses.activate
    def test_get(self):
        expected = {
            "id": self.spatial_product_id,
            "name": self.spatial_product_id,
        }

        self.mock_response("GET", f"/products/{self.spatial_product_id}", json=expected)

        result = products.get(self.spatial_product_id)
        assert result == expected

    @responses.activate
    def test_get_not_found(self):
        self.mock_response("GET", f"/products/{self.spatial_product_id}", status=404)

        with pytest.raises(NotFoundError):
            products.get(self.spatial_product_id)

    @responses.activate
    def test_update(self):
        expected = {
            "description": "This is a test",
        }

        self.mock_response(
            "PATCH", f"/products/{self.spatial_product_id}", json=expected
        )

        result = products.update(self.spatial_product_id, description="This is a test")
        assert result == expected
        self.assert_url_called(
            "PATCH", f"/products/{self.spatial_product_id}", json=expected
        )

    @responses.activate
    def test_delete(self):
        self.mock_response("DELETE", f"/products/{self.spatial_product_id}")

        products.delete(self.spatial_product_id)
        self.assert_url_called("DELETE", f"/products/{self.spatial_product_id}")

    @responses.activate
    def test_delete_not_found(self):
        self.mock_response("DELETE", f"/products/{self.spatial_product_id}", status=404)

        with pytest.raises(NotFoundError):
            products.delete(self.spatial_product_id)
