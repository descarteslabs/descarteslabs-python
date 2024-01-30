from io import BytesIO

# import pytest
import responses

import geopandas as gpd
import pandas as pd

from .. import features

from .base import BaseTestCase


# TODO: These tests are all a bit incorrect, because we are not working
# with the service and there is no product with a model to define the
# dataframe schema. Oh well...
class FeaturesTestCase(BaseTestCase):
    @responses.activate
    def test_add_spatial(self):
        buffer = BytesIO()
        self.spatial_test_dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)

        expected = buffer.read()

        self.mock_response(
            "POST",
            f"/products/{self.spatial_product_id}/featuresv2",
            body=expected,
            headers={"is_spatial": "True"},
        )

        result = features.add(self.spatial_product_id, self.spatial_test_dataframe)

        assert isinstance(result, gpd.GeoDataFrame)
        assert result.equals(self.spatial_test_dataframe)
        # not practical to verify data due to the use of file upload
        self.assert_url_called(
            "POST",
            f"/products/{self.spatial_product_id}/featuresv2",
        )

    @responses.activate
    def test_add_nonspatial(self):
        buffer = BytesIO()
        self.nonspatial_test_dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)

        expected = buffer.read()

        self.mock_response(
            "POST",
            f"/products/{self.nonspatial_product_id}/featuresv2",
            body=expected,
            headers={"is_spatial": "False"},
        )

        result = features.add(
            self.nonspatial_product_id, self.nonspatial_test_dataframe
        )

        assert isinstance(result, pd.DataFrame) and not isinstance(
            result, gpd.GeoDataFrame
        )
        assert result.equals(self.nonspatial_test_dataframe)
        # not practical to verify data due to the use of file upload
        self.assert_url_called(
            "POST",
            f"/products/{self.nonspatial_product_id}/featuresv2",
        )

    @responses.activate
    def test_query_spatial(self):
        buffer = BytesIO()
        self.spatial_test_dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)

        expected = buffer.read()

        self.mock_response(
            "POST",
            f"/products/{self.spatial_product_id}/features/query",
            body=expected,
            headers={"is_spatial": "True"},
        )

        result = features.query(self.spatial_product_id)

        assert isinstance(result, gpd.GeoDataFrame)
        assert result.equals(self.spatial_test_dataframe)
        self.assert_url_called(
            "POST",
            f"/products/{self.spatial_product_id}/features/query",
        )

    @responses.activate
    def test_query_nonspatial(self):
        buffer = BytesIO()
        self.nonspatial_test_dataframe.to_parquet(buffer, index=False)
        buffer.seek(0)

        expected = buffer.read()

        self.mock_response(
            "POST",
            f"/products/{self.nonspatial_product_id}/features/query",
            body=expected,
            headers={"is_spatial": "False"},
        )

        result = features.query(self.nonspatial_product_id)

        assert isinstance(result, pd.DataFrame) and not isinstance(
            result, gpd.GeoDataFrame
        )
        assert result.equals(self.nonspatial_test_dataframe)
        self.assert_url_called(
            "POST",
            f"/products/{self.nonspatial_product_id}/features/query",
        )

    @responses.activate
    def test_get_spatial(self):
        buffer = BytesIO()
        self.spatial_test_dataframe.iloc[:1, :].to_parquet(buffer, index=False)
        buffer.seek(0)

        expected = buffer.read()

        self.mock_response(
            "GET",
            f"/products/{self.spatial_product_id}/features/{self.spatial_feature_id}",
            body=expected,
            headers={"is_spatial": "True"},
        )

        result = features.get(self.spatial_product_id, self.spatial_feature_id)

        assert isinstance(result, gpd.GeoDataFrame)
        assert result.equals(self.spatial_test_dataframe.iloc[:1, :])
        self.assert_url_called(
            "GET",
            f"/products/{self.spatial_product_id}/features/{self.spatial_feature_id}",
        )

    @responses.activate
    def test_get_nonspatial(self):
        buffer = BytesIO()
        self.nonspatial_test_dataframe.iloc[:1, :].to_parquet(buffer, index=False)
        buffer.seek(0)

        expected = buffer.read()

        self.mock_response(
            "GET",
            f"/products/{self.nonspatial_product_id}/features/{self.nonspatial_feature_id}",
            body=expected,
            headers={"is_spatial": "False"},
        )

        result = features.get(self.nonspatial_product_id, self.nonspatial_feature_id)

        assert isinstance(result, pd.DataFrame) and not isinstance(
            result, gpd.GeoDataFrame
        )
        assert result.equals(self.nonspatial_test_dataframe.iloc[:1, :])
        self.assert_url_called(
            "GET",
            f"/products/{self.nonspatial_product_id}/features/{self.nonspatial_feature_id}",
        )

    @responses.activate
    def test_update_spatial(self):
        self.mock_response(
            "PUT",
            f"/products/{self.spatial_product_id}/featuresv2/{self.spatial_feature_id}",
        )

        features.update(
            self.spatial_product_id,
            self.spatial_feature_id,
            self.spatial_test_dataframe.iloc[:1, :],
        )

        self.assert_url_called(
            "PUT",
            f"/products/{self.spatial_product_id}/featuresv2/{self.spatial_feature_id}",
        )

    @responses.activate
    def test_update_nonspatial(self):
        self.mock_response(
            "PUT",
            f"/products/{self.nonspatial_product_id}/featuresv2/{self.nonspatial_feature_id}",
        )

        features.update(
            self.nonspatial_product_id,
            self.nonspatial_feature_id,
            self.nonspatial_test_dataframe.iloc[:1, :],
        )

        self.assert_url_called(
            "PUT",
            f"/products/{self.nonspatial_product_id}/featuresv2/{self.nonspatial_feature_id}",
        )

    @responses.activate
    def test_aggregate(self):
        self.mock_response(
            "POST",
            f"/products/{self.spatial_product_id}/features/aggregate",
            json={"num.MAX": 4},
        )

        result = features.aggregate(self.spatial_product_id, features.Statistic.MAX)

        assert result == {"num.MAX": 4}

        self.assert_url_called(
            "POST",
            f"/products/{self.spatial_product_id}/features/aggregate",
        )

    @responses.activate
    def test_delete(self):
        self.mock_response(
            "DELETE",
            f"/products/{self.spatial_product_id}/features/{self.spatial_feature_id}",
            status=204,
        )

        features.delete(self.spatial_product_id, self.spatial_feature_id)

        self.assert_url_called(
            "DELETE",
            f"/products/{self.spatial_product_id}/features/{self.spatial_feature_id}",
        )
