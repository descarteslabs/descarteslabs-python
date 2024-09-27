# Copyright 2018-2024 Descartes Labs.

from datetime import datetime
from io import BytesIO
from uuid import uuid4

import geojson
import geopandas as gpd
import pandas as pd
import pytest
import responses

from ...common.vector import models as vector_models
from ...common.geo import AOI
from ...common.property_filtering import Properties

from ..vector import Table, Feature

from .base import BaseTestCase


p = Properties()


class SpatialModel(vector_models.PointBaseModel):
    color: str
    size: str
    name: str
    type: str


class NonSpatialModel(vector_models.VectorBaseModel):
    color: str
    size: str
    name: str
    type: str


# Table operations
class TableTestCase(BaseTestCase):
    product_id = "test_product"
    table_name = "test_product"
    table_description = "test spatial vector product"

    @responses.activate
    def test_spatial_table_create(self):
        table_request = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": True,
            "model": SpatialModel.model_json_schema(),
        }

        table_response = dict(**table_request, created="2024-01-01T00:00:00.000000")

        self.mock_response("POST", "/products/", json=table_response)

        table = Table.create(
            self.product_id, self.table_name, self.table_description, model=SpatialModel
        )

        self.assert_url_called("POST", "/products/", json=table_request)

        assert table.id == self.product_id
        assert table.name == self.table_name
        assert table.description == self.table_description
        assert table.model == SpatialModel.model_json_schema()
        assert table.is_spatial is True
        assert table.created == datetime.fromisoformat("2024-01-01T00:00:00.000000")

        # Test that str and repr don't error out for Table on creation
        assert str(table)
        assert repr(table)

    @responses.activate
    def test_nonspatial_table_create(self):
        table_request = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": False,
            "model": NonSpatialModel.model_json_schema(),
        }

        table_response = dict(**table_request, created="2024-01-01T00:00:00.000000")

        self.mock_response("POST", "/products/", json=table_response)

        table = Table.create(
            self.product_id,
            self.table_name,
            self.table_description,
            model=NonSpatialModel,
        )

        self.assert_url_called("POST", "/products/", json=table_request)

        assert table.id == self.product_id
        assert table.name == self.table_name
        assert table.description == self.table_description
        assert table.model == NonSpatialModel.model_json_schema()
        assert table.is_spatial is False
        assert table.created == datetime.fromisoformat("2024-01-01T00:00:00.000000")

        # Test that str and repr don't error out for Table on creation
        assert str(table)
        assert repr(table)

    @responses.activate
    def test_table_get(self):
        table_response = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": True,
            "model": SpatialModel.model_json_schema(),
            "created": "2024-01-01T00:00:00.000000",
        }

        self.mock_response("GET", f"/products/{self.product_id}", json=table_response)

        table = Table.get(self.product_id)

        self.assert_url_called("GET", f"/products/{self.product_id}")

        assert table.id == self.product_id
        assert table.name == self.table_name
        assert table.description == self.table_description
        assert table.model == SpatialModel.model_json_schema()
        assert table.is_spatial is True
        assert table.created == datetime.fromisoformat("2024-01-01T00:00:00.000000")

        # Test that str and repr don't error out for Table on get
        assert str(table)
        assert repr(table)

    @responses.activate
    def test_table_delete(self):
        self.mock_response("DELETE", f"/products/{self.product_id}", status=204)

        table = Table({"id": self.product_id})

        table.delete()

        self.assert_url_called("DELETE", f"/products/{self.product_id}")

    @responses.activate
    def test_spatial_add_features(self):
        # Get the table
        table_response = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": True,
            "model": SpatialModel.model_json_schema(),
            "created": "2024-01-01T00:00:00.000000",
        }

        self.mock_response("GET", f"/products/{self.product_id}", json=table_response)

        table = Table.get(self.product_id)

        # Add features
        fc = geojson.FeatureCollection(
            [
                {
                    "geometry": {"coordinates": [4.5, 52.1], "type": "Point"},
                    "properties": {
                        "color": "red",
                        "size": "small",
                        "name": "Frank",
                        "type": "Person",
                    },
                    "type": "Feature",
                },
                {
                    "geometry": {"coordinates": [4.6, 52.1], "type": "Point"},
                    "properties": {
                        "color": "blue",
                        "size": "small",
                        "name": "Earth",
                        "type": "planet",
                    },
                    "type": "Feature",
                },
                {
                    "geometry": {"coordinates": [4.6, 52.2], "type": "Point"},
                    "properties": {
                        "color": "red",
                        "size": "big",
                        "name": "Clifford",
                        "type": "dog",
                    },
                    "type": "Feature",
                },
                {
                    "geometry": {"coordinates": [4.5, 52.2], "type": "Point"},
                    "properties": {
                        "color": "blue",
                        "size": "big",
                        "name": "Pacific",
                        "type": "ocean",
                    },
                    "type": "Feature",
                },
            ]
        )

        df = gpd.GeoDataFrame.from_features(fc.features, crs="EPSG:4326")

        # restructure for GenericFeatureBaseModel response
        fc2 = geojson.FeatureCollection(
            [
                {
                    "geometry": f["geometry"],
                    "properties": {**f["properties"], "uuid": str(uuid4())},
                    "type": "Feature",
                }
                for f in fc["features"]
            ]
        )
        df2 = gpd.GeoDataFrame.from_features(fc2.features, crs="EPSG:4326")

        buffer = BytesIO()
        df2.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.mock_response(
            "POST",
            f"/products/{self.product_id}/featuresv2",
            body=buffer.read(),
            headers={"is_spatial": "True"},
        )

        result = table.add(df)

        assert result.equals(df2)

    @responses.activate
    def test_nonspatial_add_features(self):
        # Get the table
        table_response = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": False,
            "model": NonSpatialModel.model_json_schema(),
            "created": "2024-01-01T00:00:00.000000",
        }

        self.mock_response("GET", f"/products/{self.product_id}", json=table_response)

        table = Table.get(self.product_id)

        # Add features
        df = pd.DataFrame(
            {
                "color": ["red", "blue", "red", "blue"],
                "size": ["small", "small", "big", "big"],
                "name": ["Frank", "Earth", "Clifford", "Pacific"],
                "type": ["Person", "planet", "dog", "ocean"],
            }
        )

        # restructure for NonSpatialModel response
        df2 = df.copy(True)
        df2["uuid"] = [str(uuid4()) for _ in range(len(df))]

        buffer = BytesIO()
        df2.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.mock_response(
            "POST",
            f"/products/{self.product_id}/featuresv2",
            body=buffer.read(),
            headers={"is_spatial": "False"},
        )

        result = table.add(df)

        assert result.equals(df2)

    @responses.activate
    def test_spatial_filter_features(self):
        # Get the table
        table_response = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": True,
            "model": SpatialModel.model_json_schema(),
            "created": "2024-01-01T00:00:00.000000",
        }

        self.mock_response("GET", f"/products/{self.product_id}", json=table_response)

        aoi = AOI(bounds=(4.55, 52.0, 4.65, 53.0))
        filter = p.color == "red"

        table = Table.get(self.product_id, aoi=aoi, property_filter=filter)

        assert table.options.aoi is not None
        assert table.options.property_filter is filter

    def test_spatial_filter_bad_aoi(self):
        aoi = "abc"

        with pytest.raises(TypeError, match=f"'{aoi}' not recognized as an aoi"):
            Table.get(self.product_id, aoi=aoi, property_filter=filter)

    @responses.activate
    def test_spatial_get_feature(self):
        # Get the table
        table_response = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": True,
            "model": SpatialModel.model_json_schema(),
            "created": "2024-01-01T00:00:00.000000",
        }

        self.mock_response("GET", f"/products/{self.product_id}", json=table_response)

        table = Table.get(self.product_id)

        feature_id = "1234"

        fc = geojson.FeatureCollection(
            [
                {
                    "geometry": {"coordinates": [4.5, 52.1], "type": "Point"},
                    "properties": {
                        "color": "red",
                        "size": "small",
                        "name": "Frank",
                        "type": "Person",
                        "uuid": feature_id,
                    },
                    "type": "Feature",
                },
            ]
        )

        df = gpd.GeoDataFrame.from_features(fc.features, crs="EPSG:4326")

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.mock_response(
            "GET",
            f"/products/{self.product_id}/features/{feature_id}",
            body=buffer.read(),
            headers={"is_spatial": "True"},
        )

        feature = table.get_feature(feature_id)

        assert isinstance(feature, Feature)
        assert feature.id == f"{self.product_id}:{feature_id}"
        assert feature.product_id == self.product_id
        assert feature.name == feature_id
        assert feature.is_spatial is True
        assert feature.values == {
            "geometry": df.iloc[0]["geometry"],
            "color": "red",
            "size": "small",
            "name": "Frank",
            "type": "Person",
            "uuid": feature_id,
        }

    @responses.activate
    def test_nonspatial_get_feature(self):
        # Get the table
        table_response = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": False,
            "model": NonSpatialModel.model_json_schema(),
            "created": "2024-01-01T00:00:00.000000",
        }

        self.mock_response("GET", f"/products/{self.product_id}", json=table_response)

        table = Table.get(self.product_id)

        feature_id = "1234"

        df = pd.DataFrame(
            {
                "color": ["red"],
                "size": ["small"],
                "name": ["Frank"],
                "type": ["Person"],
                "uuid": [feature_id],
            }
        )

        buffer = BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)

        self.mock_response(
            "GET",
            f"/products/{self.product_id}/features/{feature_id}",
            body=buffer.read(),
            headers={"is_spatial": "False"},
        )

        feature = table.get_feature(feature_id)

        assert isinstance(feature, Feature)
        assert feature.id == f"{self.product_id}:{feature_id}"
        assert feature.product_id == self.product_id
        assert feature.name == feature_id
        assert feature.is_spatial is False
        assert feature.values == {
            "color": "red",
            "size": "small",
            "name": "Frank",
            "type": "Person",
            "uuid": feature_id,
        }

    @responses.activate
    def test_delete_table(self):
        # Get the table
        table_response = {
            "id": self.product_id,
            "name": self.table_name,
            "description": self.table_description,
            "is_spatial": True,
            "model": SpatialModel.model_json_schema(),
            "created": "2024-01-01T00:00:00.000000",
        }

        self.mock_response("GET", f"/products/{self.product_id}", json=table_response)

        table = Table.get(self.product_id)

        # Delete the table:
        self.mock_response("DELETE", f"/products/{self.product_id}")

        table.delete()
