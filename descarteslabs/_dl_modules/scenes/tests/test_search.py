import json
import pytest
import unittest
from unittest.mock import patch
import responses

from ...common.geo import AOI

from ..search_api import (
    search,
    search_products,
    get_product,
    search_bands,
    get_band,
    search_derived_bands,
    get_derived_band,
)

from ...catalog.search import Search
from ...common.property_filtering import Properties
from .. import scene as scene_module
from ...catalog import catalog_base
from ...catalog import image_collection as image_collection_module
from ...catalog.tests.base import ClientTestCase
from .mock_data import _image_search, _cached_bands_by_product, catalog_mock_data


class TestScenesSearch(unittest.TestCase):
    geom = {
        "coordinates": (
            (
                (-95.836498, 39.278486),
                (-92.068696, 39.278486),
                (-92.068696, 42.799988),
                (-95.836498, 42.799988),
                (-95.836498, 39.278486),
            ),
        ),
        "type": "Polygon",
    }

    @patch.object(Search, "__iter__", _image_search)
    @patch.object(scene_module, "cached_bands_by_product", _cached_bands_by_product)
    @patch.object(
        image_collection_module,
        "cached_bands_by_product",
        catalog_mock_data._cached_bands_by_product,
    )
    def test_search_geom(self):
        sc, ctx = search(self.geom, products="landsat:LC08:PRE:TOAR", limit=4)

        assert len(sc) == 2
        assert sc.each.properties.id.combine(tuple) == (
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
        )

        assert isinstance(ctx, AOI)
        assert ctx.__geo_interface__ == self.geom
        assert ctx.resolution == 15
        assert ctx.crs == "EPSG:32615"

        for scene in sc:
            # allow for changes in publicly available data
            assert len(scene.properties.bands) == 24
            assert "derived:ndvi" in scene.properties.bands

    @patch.object(Search, "__iter__", _image_search)
    @patch.object(scene_module, "cached_bands_by_product", _cached_bands_by_product)
    @patch.object(
        image_collection_module,
        "cached_bands_by_product",
        catalog_mock_data._cached_bands_by_product,
    )
    def test_search(self):
        sc, ctx = search(
            self.geom,
            products=["landsat:LC08:PRE:TOAR"],
            start_datetime="2016-07-06",
            end_datetime="2016-07-16",
            cloud_fraction=0.6,
            storage_state="available",
            sort_order="desc",
            query=(Properties().bright_fraction < 0.3),
            limit=4,
        )

        assert len(sc) == 2
        assert sc.each.properties.id.combine(tuple) == (
            "landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1",
            "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1",
        )

    def test_search_no_aoi(self):
        with pytest.raises(ValueError):
            search(AOI(), products=["landsat:LC08:PRE:TOAR"])


class TestProductBandSearch(ClientTestCase):
    @responses.activate
    def test_get_product(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "owners": ["org:descarteslabs"],
                        "name": "My Product",
                        "readers": [],
                        "modified": "2019-06-11T23:59:46.800792Z",
                        "created": "2019-06-11T23:52:35.114938Z",
                        "start_datetime": None,
                        "writers": [],
                        "end_datetime": None,
                        "description": "A descriptive description",
                        "foobar": "unkown",
                        "v1_properties": {
                            "id": "descarteslabs:my-product",
                            "name": "My Product",
                            "description": "A descriptive description",
                        },
                    },
                    "type": "product",
                    "id": "descarteslabs:my-product",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        with patch.object(catalog_base.CatalogClient, "_instance", self.client):
            p = get_product("descarteslabs:my-product")
        assert p.id == "descarteslabs:my-product"
        assert p.name == "My Product"

        req = responses.calls[0].request
        assert req.url.endswith("?v1_compatibility=True")

    @responses.activate
    def test_product_search(self):
        self.mock_response(
            responses.PUT,
            {
                "data": [
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "name": "My Product",
                            "readers": [],
                            "modified": "2019-06-11T23:59:46.800792Z",
                            "created": "2019-06-11T23:52:35.114938Z",
                            "start_datetime": None,
                            "writers": [],
                            "end_datetime": None,
                            "description": "A descriptive description",
                            "foobar": "unkown",
                            "v1_properties": {
                                "id": "descarteslabs:my-product",
                                "name": "My Product",
                                "description": "A descriptive description",
                            },
                        },
                        "type": "product",
                        "id": "descarteslabs:my-product",
                    },
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "name": "Another Product",
                            "readers": [],
                            "modified": "2019-06-11T23:59:46.800792Z",
                            "created": "2019-06-11T23:52:35.114938Z",
                            "start_datetime": None,
                            "writers": [],
                            "end_datetime": None,
                            "description": "A descriptive description",
                            "foobar": "unkown",
                            "v1_properties": {
                                "id": "descarteslabs:another-product",
                                "name": "Another Product",
                                "description": "A descriptive description",
                            },
                        },
                        "type": "product",
                        "id": "descarteslabs:another-product",
                    },
                ],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/products"},
            },
        )

        with patch.object(catalog_base.CatalogClient, "_instance", self.client):
            products = search_products(
                limit=2, owner="org:descarteslabs", text="description"
            )

        assert len(products) == 2
        assert products[0].id == "descarteslabs:my-product"
        assert products[0].name == "My Product"
        assert products[1].id == "descarteslabs:another-product"
        assert products[1].name == "Another Product"

        req = responses.calls[0].request
        body = json.loads(req.body)
        assert (
            body["filter"] == r'[{"op":"eq","name":"owners","val":"org:descarteslabs"}]'
        )
        assert body["limit"] == 2
        assert body["text"] == "description"
        assert body["v1_compatibility"] is True

    @responses.activate
    def test_get_band(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "type": "spectral",
                        "name": "my-band",
                        "v1_properties": {
                            "type": "spectral",
                            "id": "descarteslabs:my-product:my-band",
                            "name": "my-band",
                        },
                    },
                    "type": "band",
                    "id": "descarteslabs:my-product:my-band",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        with patch.object(catalog_base.CatalogClient, "_instance", self.client):
            b = get_band("descarteslabs:my-product:my-band")
        assert b.id == "descarteslabs:my-product:my-band"
        assert b.name == "my-band"

        req = responses.calls[0].request
        assert "v1_compatibility=True" in req.url

    @responses.activate
    def test_band_search(self):
        self.mock_response(
            responses.PUT,
            {
                "data": [
                    {
                        "attributes": {
                            "type": "spectral",
                            "name": "my-band",
                            "v1_properties": {
                                "type": "spectral",
                                "id": "descarteslabs:my-product:my-band",
                                "name": "my-band",
                            },
                        },
                        "type": "band",
                        "id": "descarteslabs:my-product:my-band",
                    },
                    {
                        "attributes": {
                            "type": "spectral",
                            "name": "my-other-band",
                            "v1_properties": {
                                "type": "spectral",
                                "id": "descarteslabs:my-product:my-other-band",
                                "name": "my-other-band",
                            },
                        },
                        "type": "band",
                        "id": "descarteslabs:my-product:my-other-band",
                    },
                ],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/bands"},
            },
        )

        with patch.object(catalog_base.CatalogClient, "_instance", self.client):
            bands = search_bands(
                products="descarteslabs:my-product",
                limit=2,
                wavelength=700,
                resolution=100,
                tags=["sometag"],
            )

        assert len(bands) == 2
        assert bands[0].id == "descarteslabs:my-product:my-band"
        assert bands[0].name == "my-band"
        assert bands[0].type == "spectral"
        assert bands[1].id == "descarteslabs:my-product:my-other-band"
        assert bands[1].name == "my-other-band"
        assert bands[1].type == "spectral"

        req = responses.calls[0].request
        body = json.loads(req.body)
        filters = json.loads(body["filter"])
        assert {"op": "eq", "name": "type", "val": "spectral"} in filters
        assert {
            "op": "eq",
            "name": "product_id",
            "val": "descarteslabs:my-product",
        } in filters
        assert {"name": "wavelength_nm_min", "op": "lte", "val": 700} in filters
        assert {"name": "wavelength_nm_max", "op": "gte", "val": 700} in filters
        assert {"op": "eq", "name": "resolution", "val": 100} in filters
        assert {"op": "eq", "name": "tags", "val": "sometag"} in filters
        assert body["limit"] == 2
        assert body["v1_compatibility"] is True

    @responses.activate
    def test_get_derived_band(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "name": "derived:ndvi",
                        "bands": ["nir", "red"],
                        "data_range": [0, 65535],
                        "physical_range": [-1.0, 1.0],
                        "function_name": "ndi_uint16",
                        "data_type": "UInt16",
                        "v1_properties": {
                            "id": "derived:ndvi",
                            "name": "derived:ndvi",
                            "bands": ["nir", "red"],
                            "data_range": [0, 65535],
                            "physical_range": [-1.0, 1.0],
                            "function_name": "ndi_uint16",
                            "data_type": "UInt16",
                        },
                    },
                    "type": "derived_band",
                    "id": "derived:ndvi",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        with patch.object(catalog_base.CatalogClient, "_instance", self.client):
            b = get_derived_band("derived:ndvi")
        assert b.id == "derived:ndvi"
        assert b.name == "derived:ndvi"

        req = responses.calls[0].request
        assert "v1_compatibility=True" in req.url

    @responses.activate
    def test_derived_band_search(self):
        self.mock_response(
            responses.PUT,
            {
                "data": [
                    {
                        "attributes": {
                            "name": "derived:ndvi",
                            "bands": ["nir", "red"],
                            "data_range": [0, 65535],
                            "physical_range": [-1.0, 1.0],
                            "function_name": "ndi_uint16",
                            "data_type": "UInt16",
                            "v1_properties": {
                                "id": "derived:ndvi",
                                "name": "derived:ndvi",
                                "bands": ["nir", "red"],
                                "data_range": [0, 65535],
                                "physical_range": [-1.0, 1.0],
                                "function_name": "ndi_uint16",
                                "data_type": "UInt16",
                            },
                        },
                        "type": "derived_band",
                        "id": "derived:ndvi",
                    },
                    {
                        "attributes": {
                            "name": "derived:evi",
                            "bands": ["blue", "red", "nir"],
                            "data_range": [0, 65535],
                            "physical_range": [-1.0, 1.0],
                            "function_name": "evi_uint16",
                            "data_type": "UInt16",
                            "v1_properties": {
                                "id": "derived:evi",
                                "name": "derived:evi",
                                "bands": ["blue", "red", "nir"],
                                "data_range": [0, 65535],
                                "physical_range": [-1.0, 1.0],
                                "function_name": "evi_uint16",
                                "data_type": "UInt16",
                            },
                        },
                        "type": "derived_band",
                        "id": "derived:evi",
                    },
                ],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/derived_bands"},
            },
        )

        with patch.object(catalog_base.CatalogClient, "_instance", self.client):
            bands = search_derived_bands(
                bands="red",
                limit=2,
            )

        assert len(bands) == 2
        assert bands[0].id == "derived:ndvi"
        assert bands[0].name == "derived:ndvi"
        assert bands[1].id == "derived:evi"
        assert bands[1].name == "derived:evi"

        req = responses.calls[0].request
        body = json.loads(req.body)
        assert body["filter"] == r'[{"op":"eq","name":"bands","val":"red"}]'
        assert body["limit"] == 2
        assert body["v1_compatibility"] is True
