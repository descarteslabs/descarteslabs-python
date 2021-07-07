import pytest
import responses
import textwrap

from descarteslabs.client.exceptions import NotFoundError

from .base import ClientTestCase
from ..attributes import AttributeValidationError, DocumentState
from ..band import (
    Band,
    MaskBand,
    SpectralBand,
    DerivedBand,
    GenericBand,
    MicrowaveBand,
    ProcessingStepAttribute,
    DerivedParamsAttribute,
)
from ..product import Product


class TestBand(ClientTestCase):
    def test_create(self):
        s = SpectralBand(
            name="test", product_id="foo", physical_range=[0, 1], wavelength_nm_max=1200
        )
        assert "foo:test" == s.id
        assert "spectral" == s.type
        assert 1200 == s.wavelength_nm_max
        assert [0.0, 1.0] == s.physical_range
        with pytest.raises(AttributeError):
            s.frequency  # Attribute from a different band type

        s = SpectralBand(name="test", product=Product(id="foo", _saved=True))
        assert "foo:test" == s.id
        assert "foo" == s.product_id
        assert "product_id" in s._modified

        with pytest.raises(AttributeValidationError):
            s = SpectralBand(id="someid", name="test", product_id="foo")

        with pytest.raises(AttributeValidationError):
            SpectralBand(
                name="test", product_id="foo", product=Product(id="bar", _saved=True)
            )

    def test_constructor_no_id(self):
        s = SpectralBand()
        s.name = "test"
        s.product_id = "foo"
        assert "foo:test" == s.id
        assert "test" == s.name
        assert "foo" == s.product_id

    def test_constructor_no_name_and_product_id(self):
        s = SpectralBand()
        s.id = "foo:test"
        assert "foo:test" == s.id
        assert "test" == s.name
        assert "foo" == s.product_id

    def test_constructor_bad_id(self):
        with pytest.raises(AttributeValidationError):
            s = SpectralBand()
            s.id = "foo"

    def test_set_id_using_type(self):
        s = SpectralBand()
        s.name = "test"
        s.product_id = "foo"
        assert "foo:test" == s.id
        s._get_attribute_type("id").__set__(s, "foo:test")

    @responses.activate
    def test_get_subtype(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "readers": [],
                        "writers": [],
                        "owners": ["org:descarteslabs"],
                        "modified": "2019-06-11T23:31:33.714883Z",
                        "created": "2019-06-11T23:31:33.714883Z",
                        "name": "blue",
                        "product_id": "p1",
                        "type": "spectral",
                        "physical_range": [0.0, 1.0],
                        "wavelength_nm_min": 2000,
                    },
                    "type": "band",
                    "id": "p1:blue",
                },
                "included": [
                    {"attributes": {"name": "P1"}, "id": "p1", "type": "product"}
                ],
                "jsonapi": {"version": "1.0"},
            },
        )

        b = Band.get("p1:blue", client=self.client)
        assert isinstance(b, SpectralBand)
        assert [0.0, 1.0] == b.physical_range
        assert 2000 == b.wavelength_nm_min
        assert "P1" == b.product.name

        b_repr = repr(b)
        match_str = """\
            SpectralBand: blue
              id: p1:blue
              product: p1
              created: Tue Jun 11 23:31:33 2019"""
        assert b_repr.strip("\n") == textwrap.dedent(match_str)

        b = SpectralBand.get("p1:blue", client=self.client)
        assert isinstance(b, SpectralBand)
        assert 2000 == b.wavelength_nm_min

    @responses.activate
    def test_list_subtype(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 2},
                "data": [
                    {
                        "attributes": {
                            "readers": [],
                            "writers": [],
                            "owners": ["org:descarteslabs"],
                            "modified": "2019-06-11T23:31:33.714883Z",
                            "created": "2019-06-11T23:31:33.714883Z",
                            "name": "blue",
                            "product_id": "p1",
                            "type": "spectral",
                            "wavelength_nm_min": 2000,
                        },
                        "type": "band",
                        "id": "p1:blue",
                    },
                    {
                        "attributes": {
                            "readers": [],
                            "writers": [],
                            "owners": ["org:descarteslabs"],
                            "modified": "2019-06-11T23:31:33.714883Z",
                            "created": "2019-06-11T23:31:33.714883Z",
                            "name": "alpha",
                            "product_id": "p1",
                            "type": "mask",
                        },
                        "type": "band",
                        "id": "p1:alpha",
                    },
                ],
                "links": {"self": "https://www.example.com/catalog/v2/bands"},
                "jsonapi": {"version": "1.0"},
            },
        )

        results = list(Band.search(client=self.client))
        assert 2 == len(results)
        assert isinstance(results[0], SpectralBand)
        assert isinstance(results[1], MaskBand)

    def test_vendor_band_name(self):
        s = SpectralBand(
            name="test",
            product_id="foo",
        )
        assert s.vendor_band_name is None

        s = SpectralBand(
            name="test",
            product_id="foo",
            vendor_band_name=None,
        )
        assert s.vendor_band_name is None

        s = SpectralBand(name="test", product_id="foo", vendor_band_name="some_band")
        assert "some_band" == s.vendor_band_name

    def test_search(self):
        search = Band.search()
        assert search._filter_properties is None

        search = MaskBand.search()
        assert search._serialize_filters() == [
            {"name": "type", "val": "mask", "op": "eq"}
        ]

    def test_instantiate_band(self):
        with pytest.raises(TypeError):
            Band(name="test", product_id="foo", wavelength_nm_max=1200)

    def test_id(self):
        product_id = "some_product_id"
        band_name = "some_band_name"
        id = "{}:{}".format(product_id, band_name)

        # All successful permutations for id, product_id, and name
        b = SpectralBand(id=id)
        self.assertEqual(b.id, id)
        self.assertEqual(b.product_id, product_id)
        self.assertEqual(b.name, band_name)

        b = SpectralBand(id=id, name=band_name)
        self.assertEqual(b.id, id)
        self.assertEqual(b.product_id, product_id)
        self.assertEqual(b.name, band_name)

        b = SpectralBand(id=id, product_id=product_id)
        self.assertEqual(b.id, id)
        self.assertEqual(b.product_id, product_id)
        self.assertEqual(b.name, band_name)

        b = SpectralBand(product_id=product_id, name=band_name)
        self.assertEqual(b.id, id)
        self.assertEqual(b.product_id, product_id)
        self.assertEqual(b.name, band_name)

        b = SpectralBand(id=id, product_id=product_id, name=band_name)
        self.assertEqual(b.id, id)
        self.assertEqual(b.product_id, product_id)
        self.assertEqual(b.name, band_name)

        # Verify failures
        with pytest.raises(AttributeValidationError):
            b = SpectralBand(id=id, product_id="foo")

        with pytest.raises(AttributeValidationError):
            b = SpectralBand(id=id, name="foo")

        with pytest.raises(AttributeValidationError):
            b = SpectralBand(id=id, product_id="foo", name="foo")

        with pytest.raises(AttributeValidationError):
            b = SpectralBand(id=band_name, product_id="foo", name="foo")

        with pytest.raises(AttributeValidationError):
            b = SpectralBand(id=band_name)

        with pytest.raises(AttributeValidationError):
            b = SpectralBand(id=band_name, name=band_name)

        with pytest.raises(AttributeValidationError):
            b = SpectralBand(id=band_name, product_id=product_id, name="foo")

    def test_name_with_colon(self):
        product_id = "some_product_id"
        band_name = "some:band:name"
        id = "{}:{}".format(product_id, band_name)

        # Verify that V1 data will deserialize correctly
        b = SpectralBand(id=id, name=band_name, product_id=product_id, _saved=True)
        self.assertEqual(b.id, id)
        self.assertEqual(b.product_id, product_id)
        self.assertEqual(b.name, band_name)

    def test_processing_levels_create(self):
        band_id = "some_product_id:band"

        b = SpectralBand(id=band_id)
        self.assertIsNone(b.processing_levels)

        b = SpectralBand(id=band_id, processing_levels={"default": "toa", "toa": []})
        self.assertEqual(b.processing_levels, {"default": "toa", "toa": []})

        b = SpectralBand(
            id=band_id,
            processing_levels={
                "default": "toa_reflectance",
                "toa_reflectance": [
                    {"function": "fun", "parameter": "param", "index": 0}
                ],
            },
        )
        self.assertEqual(
            b.processing_levels,
            {
                "default": "toa_reflectance",
                "toa_reflectance": [
                    ProcessingStepAttribute(function="fun", parameter="param", index=0)
                ],
            },
        )

        with pytest.raises(AttributeValidationError):
            SpectralBand(id=band_id, processing_levels={"default": 1})

        with pytest.raises(AttributeValidationError):
            SpectralBand(id=band_id, processing_levels=[])

        with pytest.raises(AttributeValidationError):
            SpectralBand(
                id=band_id, processing_levels={"default": "toa", "toa": ["string"]}
            )

        with pytest.raises(AttributeValidationError):
            SpectralBand(
                id=band_id,
                processing_levels={
                    "default": "toa_reflectance",
                    "toa_reflectance": [
                        {"function": "fun", "parameter": "param", "index": "foo"}
                    ],
                },
            )

        with pytest.raises(AttributeValidationError):
            SpectralBand(
                id=band_id,
                processing_levels={
                    "default": "toa_reflectance",
                    "toa_reflectance": [
                        {
                            "function": "fun",
                            "parameter": "param",
                            "index": 0,
                            "foo": "bar",
                        }
                    ],
                },
            )

    def test_processing_levels_modified(self):
        band_id = "some_product_id:band"
        pl = {
            "default": "surface_reflectance",
            "surface_reflectance": [
                {
                    "function": "gain_bias",
                    "parameter": "reflectance_gain_bias",
                    "index": 0,
                }
            ],
        }
        b = SpectralBand(
            id=band_id,
            processing_levels=pl,
            _saved=True,
        )

        assert b.state == DocumentState.SAVED

        b.processing_levels["surface_reflectance"][0].index = 1
        assert b.state == DocumentState.MODIFIED

        # reset modified state
        b._modified = set()
        assert b.state == DocumentState.SAVED

        b.processing_levels["surface_reflectance"].append(
            {"function": "gain_bias", "parameter": "reflectance_gain_bias", "index": 2}
        )
        assert b.state == DocumentState.MODIFIED

    @responses.activate
    def test_processing_levels_io(self):
        band_id = "some_product_id:band"
        pl = {
            "default": "surface_reflectance",
            "surface_reflectance": [
                {
                    "function": "gain_bias",
                    "parameter": "reflectance_gain_bias",
                    "index": 0,
                }
            ],
        }

        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "readers": [],
                        "writers": [],
                        "owners": ["org:descarteslabs"],
                        "modified": "2019-06-11T23:31:33.714883Z",
                        "created": "2019-06-11T23:31:33.714883Z",
                        "name": "band",
                        "product_id": "some_product_id",
                        "type": "spectral",
                        "processing_levels": pl,
                    },
                    "type": "band",
                    "id": "p1:blue",
                },
                "links": {
                    "self": "https://www.example.com/catalog/v2/bands/{}".format(
                        band_id
                    )
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        b = SpectralBand(
            id=band_id,
            processing_levels=pl,
            client=self.client,
        )
        assert isinstance(
            b.processing_levels["surface_reflectance"][0], ProcessingStepAttribute
        )
        b.save()
        assert isinstance(
            b.processing_levels["surface_reflectance"][0], ProcessingStepAttribute
        )
        assert self.get_request_body(0)["data"]["attributes"]["processing_levels"] == pl

    def test_derived_params_create(self):
        band_id = "some_product_id:band"

        b = SpectralBand(id=band_id)
        self.assertIsNone(b.derived_params)

        b = SpectralBand(id=band_id, derived_params=None)
        self.assertIsNone(b.derived_params)

        b = SpectralBand(
            id=band_id, derived_params={"function": "function", "bands": ["band"]}
        )
        self.assertEqual(
            b.derived_params,
            DerivedParamsAttribute(function="function", bands=["band"]),
        )

        b = SpectralBand(
            id=band_id,
            derived_params={
                "function": "function",
                "bands": ["band"],
                "source_type": "UInt16",
            },
        )
        self.assertEqual(
            b.derived_params,
            DerivedParamsAttribute(
                function="function", bands=["band"], source_type="UInt16"
            ),
        )

        with pytest.raises(AttributeValidationError):
            SpectralBand(id=band_id, derived_params={})

        with pytest.raises(AttributeValidationError):
            SpectralBand(id=band_id, derived_params={"function": 1, "bands": ["band"]})

        with pytest.raises(AttributeValidationError):
            SpectralBand(
                id=band_id, derived_params={"function": "function", "bands": [1]}
            )

        with pytest.raises(AttributeValidationError):
            SpectralBand(
                id=band_id,
                derived_params={
                    "function": "function",
                    "bands": ["band"],
                    "source_type": "not a data type",
                },
            )

    def test_derived_params_modified(self):
        band_id = "some_product_id:band"
        dp = {
            "function": "function",
            "bands": ["band"],
            "source_type": "UInt16",
        }
        b = SpectralBand(
            id=band_id,
            derived_params=dp,
            _saved=True,
        )

        assert b.state == DocumentState.SAVED

        b.derived_params.source_type = "UInt32"
        assert b.state == DocumentState.MODIFIED

        # reset modified state
        b._modified = set()
        assert b.state == DocumentState.SAVED

        b.derived_params.bands.append("band2")
        assert b.state == DocumentState.MODIFIED

    @responses.activate
    def test_derived_params_io(self):
        band_id = "some_product_id:band"
        dp = {
            "function": "function",
            "bands": ["band"],
            "source_type": "UInt16",
        }

        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "readers": [],
                        "writers": [],
                        "owners": ["org:descarteslabs"],
                        "modified": "2019-06-11T23:31:33.714883Z",
                        "created": "2019-06-11T23:31:33.714883Z",
                        "name": "band",
                        "product_id": "some_product_id",
                        "type": "spectral",
                        "derived_params": dp,
                    },
                    "type": "band",
                    "id": "p1:blue",
                },
                "links": {
                    "self": "https://www.example.com/catalog/v2/bands/{}".format(
                        band_id
                    )
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        b = SpectralBand(
            id=band_id,
            derived_params=dp,
            client=self.client,
        )
        assert isinstance(b.derived_params, DerivedParamsAttribute)
        b.save()
        assert isinstance(b.derived_params, DerivedParamsAttribute)
        assert self.get_request_body(0)["data"]["attributes"]["derived_params"] == dp


class TestDerivedBand(ClientTestCase):
    def test_invalid_ops(self):
        saved_dband = DerivedBand(
            id="existing_band", name="an existing band", _saved=True
        )
        with pytest.raises(NotImplementedError):
            saved_dband.delete()
        with pytest.raises(AttributeValidationError):
            saved_dband.name = "updated name"
        with pytest.raises(NotImplementedError):
            saved_dband.save()

    @responses.activate
    def test_get(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "owners": ["org:descarteslabs"],
                        "description": None,
                        "extra_properties": {},
                        "tags": None,
                        "bands": ["blue"],
                        "data_range": [0.0, 255.0],
                        "data_type": "Byte",
                        "physical_range": None,
                        "function_name": "test",
                        "name": "derived:prod1:alpha",
                    },
                    "type": "derived_band",
                    "id": "derived:prod1:alpha",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        b = DerivedBand.get("prod1:alpha", client=self.client)
        assert b.bands == ["blue"]
        assert b.function_name == "test"

    @responses.activate
    def test_list(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 5},
                "data": [
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "description": None,
                            "extra_properties": {},
                            "tags": None,
                            "bands": ["blue"],
                            "data_range": [0.0, 255.0],
                            "data_type": "Byte",
                            "physical_range": None,
                            "function_name": "test",
                            "name": "derived:prod1:alpha",
                        },
                        "type": "derived_band",
                        "id": "derived:prod1:alpha",
                    },
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "description": None,
                            "extra_properties": {},
                            "tags": None,
                            "bands": ["blue"],
                            "data_range": [0.0, 255.0],
                            "data_type": "Byte",
                            "physical_range": None,
                            "function_name": "test",
                            "name": "derived:prod1:green",
                        },
                        "type": "derived_band",
                        "id": "derived:prod1:green",
                    },
                ],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/derived_bands"},
            },
        )

        derived_bands = list(DerivedBand.search(client=self.client))
        assert len(derived_bands) == 2
        assert isinstance(derived_bands[0], DerivedBand)

    @responses.activate
    def test_related_product(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "product",
                    "attributes": {
                        "resolution_max": None,
                        "resolution_min": None,
                        "readers": [],
                        "start_datetime": None,
                        "modified": "2020-01-09T16:45:25.913037Z",
                        "created": "2020-01-09T16:45:25.913037Z",
                        "owners": ["org:descarteslabs", "user:someone"],
                        "revisit_period_minutes_min": None,
                        "writers": [],
                        "tags": [],
                        "extra_properties": {},
                        "description": None,
                        "revisit_period_minutes_max": None,
                        "end_datetime": None,
                        "name": "Product 1",
                        "is_core": False,
                    },
                    "id": "p1",
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "band",
                    "attributes": {
                        "nodata": None,
                        "created": "2020-01-09T16:46:20.094904Z",
                        "data_type": "Float32",
                        "wavelength_nm_fwhm": None,
                        "type": "spectral",
                        "readers": [],
                        "wavelength_nm_center": None,
                        "display_range": [0.0, 255.0],
                        "file_index": 0,
                        "jpx_layer_index": 0,
                        "tags": [],
                        "wavelength_nm_max": None,
                        "band_index": 0,
                        "wavelength_nm_min": None,
                        "sort_order": 1,
                        "extra_properties": {},
                        "modified": "2020-01-09T16:46:20.094904Z",
                        "name": "b1",
                        "product_id": "p1",
                        "description": None,
                        "owners": ["org:descarteslabs", "user:someone"],
                        "data_range": [0.0, 1962239500502.9294],
                        "resolution": None,
                        "writers": [],
                    },
                    "relationships": {
                        "product": {"data": {"type": "product", "id": "p1"}}
                    },
                    "id": "p1:b1",
                },
                "included": [
                    {
                        "type": "product",
                        "attributes": {
                            "revisit_period_minutes_min": None,
                            "created": "2020-01-09T16:45:25.913037Z",
                            "resolution_min": None,
                            "resolution_max": None,
                            "readers": [],
                            "revisit_period_minutes_max": None,
                            "tags": [],
                            "is_core": False,
                            "end_datetime": None,
                            "start_datetime": None,
                            "extra_properties": {},
                            "modified": "2020-01-09T16:45:25.913037Z",
                            "name": "Product 1",
                            "description": None,
                            "owners": ["org:descarteslabs", "user:someone"],
                            "writers": [],
                        },
                        "id": "p1",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
        )
        b = SpectralBand(name="b1", product_id="p1", client=self.client, _saved=True)
        p = b.product
        assert p.id == "p1"
        b.reload()

    def test_make_valid_name(self):
        name = "This is ań @#$^*% ïñvalid name!!!!"
        valid_name = Band.make_valid_name(name)
        assert valid_name == "This_is_a_valid_name_"

    @responses.activate
    def test_get_incorrect_band_type(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "band",
                    "id": "p1:b1",
                    "attributes": {"type": "spectral"},
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        assert MaskBand.get("p1:b1", client=self.client) is None
        assert DerivedBand.get("p1:b1", client=self.client) is None
        assert SpectralBand.get("p1:b1", client=self.client) is not None
        assert Band.get("p1:b1", client=self.client) is not None

    @responses.activate
    def test_get_many_incorrect_band_type(self):
        self.mock_response(
            responses.PUT,
            {
                "data": [
                    {"type": "band", "id": "p1:b1", "attributes": {"type": "spectral"}},
                    {"type": "band", "id": "p1:b2", "attributes": {"type": "spectral"}},
                    {"type": "band", "id": "p1:b3", "attributes": {"type": "mask"}},
                    {
                        "type": "band",
                        "id": "p1:b4",
                        "attributes": {"type": "microwave"},
                    },
                ],
                "jsonapi": {"version": "1.0"},
            },
        )
        all_bands = ["p1:b1", "p1:b2", "p1:b3", "p1:b4"]
        more_bands = ["p1:b1", "p1:b2", "p1:b3", "p1:b4", "p1:b5"]

        assert len(MaskBand.get_many(all_bands, client=self.client)) == 1
        assert len(GenericBand.get_many(all_bands, client=self.client)) == 0
        assert len(SpectralBand.get_many(all_bands, client=self.client)) == 2
        assert len(Band.get_many(all_bands, client=self.client)) == 4

        with self.assertRaises(NotFoundError):
            GenericBand.get_many(more_bands, client=self.client)

        assert (
            len(
                GenericBand.get_many(
                    more_bands, ignore_missing=True, client=self.client
                )
            )
            == 0
        )
        assert (
            len(
                MicrowaveBand.get_many(
                    more_bands, ignore_missing=True, client=self.client
                )
            )
            == 1
        )
        assert (
            len(Band.get_many(more_bands, ignore_missing=True, client=self.client)) == 4
        )
