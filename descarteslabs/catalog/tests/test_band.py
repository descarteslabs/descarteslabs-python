import pytest
import responses
import textwrap

from .base import ClientTestCase
from ..attributes import AttributeValidationError
from ..band import Band, MaskBand, SpectralBand, DerivedBand
from ..product import Product


class TestBand(ClientTestCase):
    def test_create(self):
        s = SpectralBand(name="test", product_id="foo", wavelength_nm_max=1200)
        assert "foo:test" == s.id
        assert "spectral" == s.type
        assert 1200 == s.wavelength_nm_max
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

        s = SpectralBand()
        s.id = "foo:test"
        assert "foo:test" == s.id
        assert "test" == s.name
        assert "foo" == s.product_id

        with pytest.raises(AttributeValidationError):
            s = SpectralBand()
            s.id = "foo"

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


class TestDerivedBand(ClientTestCase):
    def test_invalid_ops(self):
        saved_dband = DerivedBand(
            id="existing_band", name="an existing band", _saved=True
        )
        with pytest.raises(NotImplementedError):
            saved_dband.delete()

        with pytest.raises(NotImplementedError):
            saved_dband.name = "updated name"
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
