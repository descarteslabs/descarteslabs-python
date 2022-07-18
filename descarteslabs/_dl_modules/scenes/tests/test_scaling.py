import pytest
import unittest

from .._scaling import scaling_parameters, multiproduct_scaling_parameters


class TestScaling(unittest.TestCase):

    RGBA_PROPERTIES = {
        "red": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 10000],
            "default_range": [0, 4000],
            "physical_range": [0.0, 1.0],
        },
        "green": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 10000],
            "default_range": [0, 4000],
            "physical_range": [0.0, 1.0],
        },
        "blue": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 10000],
            "default_range": [0, 4000],
            "physical_range": [0.0, 1.0],
        },
        "alpha": {"type": "mask", "dtype": "UInt16", "data_range": [0, 1]},
    }

    RGBTA_PROPERTIES_PL = {
        "red": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 65535],
            "processing_levels": {
                "DN": [],
                "default": "toa_reflectance",
                "toa": "toa_reflectance",
                "toa_reflectance": [
                    {
                        "function": "gain_bias",
                        "parameter": "reflectance_gain_bias",
                        "index": 2,
                    },
                ],
                "toa_radiance": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 2,
                        "data_type": "Float64",
                        "data_range": [0.0, 100.0],
                    },
                ],
            },
        },
        "green": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 65535],
            "processing_levels": {
                "DN": [],
                "default": "toa_reflectance",
                "toa": "toa_reflectance",
                "toa_reflectance": [
                    {
                        "function": "gain_bias",
                        "parameter": "reflectance_gain_bias",
                        "index": 2,
                    },
                ],
                "toa_radiance": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 2,
                        "data_type": "Float64",
                        "data_range": [0.0, 100.0],
                    },
                ],
            },
        },
        "blue": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 65535],
            "processing_levels": {
                "DN": [],
                "default": "toa_reflectance",
                "toa": "toa_reflectance",
                "toa_reflectance": [
                    {
                        "function": "gain_bias",
                        "parameter": "reflectance_gain_bias",
                        "index": 2,
                    },
                ],
                "toa_radiance": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 2,
                        # not realistic but to test the edge case
                        "data_type": "UInt16",
                        "data_range": [0.0, 100.0],
                    },
                ],
            },
        },
        "tirs1": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 65535],
            "processing_levels": {
                "DN": [],
                "default": "toa_brightness_temperature",
                "toa": "toa_brightness_temperature",
                "toa_brightness_temperature": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 10,
                    },
                    {
                        "function": "brightness_temperature",
                        "parameter": "brightness_temperature_k1_k2",
                        "index": 1,
                        "data_type": "Float64",
                        "data_range": [0.0, 333.0],
                        # not realistic, but to test the edge case
                        "physical_range": [0.0, 1.0],
                    },
                ],
                "toa_radiance": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 10,
                        "data_type": "Float64",
                        "data_range": [0.0, 100.0],
                        # not realistic, but to test the edge case
                        "physical_range": [0.0, 1.0],
                    },
                ],
            },
        },
        "alpha": {"type": "mask", "dtype": "UInt16", "data_range": [0, 1]},
    }

    def test_raw_data_type(self):
        mock_properties = {
            "one": dict(type="spectral", dtype="UInt16"),
            "two": dict(type="spectral", dtype="UInt16"),
            "derived:three": dict(dtype="UInt16"),
            "derived:one": dict(dtype="UInt16"),
            "its_a_byte": dict(type="class", dtype="Byte"),
            "signed": dict(type="spectral", dtype="Int16"),
            "future_unknown_type": dict(type="other", dtype="FutureInt16"),
            "alpha": dict(type="mask", dtype="Byte"),
        }

        assert (
            scaling_parameters(mock_properties, ["its_a_byte"], None, None, None)[1]
            == "Byte"
        )
        assert (
            scaling_parameters(mock_properties, ["one", "two"], None, None, None)[1]
            == "UInt16"
        )
        assert (
            scaling_parameters(
                mock_properties, ["its_a_byte", "alpha"], None, None, None
            )[1]
            == "Byte"
        )
        # alpha ignored from common datatype
        assert (
            scaling_parameters(mock_properties, ["one", "alpha"], None, None, None)[1]
            == "UInt16"
        )
        assert (
            scaling_parameters(mock_properties, ["alpha"], None, None, None)[1]
            == "Byte"
        )
        assert (
            scaling_parameters(
                mock_properties,
                ["one", "two", "derived:three", "derived:one"],
                None,
                None,
                None,
            )[1]
            == "UInt16"
        )
        assert (
            scaling_parameters(
                mock_properties, ["one", "its_a_byte"], None, None, None
            )[1]
            == "UInt16"
        )
        assert (
            scaling_parameters(
                mock_properties, ["signed", "its_a_byte"], None, None, None
            )[1]
            == "Int16"
        )
        assert (
            scaling_parameters(mock_properties, ["one", "signed"], None, None, None)[1]
            == "Int32"
        )

        with pytest.raises(ValueError, match="is not available"):
            scaling_parameters(mock_properties, ["one", "woohoo"], None, None, None)
        with pytest.raises(ValueError, match="did you mean"):
            scaling_parameters(
                mock_properties,
                ["one", "three"],
                None,
                None,
                None,
            )  # should hint that derived:three exists
        with pytest.raises(ValueError, match="Invalid data type"):
            scaling_parameters(
                mock_properties, ["its_a_byte", "future_unknown_type"], None, None, None
            )

    def test_scaling_parameters_none(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES, ["red", "green", "blue", "alpha"], None, None, None
        )
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_dtype(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            None,
            "UInt32",
        )
        assert scales is None
        assert data_type == "UInt32"

    def test_scaling_parameters_raw(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES, ["red", "green", "blue", "alpha"], None, "raw", None
        )
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_display(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            "display",
            None,
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "Byte"

    def test_scaling_parameters_display_uint16(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            "display",
            "UInt16",
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "UInt16"

    def test_scaling_parameters_auto(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES, ["red", "green", "blue", "alpha"], None, "auto", None
        )
        assert scales == [(), (), (), None]
        assert data_type == "Byte"

    def test_scaling_parameters_physical(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            "physical",
            None,
        )
        assert scales == [
            (0, 10000, 0.0, 1.0),
            (0, 10000, 0.0, 1.0),
            (0, 10000, 0.0, 1.0),
            None,
        ]
        assert data_type == "Float64"

    def test_scaling_parameters_physical_int32(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            "physical",
            "Int32",
        )
        assert scales == [
            (0, 10000, 0.0, 1.0),
            (0, 10000, 0.0, 1.0),
            (0, 10000, 0.0, 1.0),
            None,
        ]
        assert data_type == "Int32"

    def test_scaling_parameters_bad_mode(self):
        with pytest.raises(ValueError):
            scales, data_type = scaling_parameters(
                self.RGBA_PROPERTIES,
                ["red", "green", "blue", "alpha"],
                None,
                "mode",
                None,
            )

    def test_scaling_parameters_list(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            [(0, 10000), "display", (), None],
            None,
        )
        assert scales == [(0, 10000, 0, 255), (0, 4000, 0, 255), (), None]
        assert data_type == "Byte"

    def test_scaling_parameters_list_alpha(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            [(0, 4000), (0, 4000), (0, 4000), "raw"],
            None,
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "Byte"

    def test_scaling_parameters_list_bad_length(self):
        with pytest.raises(ValueError):
            scales, data_type = scaling_parameters(
                self.RGBA_PROPERTIES,
                ["red", "green", "blue", "alpha"],
                None,
                [(0, 10000), "display", ()],
                None,
            )

    def test_scaling_parameters_list_bad_mode(self):
        with pytest.raises(ValueError):
            scales, data_type = scaling_parameters(
                self.RGBA_PROPERTIES,
                ["red", "green", "blue", "alpha"],
                None,
                [(0, 10000), "mode", (), None],
                None,
            )

    def test_scaling_parameters_dict(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            {"red": "display", "green": (0, 10000), "default_": "auto"},
            None,
        )
        assert scales == [(0, 4000, 0, 255), (0, 10000, 0, 255), (), None]
        assert data_type == "Byte"

    def test_scaling_parameters_dict_default(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            {"red": (0, 4000, 0, 255), "default_": "raw"},
            None,
        )
        assert scales == [(0, 4000, 0, 255), None, None, None]
        assert data_type == "UInt16"

    def test_scaling_parameters_dict_default_none(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            {"red": "display", "green": "display"},
            None,
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), None, None]
        assert data_type == "Byte"

    def test_scaling_parameters_tuple_range(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            [(0, 10000, 0, 255), (0, 4000), (), None],
            None,
        )
        assert scales == [(0, 10000, 0, 255), (0, 4000, 0, 255), (), None]
        assert data_type == "Byte"

    def test_scaling_parameters_tuple_range_uint16(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            [(0, 10000, 0, 10000), (0, 4000), (), None],
            None,
        )
        assert scales == [(0, 10000, 0, 10000), (0, 4000, 0, 65535), (), None]
        assert data_type == "UInt16"

    def test_scaling_parameters_tuple_range_float(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            [(0, 10000, 0, 1.0), (0, 4000), (0, 4000), None],
            None,
        )
        assert scales == [(0, 10000, 0, 1), (0, 4000, 0, 1), (0, 4000, 0, 1), None]
        assert data_type == "Float64"

    def test_scaling_parameters_tuple_pct(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            [("0%", "100%", "0%", "100%"), ("2%", "98%", "2%", "98%"), "display", None],
            None,
        )
        assert scales == [
            (0, 4000, 0, 255),
            (80, 3920, 5, 250),
            (0, 4000, 0, 255),
            None,
        ]
        assert data_type == "Byte"

    def test_scaling_parameters_tuple_pct_float(self):
        scales, data_type = scaling_parameters(
            self.RGBA_PROPERTIES,
            ["red", "green", "blue", "alpha"],
            None,
            [
                ("0%", "100%", "0%", "100%"),
                ("2%", "98%", "2%", "98%"),
                "physical",
                None,
            ],
            None,
        )
        assert scales == [
            (0, 10000, 0, 1),
            (200, 9800, 0.02, 0.98),
            (0, 10000, 0, 1),
            None,
        ]
        assert data_type == "Float64"

    def test_scaling_parameters_bad_data_type(self):
        with pytest.raises(ValueError):
            scales, data_type = scaling_parameters(
                self.RGBA_PROPERTIES,
                ["red", "green", "blue", "alpha"],
                None,
                None,
                "data_type",
            )

    def test_scaling_parameters_pl(self):
        scales, data_type = scaling_parameters(
            self.RGBTA_PROPERTIES_PL,
            ["red", "green", "blue", "alpha"],
            None,
            None,
            None,
        )
        assert scales is None
        assert data_type == "Float64"

    def test_scaling_parameters_pl_ref(self):
        scales, data_type = scaling_parameters(
            self.RGBTA_PROPERTIES_PL,
            ["red", "green", "blue", "alpha"],
            "toa_reflectance",
            None,
            None,
        )
        assert scales is None
        assert data_type == "Float64"

    def test_scaling_parameters_pl_rad(self):
        scales, data_type = scaling_parameters(
            self.RGBTA_PROPERTIES_PL,
            ["red", "green", "blue", "tirs1", "alpha"],
            "toa_radiance",
            None,
            None,
        )
        assert scales is None
        assert data_type == "Float64"

    def test_scaling_parameters_pl_rad_uint16(self):
        scales, data_type = scaling_parameters(
            self.RGBTA_PROPERTIES_PL, ["blue", "alpha"], "toa_radiance", None, None
        )
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_pl_rad_physical(self):
        scales, data_type = scaling_parameters(
            self.RGBTA_PROPERTIES_PL,
            ["red", "green", "blue", "tirs1", "alpha"],
            "toa_radiance",
            "physical",
            None,
        )
        assert scales == [None, None, None, (0.0, 100.0, 0.0, 1.0), None]
        assert data_type == "Float64"

    def test_scaling_parameters_pl_bt(self):
        scales, data_type = scaling_parameters(
            self.RGBTA_PROPERTIES_PL,
            ["tirs1"],
            "toa_brightness_temperature",
            None,
            None,
        )
        assert scales is None
        assert data_type == "Float64"

    def test_scaling_parameters_pl_bt_physical(self):
        scales, data_type = scaling_parameters(
            self.RGBTA_PROPERTIES_PL,
            ["tirs1"],
            "toa_brightness_temperature",
            "physical",
            None,
        )
        assert scales == [(0.0, 333.0, 0.0, 1.0)]
        assert data_type == "Float64"


class TestMultiProductScaling(unittest.TestCase):

    RGBA_PROPERTIES = {
        "red": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 10000],
            "default_range": [0, 4000],
            "physical_range": [0.0, 1.0],
        },
        "green": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 10000],
            "default_range": [0, 4000],
            "physical_range": [0.0, 1.0],
        },
        "blue": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 10000],
            "default_range": [0, 4000],
            "physical_range": [0.0, 1.0],
        },
        "alpha": {"type": "mask", "dtype": "UInt16", "data_range": [0, 1]},
    }

    RGA_PROPERTIES = {
        "red": {
            "type": "spectral",
            "dtype": "Int16",
            "data_range": [0, 10000],
            "default_range": [0, 4000],
            "physical_range": [0.0, 1.0],
        },
        "green": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 10000],
            "default_range": [0, 4000],
            "physical_range": [-1.0, 1.0],
        },
        "alpha": {"type": "mask", "dtype": "UInt16", "data_range": [0, 1]},
    }

    RGBTA_PROPERTIES_PL = {
        "red": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 65535],
            "processing_levels": {
                "DN": [],
                "default": "toa_reflectance",
                "toa": "toa_reflectance",
                "toa_reflectance": [
                    {
                        "function": "gain_bias",
                        "parameter": "reflectance_gain_bias",
                        "index": 2,
                    },
                ],
                "toa_radiance": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 2,
                        "data_type": "Float64",
                        "data_range": [0.0, 100.0],
                    },
                ],
            },
        },
        "green": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 65535],
            "processing_levels": {
                "DN": [],
                "default": "toa_reflectance",
                "toa": "toa_reflectance",
                "toa_reflectance": [
                    {
                        "function": "gain_bias",
                        "parameter": "reflectance_gain_bias",
                        "index": 2,
                    },
                ],
                "toa_radiance": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 2,
                        "data_type": "Float64",
                        "data_range": [0.0, 100.0],
                    },
                ],
            },
        },
        "blue": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 65535],
            "processing_levels": {
                "DN": [],
                "default": "toa_reflectance",
                "toa": "toa_reflectance",
                "toa_reflectance": [
                    {
                        "function": "gain_bias",
                        "parameter": "reflectance_gain_bias",
                        "index": 2,
                    },
                ],
                "toa_radiance": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 2,
                        # not realistic but to test the edge case
                        "data_type": "UInt16",
                        "data_range": [0.0, 100.0],
                    },
                ],
            },
        },
        "tirs1": {
            "type": "spectral",
            "dtype": "UInt16",
            "data_range": [0, 65535],
            "processing_levels": {
                "DN": [],
                "default": "toa_brightness_temperature",
                "toa": "toa_brightness_temperature",
                "toa_brightness_temperature": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 10,
                    },
                    {
                        "function": "brightness_temperature",
                        "parameter": "brightness_temperature_k1_k2",
                        "index": 1,
                        "data_type": "Float64",
                        "data_range": [0.0, 333.0],
                        # not realistic, but to test the edge case
                        "physical_range": [0.0, 1.0],
                    },
                ],
                "toa_radiance": [
                    {
                        "function": "gain_bias",
                        "parameter": "radiance_gain_bias",
                        "index": 10,
                        "data_type": "Float64",
                        "data_range": [0.0, 100.0],
                        # not realistic, but to test the edge case
                        "physical_range": [0.0, 1.0],
                    },
                ],
            },
        },
        "alpha": {"type": "mask", "dtype": "UInt16", "data_range": [0, 1]},
    }

    def test_scaling_parameters_single(self):
        scales, data_type = multiproduct_scaling_parameters(
            {"product1": self.RGBA_PROPERTIES},
            ["red", "green", "blue", "alpha"],
            None,
            None,
            None,
        )
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_none(self):
        scales, data_type = multiproduct_scaling_parameters(
            {"product1": self.RGBA_PROPERTIES, "product2": self.RGBA_PROPERTIES},
            ["red", "green", "blue", "alpha"],
            None,
            None,
            None,
        )
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_display(self):
        scales, data_type = multiproduct_scaling_parameters(
            {"product1": self.RGBA_PROPERTIES, "product2": self.RGBA_PROPERTIES},
            ["red", "green", "blue", "alpha"],
            None,
            "display",
            None,
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "Byte"

    def test_scaling_parameters_missing_band(self):
        with pytest.raises(ValueError, match="not available"):
            scales, data_type = multiproduct_scaling_parameters(
                {"product1": self.RGBA_PROPERTIES, "product3": self.RGA_PROPERTIES},
                ["red", "green", "blue", "alpha"],
                None,
                None,
                None,
            )

    def test_scaling_parameters_none_data_type(self):
        scales, data_type = multiproduct_scaling_parameters(
            {"product1": self.RGBA_PROPERTIES, "product3": self.RGA_PROPERTIES},
            ["red", "alpha"],
            None,
            None,
            None,
        )
        assert scales is None
        assert data_type == "Int32"

    def test_scaling_parameters_display_range(self):
        scales, data_type = multiproduct_scaling_parameters(
            {"product1": self.RGBA_PROPERTIES, "product3": self.RGA_PROPERTIES},
            ["red", "alpha"],
            None,
            "display",
            None,
        )
        assert scales == [(0, 4000, 0, 255), None]
        assert data_type == "Byte"

    def test_scaling_parameters_raw_range(self):
        scales, data_type = multiproduct_scaling_parameters(
            {"product1": self.RGBA_PROPERTIES, "product3": self.RGA_PROPERTIES},
            ["red", "alpha"],
            None,
            "raw",
            None,
        )
        assert scales == [None, None]
        assert data_type == "Int32"

    def test_scaling_parameters_physical_incompatible(self):
        with pytest.raises(ValueError, match="incompatible"):
            scales, data_type = multiproduct_scaling_parameters(
                {"product1": self.RGBA_PROPERTIES, "product3": self.RGA_PROPERTIES},
                ["green", "alpha"],
                None,
                "physical",
                None,
            )

    def test_scaling_parameters_pl(self):
        scales, data_type = multiproduct_scaling_parameters(
            {
                "product1": self.RGBTA_PROPERTIES_PL,
                "product2": self.RGBTA_PROPERTIES_PL,
            },
            ["red", "green", "blue", "alpha"],
            None,
            None,
            None,
        )
        assert scales is None
        assert data_type == "Float64"

    def test_scaling_parameters_pl_ref(self):
        scales, data_type = multiproduct_scaling_parameters(
            {
                "product1": self.RGBTA_PROPERTIES_PL,
                "product2": self.RGBTA_PROPERTIES_PL,
            },
            ["red", "green", "blue", "alpha"],
            "toa_reflectance",
            None,
            None,
        )
        assert scales is None
        assert data_type == "Float64"

    def test_scaling_parameters_pl_rad(self):
        scales, data_type = multiproduct_scaling_parameters(
            {
                "product1": self.RGBTA_PROPERTIES_PL,
                "product2": self.RGBTA_PROPERTIES_PL,
            },
            ["red", "green", "blue", "tirs1", "alpha"],
            "toa_radiance",
            None,
            None,
        )
        assert scales is None
        assert data_type == "Float64"

    def test_scaling_parameters_pl_rad_uint16(self):
        scales, data_type = multiproduct_scaling_parameters(
            {
                "product1": self.RGBTA_PROPERTIES_PL,
                "product2": self.RGBTA_PROPERTIES_PL,
            },
            ["blue", "alpha"],
            "toa_radiance",
            None,
            None,
        )
        assert scales is None
        assert data_type == "UInt16"

    def test_scaling_parameters_pl_rad_physical(self):
        scales, data_type = multiproduct_scaling_parameters(
            {
                "product1": self.RGBTA_PROPERTIES_PL,
                "product2": self.RGBTA_PROPERTIES_PL,
            },
            ["red", "green", "blue", "tirs1", "alpha"],
            "toa_radiance",
            "physical",
            None,
        )
        assert scales == [None, None, None, (0.0, 100.0, 0.0, 1.0), None]
        assert data_type == "Float64"

    def test_scaling_parameters_pl_bt(self):
        scales, data_type = multiproduct_scaling_parameters(
            {
                "product1": self.RGBTA_PROPERTIES_PL,
                "product2": self.RGBTA_PROPERTIES_PL,
            },
            ["tirs1"],
            "toa_brightness_temperature",
            None,
            None,
        )
        assert scales is None
        assert data_type == "Float64"

    def test_scaling_parameters_pl_bt_physical(self):
        scales, data_type = multiproduct_scaling_parameters(
            {
                "product1": self.RGBTA_PROPERTIES_PL,
                "product2": self.RGBTA_PROPERTIES_PL,
            },
            ["tirs1"],
            "toa_brightness_temperature",
            "physical",
            None,
        )
        assert scales == [(0.0, 333.0, 0.0, 1.0)]
        assert data_type == "Float64"
