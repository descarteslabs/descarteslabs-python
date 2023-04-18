# Copyright 2018-2023 Descartes Labs.
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
from datetime import datetime

from ...common.property_filtering import Properties

from ..band import Band
from ..image import Image
from ..product import Product
from ..attributes import Resolution

#
# Note that filter values are not validated, so supplying invalid values
# will send that value to the backend, which *will* validate the values.
#
# These tests make sure that there are no other problems with filters.
#

prop = Properties()


class TestBoolFilter(unittest.TestCase):
    def test_valid_bool_filter(self):
        expr = prop.is_core == True  # noqa: E712
        result = expr.jsonapi_serialize(Product)
        assert result["val"] is True

        expr = prop.is_core == False  # noqa: E712
        result = expr.jsonapi_serialize(Product)
        assert result["val"] is False

    def test_invalid_bool_filter(self):
        # No exception
        expr = prop.is_core == 1.25
        result = expr.jsonapi_serialize(Product)
        assert result["val"] is True


class TestIntFilter(unittest.TestCase):
    def test_valid_int_filter(self):
        expr = prop.band_index == 1
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == 1

    def test_invalid_int_filter(self):
        # No exception
        expr = prop.band_index == 1.25
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == 1.25


class TestFloatFilter(unittest.TestCase):
    def test_valid_float_filter(self):
        expr = prop.revisit_period_minutes_min == 1.25
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == 1.25

    def test_valid_int_filter(self):
        expr = prop.revisit_period_minutes_min == 1
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == 1.0

    def test_valid_bool_filter(self):
        # float(False) == 0.0; float(True) == 1.0
        expr = prop.revisit_period_minutes_min == False  # noqa: E712
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == 0.0

        expr = prop.revisit_period_minutes_min == True  # noqa: E712
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == 1.0

    def test_invalid_float_filter(self):
        # No exception
        expr = prop.revisit_period_minutes_min == []
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == []


class TestCatalogObjectReferenceFilter(unittest.TestCase):
    def test_valid_catalog_object_reference_filter(self):
        expr = prop.product == Product(id="something")
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "something"

    def test_invalid_catalog_object_reference_filter(self):
        # Sadly, any `id` will do...
        expr = prop.product == Image(id="something:something")
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "something:something"

    def test_valid_id_filter(self):
        expr = prop.product == "something"
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "something"

    def test_invalid_id_filter(self):
        # No exception
        expr = prop.product == 12
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == 12


class TestTimestampFilter(unittest.TestCase):
    def test_valid_timestamp_filter(self):
        expr = prop.created == datetime.fromisoformat("2022-10-10")
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "2022-10-10T00:00:00"

        expr = prop.created == "2022-10-10"
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "2022-10-10"

    def test_invalid_timestamp_filter(self):
        # No exception
        expr = prop.created == "something"
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "something"


class TestEnumFilter(unittest.TestCase):
    def test_valid_enum_filter(self):
        expr = prop.data_type == "Byte"
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "Byte"

    def test_invalid_enum_filter(self):
        # No exception
        expr = prop.data_type == "BYTE"
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "BYTE"


class TestTupleAttributeFilter(unittest.TestCase):
    def test_valid_tuple_attribute_filter(self):
        expr = prop.data_range == (1, 2)
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == (1, 2)

    def test_invalid_tuple_attribute_filter(self):
        # No exception
        expr = prop.data_range == "key"
        result = expr.jsonapi_serialize(Band)
        assert result["val"] == "key"


class TestResolutionFilter(unittest.TestCase):
    def test_valid_resolution_filter(self):
        expr = prop.resolution_min == Resolution(value=60, unit="meters")
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == {"value": 60, "unit": "meters"}

        expr = prop.resolution_min == {"value": 60, "unit": "meters"}
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == {"value": 60, "unit": "meters"}

    def test_invalid_resolution_filter(self):
        # No exception
        expr = prop.resolution_min == "key"
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == "key"


class TestListAttributeFilter(unittest.TestCase):
    # A list goes down to individual elements

    def test_valid_list_attribute_filter(self):
        expr = prop.default_display_bands == "one"
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == "one"

    def test_invalid_resolution_filter(self):
        # No exception
        expr = prop.default_display_bands == 12
        result = expr.jsonapi_serialize(Product)
        assert result["val"] == 12
