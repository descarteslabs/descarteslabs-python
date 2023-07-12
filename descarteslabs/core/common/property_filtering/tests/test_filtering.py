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

from datetime import datetime

import pytest

from ....catalog import MaskBand, Product
from .. import GenericProperties, Properties, Property


def v1_expression(expression, expected_expression):
    assert expression.serialize() == expected_expression


def v2_expression(expression, expected_expression):
    assert expression.jsonapi_serialize() == expected_expression
    assert expression.jsonapi_serialize(Product) == expected_expression


def test_generic_properties():
    properties = Properties()
    assert isinstance(properties.foo, Property)
    assert isinstance(properties.bar, Property)

    properties = GenericProperties()
    assert isinstance(properties.foo, Property)
    assert isinstance(properties.bar, Property)


def test_specific_properties():
    properties = Properties("foo")
    assert isinstance(properties.foo, Property)

    with pytest.raises(AttributeError):
        assert isinstance(properties.bar, Property)


def test_isnull_expression():
    name = "modified"

    property = Property(name)
    expression = property.isnull

    assert expression.name == name

    with pytest.raises(TypeError):
        expression.serialize()

    v2_expression(expression, dict(name=name, op="isnull"))


def test_isnull_reference():
    name = "product"

    property = Property(name)
    expression = property.isnull

    expected_expression = dict(name=f"{name}_id", op="isnull")
    assert expression.jsonapi_serialize(MaskBand) == expected_expression


def test_isnull_expression_no_attribute():
    name = "foo"

    property = Property(name)
    expression = property.isnull

    with pytest.raises(AttributeError):
        expression.jsonapi_serialize(Product)


def test_isnotnull_expression():
    name = "modified"

    property = Property(name)
    expression = property.isnotnull

    assert expression.name == name

    with pytest.raises(TypeError):
        expression.serialize()

    v2_expression(expression, dict(name=name, op="isnotnull"))


def test_isnotnull_reference():
    name = "product"

    property = Property(name)
    expression = property.isnotnull

    expected_expression = dict(name=f"{name}_id", op="isnotnull")
    assert expression.jsonapi_serialize(MaskBand) == expected_expression


def test_isnotnull_expression_no_attribute():
    name = "foo"

    property = Property(name)
    expression = property.isnotnull

    with pytest.raises(AttributeError):
        expression.jsonapi_serialize(Product)


def test_like_expression():
    name = "description"
    value = "%scr%"

    property = Property(name)
    expression = property.like(value)

    assert expression.name == name

    v1_expression(expression, dict(like=dict(description=value)))

    expected = dict(name=name, op="like", val=value)
    expression.jsonapi_serialize() == expected


def test_any_of_expression():
    name = "tags"
    value = ["one", "two"]

    property = Property(name)
    expression = property.any_of(value)

    assert expression.parts[0].name == name
    assert expression.parts[1].name == name
    assert Property.any_of == Property.in_

    v1_expression(
        expression, {"or": [dict(eq={name: value[0]}), dict(eq={name: value[1]})]}
    )
    v2_expression(
        expression,
        {
            "or": [
                dict(name=name, op="eq", val=value[0]),
                dict(name=name, op="eq", val=value[1]),
            ]
        },
    )


def test_any_of_expression_no_attribute():
    name = "foo"
    value = ["one", "two"]

    property = Property(name)
    expression = property.any_of(value)

    assert expression.serialize()
    assert expression.jsonapi_serialize()

    with pytest.raises(AttributeError):
        expression.jsonapi_serialize(Product)


def test_eq_ne_expression():
    name = "tags"
    value = "one"

    for op in ("eq", "ne"):
        property = Property(name)

        if op == "eq":
            expression = property == value
        else:
            expression = property != value

        assert expression.name == name

        v1_expression(expression, {op: {name: value}})
        v2_expression(expression, dict(name=name, op=op, val=value))


def test_reference_attribute():
    op = "eq"
    name = "product"
    value = "one:two"

    property = Property(name)
    expression = property == MaskBand(id=value)

    assert expression.name == f"{name}_id"

    expected_expression = dict(name=f"{name}_id", op=op, val=value)
    assert expression.jsonapi_serialize(MaskBand) == expected_expression


def test_reference_id():
    op = "eq"
    name = "product"
    value = "one"

    property = Property(name)
    expression = property == value

    assert expression.name == name

    expected_expression = dict(name=f"{name}_id", op=op, val=value)
    assert expression.jsonapi_serialize(MaskBand) == expected_expression


def test_reversed_eq_ne_expression():
    name = "tags"
    value = "one"

    for op in ("eq", "ne"):
        property = Property(name)

        if op == "eq":
            expression = value == property
        else:
            expression = value != property

        assert expression.name == name

        v1_expression(expression, {op: {name: value}})
        v2_expression(expression, dict(name=name, op=op, val=value))


def test_eq_ne_expression_no_attribute():
    name = "foo"
    value = "one"

    for op in ("eq", "ne"):
        property = Property(name)

        if op == "eq":
            expression = property == value
        else:
            expression = property != value

        assert expression.serialize()
        assert expression.jsonapi_serialize()

        with pytest.raises(AttributeError):
            expression.jsonapi_serialize(Product)


def test_range_expression_single():
    name = "created"
    value = "2022-09-22"

    for op in ("lt", "gt", "lte", "gte"):
        property = Property(name)

        if op == "lt":
            expression = property < value
        elif op == "gt":
            expression = property > value
        elif op == "lte":
            expression = property <= value
        else:
            expression = property >= value

        assert expression.name == name

        v1_expression(expression, dict(range={name: {op: value}}))
        v2_expression(expression, dict(name=name, op=op, val=value))


def test_range_expression():
    name = "created"
    value1 = "2022-08-22"
    value2 = "2022-09-22"

    # Operation and opposite operation
    for op, oop in (("lt", "gt"), ("gt", "lt"), ("lte", "gte"), ("gte", "lte")):
        property = Property(name)

        if op == "lt":
            expression = value2 < property < value1
        elif op == "gt":
            expression = value2 > property > value1
        elif op == "lte":
            expression = value2 <= property <= value1
        else:
            expression = value2 >= property >= value1

        assert expression.name == name

        v1_expression(expression, dict(range={name: {op: value1, oop: value2}}))
        v2_expression(
            expression,
            {
                "and": [
                    dict(name=name, op=oop, val=value2),
                    dict(name=name, op=op, val=value1),
                ]
            },
        )


def test_range_expression_datetime():
    name = "created"
    value = datetime.now()

    for op in ("lt", "gt", "lte", "gte"):
        property = Property(name)

        if op == "lt":
            expression = property < value
        elif op == "gt":
            expression = property > value
        elif op == "lte":
            expression = property <= value
        else:
            expression = property >= value

        expected_expression = dict(name=name, op=op, val=value.isoformat())
        assert expression.jsonapi_serialize(Product) == expected_expression


def test_range_expression_no_attribute():
    name = "foo"
    value = "2022-09-22"

    for op in ("lt", "gt", "lte", "gte"):
        property = Property(name)

        if op == "lt":
            expression = property < value
        elif op == "gt":
            expression = property > value
        elif op == "lte":
            expression = property <= value
        else:
            expression = property >= value

        assert expression.serialize()
        assert expression.jsonapi_serialize()

        with pytest.raises(AttributeError):
            expression.jsonapi_serialize(Product)
