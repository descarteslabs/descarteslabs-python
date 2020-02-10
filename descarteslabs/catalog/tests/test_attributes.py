import pytest
import unittest
import textwrap
from copy import deepcopy
from datetime import datetime
from enum import Enum

from ..catalog_base import CatalogObject
from ..attributes import (
    Attribute,
    Timestamp,
    EnumAttribute,
    utc,
    MappingAttribute,
    ListAttribute,
    DocumentState,
    Resolution,
    File,
    AttributeValidationError,
    ExtraPropertiesAttribute,
)
from ..band import BandType


class CountToThree(str, Enum):
    ONE = "One"
    TWO = "Two"
    THREE = "Three"


class Nested(MappingAttribute):
    foo = Attribute()
    dt = Timestamp(mutable=False)
    en = EnumAttribute(CountToThree)


class Mapping(MappingAttribute):
    bar = Attribute()
    nested = Nested()


class FakeCatalogObject(CatalogObject):
    mapping = Mapping()
    listmapping = ListAttribute(Mapping)
    listattribute = ListAttribute(Attribute)


class TestAttributes(unittest.TestCase):
    def test_immutabletimestamp(self):
        date = Timestamp(readonly=True)
        assert date.deserialize(None) is None

        assert (
            date.deserialize("2019-02-01T00:00:00.0000Z", validate=False).tzinfo == utc
        )
        assert date.deserialize(
            "2019-02-01T00:00:00.0000Z", validate=False
        ) == datetime(2019, 2, 1, tzinfo=utc)

        date = Timestamp(readonly=True)
        assert date.deserialize(
            datetime(2013, 12, 31, 23, 59, 59), validate=False
        ) == datetime(2013, 12, 31, 23, 59, 59, tzinfo=utc)
        value = date.deserialize(datetime(2013, 12, 31, 23, 59, 59), validate=False)
        assert date.serialize(value) == "2013-12-31T23:59:59+00:00"

    def test_mutable_timestamp(self):
        mutable_date = Timestamp()
        assert mutable_date.deserialize(None) is None
        assert (
            mutable_date.deserialize("2019-02-01T00:00:00.0000Z", validate=False).tzinfo
            == utc
        )

        class TimeObj(CatalogObject):
            date = Timestamp()

        # does not deserialize when unsaved
        obj = TimeObj(id="test-date", date="06/02/2019")
        assert obj.date == "06/02/2019"

        # does not deserialize when modified from unsaved
        obj.date = "Monday, June 2 2019"
        assert not isinstance(obj.date, datetime)

        assert obj.serialize()["date"] == "Monday, June 2 2019"

        # deserializes to datetime when validate=False
        obj._attribute_types["date"].__set__(
            obj, "2019-06-02T00:00:00.0000Z", validate=False
        )
        assert isinstance(obj.date, datetime)

        # does not deserialize when modified from saved
        obj.date = "Monday, June 2 2019"
        assert not isinstance(obj.date, datetime)

        obj.date = None
        assert obj.date is None

    def test_datetime_invalid(self):
        # This should not raise an exception
        Timestamp(readonly=True).deserialize("123439", validate=False)

    def test_enum_attribute(self):
        enum_attr = EnumAttribute(BandType)
        assert enum_attr.deserialize("spectral") == "spectral"
        assert enum_attr.serialize("spectral") == "spectral"
        assert BandType.SPECTRAL == "spectral"

    def test_enum_attribute_invalid(self):
        enum_attr = EnumAttribute(BandType)
        with pytest.raises(ValueError):
            enum_attr.deserialize("foobar")

    def test_enum_attribute_new(self):
        owner = FakeCatalogObject(id="id")
        enum_attr = EnumAttribute(BandType)
        owner._attribute_types["something"] = enum_attr
        enum_attr._attribute_name = "something"

        enum_attr.__set__(owner, "foobar", False)
        with pytest.raises(ValueError):
            enum_attr.__set__(owner, "foobar", True)

    def test_mapping_attributes(self):
        nested = Nested(foo="foo", dt="2019-02-01T00:00:00.0000Z", validate=False)
        mapping = Mapping(nested=nested)
        model_object = FakeCatalogObject(id="id", mapping=mapping)

        assert model_object.mapping.nested.foo == "foo"
        assert model_object.mapping.nested.dt == datetime(2019, 2, 1, tzinfo=utc)
        assert model_object.mapping is mapping
        assert model_object.mapping.nested is nested

        m_repr = repr(model_object.mapping)
        match_str = """\
            Mapping:
              nested: Nested:
                dt: 2019-02-01 00:00:00+00:00
                foo: foo"""
        assert m_repr.strip("\n") == textwrap.dedent(match_str)

        with pytest.raises(TypeError):
            Mapping("positionals not accepted")

    def test_mapping_change_tracking(self):
        nested = Nested(foo="foo", dt="2019-02-01T00:00:00.0000Z")
        mapping = Mapping(nested=nested)
        model_object = FakeCatalogObject(id="id", mapping=mapping, _saved=True)
        assert not model_object.is_modified

        # changes to mapping objects not accessed from the model_object
        # affect model state
        nested.foo = "blah"
        assert model_object.is_modified
        assert model_object.mapping.nested.foo == "blah"

        # assigning a new attribute value to the model does propagate state changes
        new_mapping = Mapping(
            nested=Nested(foo="bar", dt=datetime(2019, 3, 1, tzinfo=utc))
        )
        model_object.mapping = new_mapping
        assert model_object.is_modified
        assert model_object.mapping.nested.foo == "bar"
        assert model_object.mapping.nested.dt == datetime(2019, 3, 1, tzinfo=utc)
        assert model_object.mapping is new_mapping
        assert len(mapping._model_objects) == 0

    def test_mapping_references(self):
        nested = Nested(foo="foo", dt="2019-02-01T00:00:00.0000Z")
        mapping = Mapping(nested=nested, bar="bar")
        model_object = FakeCatalogObject(id="id", mapping=mapping, _saved=True)
        assert not model_object.is_modified

        # once a model mapping attribute is accessed, the reference is reused
        mapping1 = model_object.mapping
        mapping2 = model_object.mapping
        assert mapping1 is mapping2
        assert mapping1.nested is mapping2.nested

        # changes propagate to all references
        assert mapping1.bar == "bar"
        assert mapping2.bar == "bar"
        mapping1.bar = "baz"
        assert mapping1.bar == "baz"
        assert mapping2.bar == "baz"
        assert model_object.mapping.bar == "baz"

    def test_mapping_multiple_assignment(self):
        nested = Nested(foo="foo", dt="2019-02-01T00:00:00.0000Z")
        mapping = Mapping(nested=nested, bar="bar")
        model_object1 = FakeCatalogObject(id="id", mapping=mapping, _saved=True)
        model_object2 = FakeCatalogObject(id="id", mapping=mapping, _saved=True)
        assert not model_object1.is_modified
        assert not model_object2.is_modified

        # changing 1 reference propagates to all referencing objects
        model_object1.mapping.bar = "baz"
        assert model_object1.is_modified
        assert model_object2.is_modified

    def test_mapping_nested_change_tracking(self):
        nested = Nested(foo="foo", dt="2019-02-01T00:00:00.0000Z")
        mapping = Mapping(nested=nested)
        model_object = FakeCatalogObject(id="id", mapping=mapping, _saved=True)
        assert not model_object.is_modified

        # state changes at any level propagate the change back to the model
        model_object.mapping.nested.foo = "baz"
        assert model_object.mapping.nested.foo == "baz"
        assert model_object.is_modified
        assert "mapping" in model_object._modified

    def test_mapping_serialization(self):
        nested = Nested(foo="foo", dt="2019-02-01T00:00:00.0000Z", validate=False)
        mapping = Mapping(nested=nested)
        model_object = FakeCatalogObject(id="id", mapping=mapping)

        serialized = model_object.serialize(modified_only=True)
        assert serialized == {
            "mapping": {"nested": {"foo": "foo", "dt": "2019-02-01T00:00:00+00:00"}}
        }
        assert model_object._attributes is not serialized

    def test_mapping_deserialization(self):
        # Creation with valid enum should be fine
        model_object = FakeCatalogObject(id="id", mapping={"nested": {"en": "One"}})
        mapping = model_object.mapping
        nested = mapping.nested
        assert nested.en == "One"

        # Creation with invalid enum with values from server should be fine
        model_object = FakeCatalogObject(
            id="id", mapping={"nested": {"en": "Four"}}, _saved=True
        )
        mapping = model_object.mapping
        nested = mapping.nested
        assert nested.en == "Four"

        # Creation with invalid enum causes exception
        with pytest.raises(ValueError):
            FakeCatalogObject(id="id", mapping={"nested": {"en": "Four"}})

        # Creation with undefined attribute from server should be fine
        model_object = FakeCatalogObject(id="id", mapping={"baz": "qux"}, _saved=True)
        mapping = model_object.mapping
        assert "baz" not in mapping._attributes

        # Creation with undefined attribute causes exception
        with pytest.raises(AttributeError):
            FakeCatalogObject(id="id", mapping={"baz": "qux"})

    def test_mapping_equality(self):
        assert Mapping() != Nested()
        assert Mapping() == Mapping()
        assert Mapping(bar="bar") == Mapping(bar="bar")
        assert Mapping(bar="bar") != Mapping()
        assert Mapping() != Mapping(bar="bar")
        assert Mapping(bar="bar1") != Mapping(bar="bar2")
        assert Mapping(bar="bar", nested=Nested(foo="foo")) == Mapping(
            bar="bar", nested=Nested(foo="foo")
        )
        assert Mapping(bar="bar", nested=Nested(foo="foo1")) != Mapping(
            bar="bar", nested=Nested(foo="foo2")
        )

    def test_mapping_hash(self):
        with pytest.raises(TypeError):
            hash(Mapping())

    def test_list_attributes(self):

        nested1 = Nested(foo="zap", dt="2019-02-01T00:00:00.0000Z", validate=False)
        nested2 = Nested(foo="zip", dt="2019-02-02T00:00:00.0000Z", validate=False)

        mapping1 = Mapping(nested=nested1)
        mapping2 = Mapping(nested=nested2)
        model_object = FakeCatalogObject(
            id="id", listmapping=[mapping1, mapping2], listattribute=[12]
        )

        assert model_object.listmapping[0].nested.foo == "zap"
        assert model_object.listmapping[1].nested.foo == "zip"
        assert model_object.listmapping[0].nested.dt == datetime(2019, 2, 1, tzinfo=utc)
        assert model_object.listmapping[1].nested.dt == datetime(2019, 2, 2, tzinfo=utc)
        assert model_object.listmapping is not [mapping1, mapping2]
        assert model_object.listmapping[0] is mapping1
        assert model_object.listattribute[0] == 12

        m_repr = repr(model_object.listmapping)
        match_str = """\
            [Mapping:
              nested: Nested:
                dt: 2019-02-01 00:00:00+00:00
                foo: zap, Mapping:
              nested: Nested:
                dt: 2019-02-02 00:00:00+00:00
                foo: zip]"""

        assert m_repr.strip("\n") == textwrap.dedent(match_str)

    def test_listattribute_change_tracking(self):
        nested1 = Nested(foo="zap", dt="2019-02-01T00:00:00.0000Z", validate=False)
        nested2 = Nested(foo="zip", dt="2019-02-02T00:00:00.0000Z", validate=False)
        mapping1 = Mapping(nested=nested1)
        mapping2 = Mapping(nested=nested2)
        model_object = FakeCatalogObject(
            id="id", listmapping=[mapping1, mapping2], _saved=True
        )
        assert not model_object.is_modified

        # references to already instantiated objects are carried forward
        assert model_object.listmapping[0] is mapping1
        assert model_object.listmapping[1] is mapping2

        # changes to references not accessed from attribute still propagate changes
        nested1.foo = "zop"
        assert model_object.is_modified
        assert model_object.listmapping[0].nested.foo == "zop"

        # assigning a new attribute value to the model does propagate state changes
        new_mapping = Mapping(
            nested=Nested(foo="meep", dt=datetime(2019, 3, 1, tzinfo=utc))
        )
        model_object.listmapping = [new_mapping]
        assert model_object.is_modified
        assert model_object.listmapping[0].nested.foo == "meep"
        assert model_object.listmapping[0].nested.dt == datetime(2019, 3, 1, tzinfo=utc)

    def test_listattribute_deserialization(self):
        # Creation with valid enum should be fine
        model_object = FakeCatalogObject(
            id="id",
            listmapping=[{"nested": {"en": "One"}}, {"nested": {"en": "Three"}}],
        )
        listmapping = model_object.listmapping
        nested1 = listmapping[0].nested
        assert nested1.en == "One"
        nested2 = listmapping[1].nested
        assert nested2.en == "Three"

        # Creation with invalid enum with values from server should be fine
        model_object = FakeCatalogObject(
            id="id", listmapping=[{"nested": {"en": "Four"}}], _saved=True
        )
        listmapping = model_object.listmapping
        nested = listmapping[0].nested
        assert nested.en == "Four"

        # Creation with invalid enum causes exception
        with pytest.raises(ValueError):
            FakeCatalogObject(id="id", listmapping=[{"nested": {"en": "Four"}}])

    def test_listattribute_container_methods(self):
        nested1 = Nested(foo="zap", dt="2019-02-01T00:00:00.0000Z", validate=False)
        nested2 = Nested(foo="zip", dt="2019-02-02T00:00:00.0000Z", validate=False)
        mapping1 = Mapping(nested=nested1)
        mapping2 = Mapping(nested=nested2)
        model_object1 = FakeCatalogObject(
            id="id1",
            listmapping=[mapping1, mapping2],
            listattribute=["hi"],
            _saved=True,
        )
        model_object2 = FakeCatalogObject(
            id="id2",
            listmapping=[mapping1, mapping2],
            listattribute=["hi"],
            _saved=True,
        )

        assert model_object1.listmapping == model_object2.listmapping
        assert model_object1.listattribute == model_object2.listattribute

        model_object1.listmapping.append(Mapping(nested=nested1))
        model_object1.listattribute.append("hello")

        assert model_object1.listmapping != model_object2.listmapping
        assert model_object1.listattribute != model_object2.listattribute

        assert len(model_object1.listmapping) == 3
        assert len(model_object1.listattribute) == 2

        sliced_list = model_object1.listmapping[1:]
        assert len(sliced_list) == 2
        assert sliced_list[0] == mapping2

        # since model_object1 and model_object2 have different ListAttribute
        # instances, one should me modified, and the other not
        # they should still retain references to contained MappingAttributes though!
        assert model_object1.state == DocumentState.MODIFIED
        assert model_object2.state == DocumentState.SAVED
        assert model_object1.listmapping[0] is model_object2.listmapping[0]

        popped_attr = model_object1.listattribute.pop()
        popped_mapping = model_object1.listmapping.pop()
        assert popped_attr == "hello"
        assert popped_mapping == mapping1
        assert len(model_object1.listmapping) == 2
        assert len(model_object1.listattribute) == 1

    def test_listattribute_delegate_methods(self):
        nested1 = Nested(foo="zap", dt="2019-02-01T00:00:00.0000Z", validate=False)
        nested2 = Nested(foo="zip", dt="2019-02-02T00:00:00.0000Z", validate=False)
        mapping1 = Mapping(nested=nested1)
        mapping2 = Mapping(nested=nested2)
        model_object = FakeCatalogObject(
            id="id1",
            listmapping=[mapping1, mapping2],
            listattribute=["hi", "bye"],
            _saved=True,
        )

        # magigmethods
        la = model_object.listattribute
        map_la = model_object.listmapping
        assert la + [2] == ["hi", "bye", 2]
        assert "hi" in la
        assert la * 2 == ["hi", "bye", "hi", "bye"]
        assert list(iter(la)) == ["hi", "bye"]
        assert list(reversed(la)) == ["bye", "hi"]

        assert map_la + [dict(bar="baz")] == [mapping1, mapping2, Mapping(bar="baz")]
        assert mapping1 in map_la
        assert map_la * 2 == [mapping1, mapping2, mapping1, mapping2]
        assert list(iter(map_la)) == [mapping1, mapping2]
        assert list(reversed(map_la)) == [mapping2, mapping1]

        # comparison magicmethods
        assert la >= ["a"]
        assert la >= ["hi"]
        assert la > ["a"]
        assert la == ["hi", "bye"]
        assert la != ["hi!"]
        assert la <= ["hi!"]
        assert la <= ["hi", "bye"]
        assert la < ["hi!"]

        assert map_la >= [mapping1]
        assert map_la >= [mapping1, mapping2]
        assert map_la > [mapping1]
        assert map_la == [mapping1, mapping2]
        assert map_la != [mapping1]
        assert map_la <= [mapping1, mapping2, mapping2]
        assert map_la <= [mapping1, mapping2]
        assert map_la < [mapping1, mapping2, mapping2]

        with pytest.raises(TypeError):
            la >= 1

        # other methods
        assert la.count("bye") == 1
        assert la.index("bye") == 1

        assert map_la.count(mapping2) == 1
        assert map_la.index(mapping2) == 1

        # copy is only in py3
        copy = la.copy()
        assert copy is not la
        assert copy == la

        map_copy = map_la.copy()
        assert map_copy is not map_la
        assert map_copy == map_la

        # none of these should have changed the list
        assert la == ["hi", "bye"]
        assert map_la == [mapping1, mapping2]
        assert not model_object.is_modified

        # methods that create copies are shallow and don't detach the ListAttribute from
        # contained MappingAttributes, so modifications to those contained objects
        # still propagate changes
        new_map_la = map_la + [dict(bar="baz")]
        assert len(new_map_la) == 3
        assert not model_object.is_modified
        new_map_la[-1].bar = "qux"
        assert not model_object.is_modified
        new_map_la[0].bar = "quux"
        assert model_object.is_modified

    def test_list_attribute_delegate_mutable_methods_simple_items(self):
        model_object = FakeCatalogObject(
            id="id1", listattribute=["hi", "bye"], _saved=True
        )

        # magigmethods
        la = model_object.listattribute
        la += [2]
        assert la == ["hi", "bye", 2]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.append(2)
        assert la == ["hi", "bye", 2, 2]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la *= 2
        assert la == ["hi", "bye", 2, 2, "hi", "bye", 2, 2]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        del la[:]
        assert la == []
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.append(0)
        la.clear()
        assert la == []
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.extend([1, 2])
        assert la == [1, 2]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.insert(1, 3)
        assert la == [1, 3, 2]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.pop()
        assert la == [1, 3]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.remove(3)
        assert la == [1]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.extend([2, 3])
        la.reverse()
        assert la == [3, 2, 1]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.sort()
        assert la == [1, 2, 3]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        # assignment
        la[1] = "foo"
        assert la == [1, "foo", 3]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        # slice assignment is particularly crazy
        la[1:3] = "abc"
        assert la == [1, "a", "b", "c"]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

    def test_list_attribute_modification(self):
        model_object = FakeCatalogObject(
            id="id1", listmapping=[], listattribute=[5, 5, 5, 5], _saved=True
        )
        la = model_object.listmapping
        la.sort()
        assert not model_object.is_modified
        la.reverse()
        assert not model_object.is_modified
        del la[:]
        assert not model_object.is_modified
        la += []
        assert not model_object.is_modified
        la *= 5
        assert not model_object.is_modified

        la += [{"bar": 5}]
        assert model_object.is_modified

        model_object._clear_modified_attributes()
        la.sort()
        assert not model_object.is_modified
        la.reverse()
        assert not model_object.is_modified
        la *= 1
        assert not model_object.is_modified
        del la[:]

        laa = model_object.listattribute
        model_object._clear_modified_attributes()
        laa.sort()
        assert not model_object.is_modified
        laa.reverse()
        assert not model_object.is_modified

        la = [Mapping(bar="foo"), Mapping(bar=1), Mapping(bar=2), Mapping(bar=3)]
        model_object._clear_modified_attributes()
        la[1:3] = [dict(bar=1), dict(bar=2)]
        assert not model_object.is_modified
        la[-1].bar = 3
        assert not model_object.is_modified

    def test_list_attribute_delegate_mutable_methods_mapping_items(self):
        nested1 = Nested(foo="zap", dt="2019-02-01T00:00:00.0000Z", validate=False)
        nested2 = Nested(foo="zip", dt="2019-02-02T00:00:00.0000Z", validate=False)
        mapping1 = Mapping(nested=nested1)
        mapping2 = Mapping(nested=nested2)
        model_object = FakeCatalogObject(
            id="id1", listmapping=[mapping1, mapping2], _saved=True
        )

        la = model_object.listmapping
        la.append(dict(bar="foo"))
        assert la == [mapping1, mapping2, Mapping(bar="foo")]
        assert model_object.is_modified
        model_object._clear_modified_attributes()
        la[-1].bar = "baz"
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la *= 2
        assert la == [
            mapping1,
            mapping2,
            Mapping(bar="baz"),
            mapping1,
            mapping2,
            Mapping(bar="baz"),
        ]
        assert model_object.is_modified
        model_object._clear_modified_attributes()
        la[-1].bar = "zab"  # new different item is attached
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        del la[:]
        assert la == []
        assert model_object.is_modified
        model_object._clear_modified_attributes()
        mapping1.bar = "baz"
        assert not model_object.is_modified

        la.append(dict(bar="foo"))
        new_mapping = la[0]
        la.clear()
        assert la == []
        assert model_object.is_modified
        model_object._clear_modified_attributes()
        new_mapping.bar = "baz"
        assert not model_object.is_modified

        la.extend([dict(bar="foo"), dict(bar="baz")])
        assert la == [Mapping(bar="foo"), Mapping(bar="baz")]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        la.insert(1, dict(bar="qux"))
        assert la == [Mapping(bar="foo"), Mapping(bar="qux"), Mapping(bar="baz")]
        assert model_object.is_modified
        model_object._clear_modified_attributes()

        popped = la.pop()
        assert la == [Mapping(bar="foo"), Mapping(bar="qux")]
        assert model_object.is_modified
        model_object._clear_modified_attributes()
        popped.bar = "quux"
        assert not model_object.is_modified
        model_object._clear_modified_attributes()

        removed = la[1]
        la.remove(Mapping(bar="qux"))
        assert la == [Mapping(bar="foo")]
        assert model_object.is_modified
        model_object._clear_modified_attributes()
        removed.bar = "quux"
        assert not model_object.is_modified

        # slice assignment is crazy
        la[1:3] = [dict(bar=1), dict(bar=2), dict(bar=3)]
        assert la == [
            Mapping(bar="foo"),
            Mapping(bar=1),
            Mapping(bar=2),
            Mapping(bar=3),
        ]
        assert model_object.is_modified
        model_object._clear_modified_attributes()
        la[-1].bar = 4
        assert model_object.is_modified

    def test_listattribute_extra_properties_attribute(self):
        la = ListAttribute(ExtraPropertiesAttribute(mutable=False))
        la.append({"one": "two"})
        la.append({"three": "four"})
        assert la[0] == ExtraPropertiesAttribute({"one": "two"})
        assert la[1] == ExtraPropertiesAttribute({"three": "four"})

        with self.assertRaises(AttributeValidationError):
            la[1]["three"] = "six"

        class ExtraPropertiesListCatalogObject(CatalogObject):
            epl = ListAttribute(ExtraPropertiesAttribute)

        eplco = ExtraPropertiesListCatalogObject(
            epl=[{"one": "two"}, {"three": "four"}], _saved=True
        )
        assert not eplco.is_modified
        eplco.epl.append({"five": "six", "seven": "eight"})
        assert eplco.is_modified

    def test_deepcopy(self):
        r = Resolution(value=2, unit="meters")
        r_copy = deepcopy(r)
        assert r is not r_copy
        assert r == r_copy

        r.value = 0
        r_copy.value = 20
        assert r.value == 0
        assert r_copy.value == 20

        f = File(href="foo")
        f_copy = deepcopy(f)
        assert f is not f_copy
        assert f == f_copy

        f_copy.href = "bar"
        assert f.href == "foo"
        assert f_copy.href == "bar"

        la = ListAttribute(File)
        la.append(f)
        la_copy = deepcopy(la)
        assert la is not la_copy
        assert la == la_copy

        la_copy.append(f_copy)
        assert la == [f]
        assert la_copy == [f, f_copy]

    def test_create_non_attr(self):
        with pytest.raises(AttributeError):
            Resolution(value=2, bar="foo")

    def test_set_non_attr(self):
        r = Resolution()

        with pytest.raises(AttributeError):
            r.foo = "bad"

    def test_del(self):
        class Obj(CatalogObject):
            attr = Attribute()
            mapping = Resolution()
            list = ListAttribute(Attribute)

        o = Obj(attr="something", mapping={"value": 5}, list=["one"])
        assert len(o._attributes) == 3
        del o.attr
        del o.mapping
        del o.list
        assert len(o._attributes) == 0

    def test_set_immutable(self):
        class Obj(CatalogObject):
            attr = Attribute(mutable=False)
            mapping = Resolution(mutable=False)
            list = ListAttribute(Attribute, mutable=False)

        o = Obj()
        o.attr = "something"
        o.mapping = Resolution(value=5)
        o.list = ["something"]

        # Cannot reassign
        with self.assertRaises(AttributeValidationError):
            o.attr = "something else"
        with self.assertRaises(AttributeValidationError):
            o.mapping = Resolution()
        with self.assertRaises(AttributeValidationError):
            o.list = []
        with self.assertRaises(AttributeValidationError):
            o.mapping = None
        with self.assertRaises(AttributeValidationError):
            o.list = None

        # Cannot change value of immutable mapping
        with self.assertRaises(AttributeValidationError):
            o.mapping.unit = "meters"
        with self.assertRaises(AttributeValidationError):
            o.mapping.value = 6
        with self.assertRaises(AttributeValidationError):
            o.list[0] = "something else"
        with self.assertRaises(AttributeValidationError):
            o.list.append("something else")

    def test_set_mutable_and_immutable(self):
        class Obj(CatalogObject):
            mapping = Resolution()
            list = ListAttribute(Attribute)

        class ImmutableObj(CatalogObject):
            mapping = Resolution(mutable=False)
            list = ListAttribute(Attribute, mutable=False)

        r = Resolution(value=5)
        la = ListAttribute(Attribute, items=["one", "two"])

        mutable = Obj(mapping=r, list=la)
        mutable.mapping.value = 6
        mutable.list.append("three")

        mutable2 = Obj(mapping=r, list=la)
        mutable2.mapping.value = 7
        mutable2.list.append("four")

        assert mutable.mapping.value == 7

        mutable2.mapping = Resolution()
        mutable2.mapping.value = 8
        mutable2.list = ["six"]

        # Once assigned to an immutable object, the shared attributes are immutable
        immutable = ImmutableObj(mapping=r, list=la)

        with self.assertRaises(AttributeValidationError):
            mutable.mapping.value = 9
        with self.assertRaises(AttributeValidationError):
            mutable.list.append("seven")

        # Can't delete immutable attributes
        with self.assertRaises(AttributeValidationError):
            del immutable.mapping
        with self.assertRaises(AttributeValidationError):
            del immutable.list

        assert r == Resolution(value=7)
        assert la == ["one", "two", "three", "four"]

    def test_set_readonly(self):
        class Obj(CatalogObject):
            readonly_attr = Attribute(readonly=True)
            readonly_mapping = Resolution(readonly=True)
            readonly_list = ListAttribute(Attribute, readonly=True)

        # First check simple assignment
        o = Obj()
        with self.assertRaises(AttributeValidationError):
            o.readonly_attr = "something"
        with self.assertRaises(AttributeValidationError):
            o.readonly_mapping = Resolution()
        with self.assertRaises(AttributeValidationError):
            o.readonly_list = []
        assert not o.is_modified

        # Next check re-assignment
        o = Obj(
            readonly_attr="something",
            readonly_mapping=Resolution(value=5, unit="meters"),
            readonly_list=[],
            _saved=True,
        )
        with self.assertRaises(AttributeValidationError):
            o.readonly_attr = "something else"
        with self.assertRaises(AttributeValidationError):
            o.readonly_mapping = Resolution()
        with self.assertRaises(AttributeValidationError):
            o.readonly_mapping.value = 6
        with self.assertRaises(AttributeValidationError):
            o.readonly_list = ["something"]
        with self.assertRaises(AttributeValidationError):
            o.readonly_list.append("something")
        assert not o.is_modified

    def test_set_writable_and_readonly(self):
        class Obj(CatalogObject):
            mapping = Resolution()
            list = ListAttribute(Attribute)

        class ReadonlyObj(CatalogObject):
            mapping = Resolution(readonly=True)
            list = ListAttribute(Attribute, readonly=True)

        r = Resolution(value=5)
        la = ListAttribute(Attribute, items=["one", "two"])

        obj = Obj(mapping=r, list=la)
        obj.mapping.value = 6
        obj.list.append("three")

        # Once assigned to an immutable object, the shared attributes are immutable
        readonly = ReadonlyObj(mapping=r, list=la, _saved=True)

        with self.assertRaises(AttributeValidationError):
            readonly.mapping.value = 7
        with self.assertRaises(AttributeValidationError):
            readonly.list.append("four")

        # Can't delete readonly attributes
        with self.assertRaises(AttributeValidationError):
            del readonly.mapping
        with self.assertRaises(AttributeValidationError):
            del readonly.list

        assert r == Resolution(value=6)
        assert la == ["one", "two", "three"]

    def test_resolution_string(self):
        Resolution("60m")
        Resolution("-6.5 deg.")
        with self.assertRaises(AttributeValidationError):
            Resolution("60")

        class Foo(MappingAttribute):
            r = Resolution()

        Foo.r = "60  m."
        Foo.r = "1.234 °"
        with self.assertRaises(AttributeValidationError):
            Resolution("m")

    def test_create_and_assign_property_attribute(self):
        d = {"one": "two", "three": "four"}
        p = ExtraPropertiesAttribute(d)
        assert p.serialize(p) == d
        assert p.serialize(p) is not d

        class Foo(CatalogObject):
            properties = ExtraPropertiesAttribute()

        f = Foo(properties=d)
        f = Foo(properties=ExtraPropertiesAttribute(d))
        f.properties = d
        f.properties = ExtraPropertiesAttribute(d)

    def test_model_for_property_attribute(self):
        class Foo(CatalogObject):
            properties = ExtraPropertiesAttribute()

        d = {"one": "two", "three": "four"}
        f = Foo(properties=d)
        assert f.is_modified

        p = f.properties
        f.properties = d
        f._clear_modified_attributes()
        assert not f.is_modified

        p["five"] = "six"
        assert not f.is_modified

        f.properties["seven"] = "eight"
        assert f.is_modified
        f._clear_modified_attributes()

        del f.properties["seven"]
        assert f.is_modified
        assert f.properties == d
        assert f.properties != p

        del p["five"]
        assert f.properties == p
        assert f.properties is not p

    def test_create_bad_property_attribute(self):
        with self.assertRaises(AttributeValidationError):
            ExtraPropertiesAttribute({15: "something"})
        with self.assertRaises(AttributeValidationError):
            ExtraPropertiesAttribute({"something": object()})
        with self.assertRaises(AttributeValidationError):
            ExtraPropertiesAttribute({"something": {"something_else": 15}})

        class Foo(CatalogObject):
            properties = ExtraPropertiesAttribute()

        with self.assertRaises(AttributeValidationError):
            Foo(properties=True)
        with self.assertRaises(AttributeValidationError):
            f = Foo(properties={})
            f.properties = object()

    def test_readonly_property_attribute(self):
        class Foo(CatalogObject):
            properties = ExtraPropertiesAttribute(readonly=True)

        d = {"one": "two", "three": "four"}
        f = Foo(properties=d, _saved=True)
        assert not f.is_modified

        with self.assertRaises(AttributeValidationError):
            f.properties = d
        with self.assertRaises(AttributeValidationError):
            f.properties["six"] = "seven"
        with self.assertRaises(AttributeValidationError):
            del f.properties["one"]
