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
from datetime import datetime, timezone

import pytz

from .. import Attribute, DatetimeAttribute, Document, DocumentState, ListAttribute


class MyDocument(Document):
    id: int = Attribute(int, readonly=True)
    name: str = Attribute(str)
    local: str = Attribute(str, default="local", sticky=True)
    once: int = Attribute(int, mutable=False)
    default: datetime = DatetimeAttribute(default=lambda: datetime.utcnow())
    created_at: datetime = DatetimeAttribute(readonly=True)


class TestDocument(unittest.TestCase):
    def test_attribute(self):
        doc = MyDocument(name="testing")
        assert doc.name == "testing"
        assert doc.state == DocumentState.NEW

    def test_default(self):
        doc = MyDocument()
        assert doc.id is None
        assert doc.name is None
        assert doc.local == "local"
        assert doc.once is None

        date = doc.default
        assert date is not None
        assert doc.default == date
        assert doc.created_at is None

    def test_modified(self):
        doc = MyDocument(name="test")
        doc.name = "something new"
        assert doc.name == "something new"
        assert doc.is_modified
        assert doc._modified == {"name"}

        doc.name = None
        assert doc.is_modified
        assert doc._modified == {"name"}
        assert doc.name is None

    def test_coerce(self):
        doc = MyDocument(once="1")
        assert doc.once == 1

        with self.assertRaises(ValueError) as ctx:
            doc = MyDocument(once="1blguoaw")
        assert "Unable to assign" in str(ctx.exception)

    def test_attribute_immutable(self):
        # Should be able to set the value once even if it's None
        doc = MyDocument(once=None)
        doc.once == 1

        doc = MyDocument(once="1")
        doc.once == 1

        with self.assertRaises(ValueError) as ctx:
            doc.once = 2
        assert "Unable to set immutable attribute 'once'" == str(ctx.exception)

        with self.assertRaises(ValueError) as ctx:
            doc.once = None
        assert "Unable to set immutable attribute 'once'" == str(ctx.exception)

    def test_attribute_readonly(self):
        with self.assertRaises(ValueError) as ctx:
            MyDocument(id="123")
        assert "Unable to set readonly attribute 'id'" == str(ctx.exception)

        doc = MyDocument()
        with self.assertRaises(ValueError) as ctx:
            doc.id = "123"
        assert "Unable to set readonly attribute 'id'" == str(ctx.exception)

    def test_init_from_server(self):
        now = datetime.utcnow()
        # 2000-01-01, if set to 0 astimezone on windows in python 3.8 will error
        timestamp = 946710000
        data = {
            "id": 1,
            "name": "server",
            "local": "server",
            "once": 2,
            "default": datetime.fromtimestamp(timestamp).isoformat(),
            "created_at": now.isoformat(),
            "extra": "should be ignored",
        }

        doc = MyDocument(**data, saved=True)
        assert doc.id == 1
        assert doc.name == "server"
        assert doc.local == "local"
        assert doc.once == 2
        assert doc.default == datetime.fromtimestamp(timestamp, tz=timezone.utc)
        assert doc.created_at == now.replace(tzinfo=timezone.utc)
        with self.assertRaises(AttributeError):
            doc.extra

    def test_set_from_server(self):
        now = datetime.utcnow()
        doc = MyDocument(name="local", once="1", default=now)
        # 2000-01-01, if set to 0 astimezone on windows in python 3.8 will error
        timestamp = 946710000
        assert doc.once == 1

        data = {
            "id": 1,
            "name": "server",
            "local": "server",
            "once": 2,
            "default": datetime.fromtimestamp(timestamp).isoformat(),
            "created_at": now.isoformat(),
        }
        doc._load_from_remote(data)
        assert doc.id == 1
        assert doc.name == "server"
        assert doc.local == "local"
        assert doc.once == 2
        assert doc.default == datetime.fromtimestamp(timestamp, tz=timezone.utc)
        assert doc.created_at == now.replace(tzinfo=timezone.utc)

    def test_to_dict(self):
        doc = MyDocument(name="local", once="1")
        assert doc.to_dict() == {
            "id": None,
            "name": "local",
            "local": "local",
            "once": 1,
            "default": doc.default.isoformat(),
            "created_at": None,
        }

    def test_deleted(self):
        doc = MyDocument(name="local", once="1")
        doc._deleted = True

        with self.assertRaises(AttributeError) as ctx:
            doc.name
        assert "MyDocument has been deleted" == str(ctx.exception)


class TestDatetimeAttribute(unittest.TestCase):
    def test_local_time(self):
        class TzTest(Document):
            date: datetime = DatetimeAttribute(timezone=pytz.timezone("MST"))

        now = datetime.utcnow()
        doc = TzTest(date=now.isoformat())
        print(f"doc {type(doc)} {doc}")
        print(f"doc.date {type(doc.date)} {doc.date}")
        print(f"doc.date.tzinfo {type(doc.date.tzinfo)} {doc.date.tzinfo}")
        print(f"pytz.timezone(MST)" {type(pytz.timezone('MST'))} {pytz.timezone('MST')}")
        assert doc.date.tzinfo == pytz.timezone("MST")
        assert doc.date.astimezone(tz=timezone.utc) == now.replace(tzinfo=timezone.utc)

        assert doc.to_dict()["date"] == now.replace(tzinfo=timezone.utc).isoformat()

    def test_trailing_z(self):
        class TrailingTest(Document):
            date: datetime = DatetimeAttribute()

        now = datetime.utcnow()
        doc = TrailingTest(date=now.isoformat() + "Z")
        doc.date == now.replace(tzinfo=timezone.utc)

    def test_assign_instance(self):
        tz = pytz.timezone("MST")

        class InstanceTest(Document):
            date: datetime = DatetimeAttribute(timezone=tz)

        now = datetime.utcnow()
        doc = InstanceTest(date=now)
        assert doc.date == now.replace(tzinfo=timezone.utc).astimezone(tz=tz)

    def test_validation(self):
        class ValidationTest(Document):
            date: datetime = DatetimeAttribute()

        with self.assertRaises(ValueError) as ctx:
            doc = ValidationTest(date={})
        assert "Expected datetime, iso formatted date or unix timestamp" in str(
            ctx.exception
        )

        now = datetime.utcnow()
        doc = ValidationTest(date=now.timestamp())
        assert doc.date == now.replace(tzinfo=timezone.utc)

    def test_serialize_filter(self):
        with self.assertRaises(ValueError) as ctx:
            DatetimeAttribute()._serialize_to_filter({})
        assert "Expected datetime, iso formatted date or unix timestamp" in str(
            ctx.exception
        )

        value = DatetimeAttribute()._serialize_to_filter("2023-01-01")
        assert value == datetime(2023, 1, 1, tzinfo=timezone.utc).isoformat()


class TestListAttribute(unittest.TestCase):
    def test_append(self):
        class ListTest(Document):
            items: list = ListAttribute(int)

        doc = ListTest(items=[1, 2], saved=True)
        doc.items.append(3)
        assert doc.items == [1, 2, 3]
        assert doc.is_modified
        assert doc.to_dict()["items"] == [1, 2, 3]

    def test_append_readonly(self):
        class ListTest(Document):
            items: list = ListAttribute(int, readonly=True)

        doc = ListTest(items=[1, 2], saved=True)
        with self.assertRaises(ValueError) as ctx:
            doc.items.append(3)
        assert "Unable to append readonly attribute 'items'" == str(ctx.exception)
        assert doc.items == [1, 2]

    def test_delete(self):
        class ListTest(Document):
            items: list = ListAttribute(int)

        doc = ListTest(items=[1, 2], saved=True)
        del doc.items[0]
        assert doc.items == [2]
        assert doc.is_modified
        assert doc.to_dict()["items"] == [2]

    def test_add_assign(self):
        class ListTest(Document):
            items: list = ListAttribute(int)

        doc = ListTest(items=[1, 2], saved=True)
        doc.items += [3]
        assert doc.items == [1, 2, 3]
        assert doc.is_modified
        assert doc.to_dict()["items"] == [1, 2, 3]

        doc._clear_modified()
        doc.items += []
        assert doc.items == [1, 2, 3]
        assert doc.is_modified is False
        assert doc.to_dict()["items"] == [1, 2, 3]

    def test_clear(self):
        class ListTest(Document):
            items: list = ListAttribute(int)

        doc = ListTest(items=[1, 2], saved=True)
        doc.items.clear()
        assert doc.items == []
        assert doc.is_modified
        assert doc.to_dict()["items"] == []

    def test_extend(self):
        class ListTest(Document):
            items: list = ListAttribute(int)

        doc = ListTest(items=[1, 2], saved=True)
        doc.items.extend([3, 4])
        assert doc.items == [1, 2, 3, 4]
        assert doc.is_modified
        assert doc.to_dict()["items"] == [1, 2, 3, 4]

    def test_insert(self):
        class ListTest(Document):
            items: list = ListAttribute(int)

        doc = ListTest(items=[1, 2], saved=True)
        doc.items.insert(0, 0)
        assert doc.items == [0, 1, 2]
        assert doc.is_modified
        assert doc.to_dict()["items"] == [0, 1, 2]

    def test_pop(self):
        class ListTest(Document):
            items: list = ListAttribute(int)

        doc = ListTest(items=[1, 2, 3], saved=True)
        assert doc.items.pop() == 3
        assert doc.items == [1, 2]
        assert doc.is_modified
        assert doc.to_dict()["items"] == [1, 2]

        doc._clear_modified()
        assert doc.items.pop(0) == 1
        assert doc.items == [2]
        assert doc.is_modified
        assert doc.to_dict()["items"] == [2]

    def test_remove(self):
        class ListTest(Document):
            items: list = ListAttribute(int)

        doc = ListTest(items=[1, 2, 3], saved=True)
        doc.items.remove(2)
        assert doc.items == [1, 3]
        assert doc.is_modified
        assert doc.to_dict()["items"] == [1, 3]

    def test_serializes_type(self):
        class ListTest(Document):
            items: list = ListAttribute(str)

        doc = ListTest(items=[1, 2, 3], saved=True)
        assert doc.to_dict()["items"] == ["1", "2", "3"]
        doc.items.append(4)
        assert doc.is_modified
        assert doc.to_dict()["items"] == ["1", "2", "3", "4"]
