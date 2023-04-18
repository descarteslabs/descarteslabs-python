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

import pytest
import responses
from datetime import datetime
from pytz import utc

from descarteslabs.exceptions import (
    NotFoundError,
    BadRequestError,
    ConflictError,
)

from .base import ClientTestCase
from ..attributes import (
    Attribute,
    AttributeValidationError,
    CatalogObjectReference,
    DocumentState,
)
from ..catalog_base import (
    CatalogClient,
    CatalogObject as OriginalCatalogObject,
    DeletedObjectError,
    UnsavedObjectError,
)
from ..named_catalog_base import NamedCatalogObject


class CatalogObject(OriginalCatalogObject):
    pass


class Foo(CatalogObject):
    _doc_type = "foo"
    _url = "/foo"
    bar = Attribute()


Foo._model_classes_by_type_and_derived_type = {("foo", None): Foo}


class TestCatalogObject(ClientTestCase):
    def test_abstract_class(self):
        with pytest.raises(TypeError):
            OriginalCatalogObject()

        with pytest.raises(TypeError):
            NamedCatalogObject()

    def test_abstract_class_methods(self):
        with pytest.raises(TypeError):
            OriginalCatalogObject.exists("foo")

        with pytest.raises(TypeError):
            OriginalCatalogObject.search()

        with pytest.raises(TypeError):
            OriginalCatalogObject.delete("foo")

        with pytest.raises(TypeError):
            OriginalCatalogObject.get("foo")

        with pytest.raises(TypeError):
            OriginalCatalogObject.get_many(["foo"])

    def test_constructor(self):
        c = CatalogObject(id="id")
        self.assertCountEqual(
            list(c._attribute_types.keys()),
            [
                "id",
                "created",
                "modified",
                "extra_properties",
                "tags",
                "v1_properties",
            ],
        )
        assert c.is_modified

    def test_constructor_no_id(self):
        c = CatalogObject()
        assert c.id is None
        c.id = "id"
        assert "id" == c.id
        assert c.is_modified

        with pytest.raises(AttributeValidationError):
            c.id = "oh no"

    def test_set_get(self):
        c = CatalogObject(id={})
        assert not c.is_modified

        c.tags = ["foo", "bar"]
        assert c.tags == ["foo", "bar"]
        assert c.is_modified

    def test_create_non_attr(self):
        with pytest.raises(AttributeError):
            CatalogObject(foo="bad")

    def test_set_non_attr(self):
        c = CatalogObject(id={})

        with pytest.raises(AttributeError):
            c.foo = "bad"

    def test_serialize(self):
        c = CatalogObject(id="id", tags=["foo", "bar"])
        assert c.tags == ["foo", "bar"]
        assert c.is_modified
        assert {"id", "tags"} == c._modified

        self.assertDictEqual(c.serialize(), dict(tags=["foo", "bar"]))

        self.assertDictEqual(
            c.serialize(jsonapi_format=True),
            dict(
                data=dict(
                    id="id",
                    type=None,
                    attributes=dict(tags=["foo", "bar"]),
                )
            ),
        )

    def test_clear_modified_attributes(self):
        c = CatalogObject(id="id", tags=["foo", "bar"], _saved=True)
        assert not c.is_modified
        c.tags = ["baz"]
        assert c.is_modified
        assert c.serialize(modified_only=True) == {"tags": ["baz"]}

        c._clear_modified_attributes()
        assert not c.is_modified

    def test_list_properties(self):
        c = CatalogObject(id="foo1", tags=["something"], _saved=True)
        assert not c.is_modified

        c.tags.append("nothing")

        assert c.is_modified
        assert c.serialize(modified_only=True) == {
            "tags": ["something", "nothing"],
        }

    @responses.activate
    def test_get(self):
        self.mock_response(
            responses.GET,
            {
                "jsonapi": {"version": "1.0"},
                "data": {
                    "type": Foo._doc_type,
                    "id": "foo1",
                    "attributes": {"bar": "baz"},
                },
            },
        )

        foo = Foo.get("foo1", client=self.client)
        assert foo is not None
        assert foo.id == "foo1"
        assert foo.bar == "baz"
        assert foo.state == DocumentState.SAVED

        CatalogClient.set_default_client(self.client)
        foo = Foo.get("foo1")
        assert foo._client is not None

    @responses.activate
    def test_get_many(self):
        self.mock_response(
            responses.PUT,
            {
                "data": [
                    {
                        "attributes": {"bar": "baz"},
                        "id": "p1:foo",
                        "type": Foo._doc_type,
                    },
                    {
                        "attributes": {"bar": "qux"},
                        "id": "p1:bar",
                        "type": Foo._doc_type,
                    },
                ],
                "jsonapi": {"version": "1.0"},
            },
        )

        with pytest.raises(NotFoundError):
            foos = Foo.get_many(["p1:foo", "p1:bar", "p1:missing"], client=self.client)

        foos = Foo.get_many(
            ["p1:foo", "p1:bar", "p1:missing"], ignore_missing=True, client=self.client
        )
        assert ["p1:foo", "p1:bar"] == [f.id for f in foos]
        assert ["baz", "qux"] == [f.bar for f in foos]

    @responses.activate
    def test_reload(self):
        self.mock_response(
            responses.GET,
            {
                "jsonapi": {"version": "1.0"},
                "data": {
                    "type": Foo._doc_type,
                    "id": "foo1",
                    "attributes": {"bar": "baz"},
                },
            },
        )
        self.mock_response(
            responses.GET,
            {
                "jsonapi": {"version": "1.0"},
                "data": {
                    "type": Foo._doc_type,
                    "id": "foo1",
                    "attributes": {"bar": "qux"},
                },
            },
        )

        foo = Foo.get("foo1", client=self.client)
        assert foo is not None
        assert foo.id == "foo1"
        assert foo.bar == "baz"
        assert foo.state == DocumentState.SAVED

        foo.reload()
        assert foo.id == "foo1"
        assert foo.bar == "qux"
        assert foo.state == DocumentState.SAVED

    @responses.activate
    def test_save_request_params(self):
        self.mock_response(
            responses.POST,
            {
                "jsonapi": {"version": "1.0"},
                "data": {
                    "type": Foo._doc_type,
                    "id": "foo1",
                    "attributes": {"bar": "baz", "foo": "bar"},
                },
            },
        )

        foo = Foo(id="foo1", client=self.client)
        foo.bar = "baz"
        foo.save(request_params={"foo": "bar"})
        assert foo.state == DocumentState.SAVED

        body = self.get_request_body(0)
        assert {"bar": "baz", "foo": "bar"} == body["data"]["attributes"]

    @responses.activate
    def test_save_update_request_params(self):
        self.mock_response(
            responses.PATCH,
            {
                "jsonapi": {"version": "1.0"},
                "data": {
                    "type": Foo._doc_type,
                    "id": "foo1",
                    "attributes": {"bar": "baz", "foo": "bar"},
                },
            },
        )

        foo = Foo(id="foo1", bar="baz", client=self.client, _saved=True)
        foo.save(request_params={"foo": "bar"})
        assert foo.state == DocumentState.SAVED

        body = self.get_request_body(0)
        assert {"foo": "bar"} == body["data"]["attributes"]

    def test_equality(self):
        assert Foo(id="foo2") != Foo(id="foo1")
        assert CatalogObject(id="foo") != Foo(id="foo")
        assert Foo(id="foo") != Foo(id="foo", _saved=True)

        foo1 = Foo(id="foo")
        foo2 = Foo(id="foo")
        assert foo1 == foo2

        foo2.bar = "bar"
        assert foo1 != foo2
        assert foo2 != foo1

        class Bar(CatalogObject):
            _doc_type = "bar"
            _url = "/bar"
            foo_id = Attribute()
            foo = CatalogObjectReference(Foo)

        bar1 = Bar(id="bar", foo_id="foo")
        bar2 = Bar(id="bar", foo_id="foo")
        assert bar1 == bar2

        bar1.foo = Foo(id="foo", _saved=True)
        assert bar1 == bar2

        bar2.foo = Foo(id="foo1", _saved=True)
        assert bar1 != bar2
        assert bar2 != bar1

    def test_hash(self):
        with pytest.raises(TypeError):
            hash(Foo(id="foo"))

    @responses.activate
    def test_delete_classmethod_notfound(self):
        self.mock_response(responses.DELETE, self.not_found_json, status=404)
        assert not Foo.delete("nerp", client=self.client)

    @responses.activate
    def test_delete_classmethod_conflict(self):
        self.mock_response(
            responses.DELETE,
            {
                "errors": [
                    {
                        "detail": "One or more related objects exist",
                        "status": "409",
                        "title": "Related objects exist",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=409,
        )
        with self.assertRaises(ConflictError):
            Foo.delete("nerp", client=self.client)

    @responses.activate
    def test_delete_classmethod(self):
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        assert Foo.delete("nerp", client=self.client)

    @responses.activate
    def test_delete_instancemethod(self):
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )
        instance = Foo(id="nerp", client=self.client, _saved=True)
        instance.delete()
        assert instance.state == DocumentState.DELETED
        assert "* Deleted" in repr(instance)

    def test_delete_not_saved(self):
        foo = Foo(id="nerp", client=self.client)
        with pytest.raises(UnsavedObjectError):
            foo.delete()

    @responses.activate
    def test_delete_instancemethod_notfound(self):
        self.mock_response(responses.DELETE, self.not_found_json, status=404)
        foo = Foo(id="nerp", client=self.client, _saved=True)
        assert foo.state == DocumentState.SAVED
        with pytest.raises(DeletedObjectError):
            foo.delete()
        assert foo.state == DocumentState.DELETED

    @responses.activate
    def test_prevent_operations_after_delete(self):
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )
        instance = Foo(id="merp", client=self.client, _saved=True)
        instance.delete()

        with pytest.raises(DeletedObjectError):
            instance.reload()

    def test_update(self):
        c = CatalogObject()
        assert not c.is_modified
        assert c.state == DocumentState.UNSAVED

        c.update(tags=["tag"])
        assert c.tags == ["tag"]
        assert c.is_modified

    def test_update_immutable_attr(self):
        timestamp = datetime.now(utc)
        c = CatalogObject(id="id", created=timestamp, _saved=True)
        assert not c.is_modified
        assert c.state == DocumentState.SAVED

        with pytest.raises(AttributeValidationError):
            c.update(created=["created"])

        assert c.created == timestamp
        assert not c.is_modified
        assert c.state == DocumentState.SAVED

    def test_update_non_attr(self):
        c = CatalogObject()
        assert not c.is_modified

        with pytest.raises(AttributeError):
            c.update(tags=["foo"], foo=["bar"])

        assert c.tags is None
        with pytest.raises(AttributeError):
            c.foo
        assert not c.is_modified

    def test_update_bad_value(self):
        c = CatalogObject(id="id")
        assert c._modified == set(("id",))

        with pytest.raises(AttributeValidationError):
            c.update(tags=123)

        assert c.id == "id"
        assert c.tags is None
        assert c._modified == set(("id",))

    def test_update_ignore_errors(self):
        c = CatalogObject(id="id")
        assert c._modified == set(("id",))

        c.update(tags=123, ignore_errors=True)
        assert c.id == "id"
        assert c.tags is None
        assert c._modified == set(("id",))

    @responses.activate
    def test_update_deleted_object(self):
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        c = Foo(id="id", _saved=True, client=self.client)
        c.delete()

        with pytest.raises(DeletedObjectError):
            c.update(tags=["foo"])

    @responses.activate
    def test_rewritten_errors(self):
        title = "Validation error"

        self.mock_response(
            responses.POST,
            {
                "errors": [
                    {
                        "detail": "Missing data for required field.",
                        "status": "422",
                        "title": title,
                        "source": {"pointer": "/data/attributes/name"},
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=400,
        )

        try:
            foo = Foo(id="nerp", client=self.client)
            foo.save()
            assert False
        except BadRequestError as error:
            assert str(error) == "\n    {}: {}".format(
                title, "Missing data for required field: name"
            )

    def test_update_no_changes(self):
        c = CatalogObject(id="id", tags=["tag"], _saved=True)
        assert not c.is_modified

        c.update(tags=["tag"])
        assert not c.is_modified

    @responses.activate
    def test_get_or_create(self):
        self.mock_response(responses.GET, self.not_found_json, status=404)

        foo = Foo.get("foo1", client=self.client)
        assert foo is None

        foo = Foo.get_or_create("foo1", bar="baz", client=self.client)
        assert foo is not None
        assert foo.id == "foo1"
        assert foo.bar == "baz"
        assert foo.state == DocumentState.UNSAVED

    @responses.activate
    def test_deleted_notfound(self):
        self.mock_response(responses.PATCH, self.not_found_json, status=404)
        instance = Foo(id="foo", client=self.client, _saved=True)
        instance.bar = "something"

        with pytest.raises(DeletedObjectError):
            instance.save()
        assert instance.state == DocumentState.DELETED

        self.mock_response(responses.GET, self.not_found_json, status=404)
        instance = Foo(id="foo", client=self.client, _saved=True)

        with pytest.raises(DeletedObjectError):
            instance.reload()
        assert instance.state == DocumentState.DELETED
