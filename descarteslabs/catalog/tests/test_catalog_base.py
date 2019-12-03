import json
import pytest
import responses
from six import assertCountEqual, ensure_str

from descarteslabs.client.exceptions import NotFoundError

from .base import ClientTestCase
from ..attributes import (
    Attribute,
    AttributeValidationError,
    CatalogObjectReference,
    DocumentState,
)
from ..catalog_base import (
    CatalogClient,
    CatalogObject,
    DeletedObjectError,
    UnsavedObjectError,
)


class Foo(CatalogObject):
    _doc_type = "foo"
    _url = "/foo"
    bar = Attribute()


class TestCatalogObject(ClientTestCase):
    def test_constructor(self):
        c = CatalogObject(id="id")
        assertCountEqual(
            self,
            list(c._attribute_types.keys()),
            [
                "id",
                "owners",
                "writers",
                "readers",
                "created",
                "modified",
                "extra_properties",
                "tags",
            ],
        )
        assert c.owners is None
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

        c.owners = ["user", "org"]
        assert c.owners == ["user", "org"]
        assert c.is_modified

    def test_serialize(self):
        c = CatalogObject(id="id", owners=["user", "org"], readers=["public"])
        assert c.readers == ["public"]
        assert c.is_modified
        assert {"id", "owners", "readers"} == c._modified

        self.assertDictEqual(
            c.serialize(), dict(owners=["user", "org"], readers=["public"])
        )

        self.assertDictEqual(
            c.serialize(jsonapi_format=True),
            dict(
                data=dict(
                    id="id",
                    type=None,
                    attributes=dict(owners=["user", "org"], readers=["public"]),
                )
            ),
        )

    def test_clear_modified_attributes(self):
        c = CatalogObject(
            id="id", owners=["user", "org"], readers=["public"], _saved=True
        )
        assert not c.is_modified
        c.owners = ["org"]
        assert c.is_modified
        assert c.serialize(modified_only=True) == {"owners": ["org"]}

        c._clear_modified_attributes()
        assert not c.is_modified

    def test_list_properties(self):
        c = CatalogObject(id="foo1", owners=["foo"], tags=["something"], _saved=True)
        assert not c.is_modified

        c.owners.append("bar")
        c.tags.append("nothing")

        assert c.is_modified
        assert c.serialize(modified_only=True) == {
            "owners": ["foo", "bar"],
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
    def test_save_extra_attributes(self):
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
        foo.save(extra_attributes={"foo": "bar"})
        assert foo.state == DocumentState.SAVED

        body = json.loads(ensure_str(responses.calls[0].request.body))
        assert {"bar": "baz", "foo": "bar"} == body["data"]["attributes"]

    @responses.activate
    def test_save_update_extra_attributes(self):
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
        foo.save(extra_attributes={"foo": "bar"})
        assert foo.state == DocumentState.SAVED

        body = json.loads(ensure_str(responses.calls[0].request.body))
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
        self.mock_response(
            responses.DELETE,
            {
                "errors": [
                    {
                        "detail": "Object not found: nerp",
                        "status": "404",
                        "title": "Object not found",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=404,
        )
        with pytest.raises(NotFoundError):
            Foo.delete("nerp", client=self.client)

        r = Foo.delete("nerp", ignore_missing=True, client=self.client)
        assert r is False

    @responses.activate
    def test_delete_classmethod(self):
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        r = Foo.delete("nerp", client=self.client)
        assert r is True

    @responses.activate
    def test_delete_instancemethod(self):
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )
        instance = Foo(id="merp", client=self.client, _saved=True)
        r = instance.delete()
        assert r is True
        assert instance.state == DocumentState.DELETED

    def test_delete_not_saved(self):
        foo = Foo(id="nerp", client=self.client)
        with pytest.raises(UnsavedObjectError):
            foo.delete()

    @responses.activate
    def test_delete_instancemethod_notfound(self):
        self.mock_response(
            responses.DELETE,
            {
                "errors": [
                    {
                        "detail": "Object not found: nerp",
                        "status": "404",
                        "title": "Object not found",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=404,
        )
        foo = Foo(id="nerp", client=self.client, _saved=True)
        with pytest.raises(NotFoundError):
            foo.delete()

        instance = Foo(id="nerp", client=self.client, _saved=True)
        r = instance.delete(ignore_missing=True)
        assert r is False
        assert instance.state != DocumentState.DELETED

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
