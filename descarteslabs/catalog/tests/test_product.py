# -*- coding: utf-8 -*-
import pytest
import responses
from datetime import datetime
import textwrap
import json
from mock import patch
from six import ensure_str

from descarteslabs.client.exceptions import BadRequestError, NotFoundError
from .base import ClientTestCase
from ..catalog_base import DocumentState
from ..attributes import AttributeValidationError, ListAttribute
from ..product import (
    Product,
    Resolution,
    TaskState,
    TaskStatus,
    DeletionTaskStatus,
    UpdatePermissionsTaskStatus,
)
from ..band import DerivedBand

from ..image_upload import ImageUploadStatus
from .. import properties


class TestProduct(ClientTestCase):
    def test_constructor(self):
        p = Product(
            id="p1",
            name="Test Product",
            start_datetime="2019-01-01",
            resolution_min={"value": 10.0, "unit": "meters"},
        )
        assert p.id == "p1"
        assert p.name == "Test Product"

        assert p.resolution_min.value == 10.0
        assert p.resolution_min.unit == "meters"
        assert p.state == DocumentState.UNSAVED

    def test_repr_non_ascii(self):
        p = Product(id="plieades", name="Pléiades")
        p_repr = repr(p)
        match_str = """\
            Product: Pléiades
              id: plieades
            * Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."""
        assert p_repr.strip("\n") == textwrap.dedent(match_str)

    def test_resolution(self):
        p = Product(
            id="p1",
            name="Test Product",
            resolution_min=Resolution(value=10.0, unit="meters"),
            _saved=True,
        )

        assert isinstance(p.resolution_min, Resolution)
        assert not p.is_modified

        new_resolution = Resolution(value=15.0, unit="degrees")
        p.resolution_min = new_resolution

        assert p.resolution_min.value == new_resolution.value
        assert p.resolution_min.unit == new_resolution.unit
        assert p.resolution_min is new_resolution
        assert p.is_modified

    def test_resolution_new(self):
        p = Product(
            id="p1",
            name="Test Product",
            resolution_min={"value": 10.0, "unit": "miles"},
            _saved=True,
        )

        assert p.resolution_min.unit == "miles"
        with pytest.raises(ValueError):
            Resolution(value=15.0, unit="miles")

    @responses.activate
    def test_list(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 1},
                "data": [
                    {
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "name": "My Test Product",
                            "readers": [],
                            "revisit_period_minutes_min": None,
                            "revisit_period_minutes_max": None,
                            "modified": "2019-06-10T18:48:13.066192Z",
                            "created": "2019-06-10T18:48:13.066192Z",
                            "start_datetime": "2019-01-01T00:00:00Z",
                            "writers": [],
                            "end_datetime": None,
                            "description": None,
                            "resolution_min": {"value": 10.0, "unit": "meters"},
                        },
                        "type": "product",
                        "id": "descarteslabs:test",
                    }
                ],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/products"},
            },
        )

        r = list(Product.search(client=self.client))
        assert len(r) == 1
        product = r[0]
        assert responses.calls[0].request.url == self.url + "/products"
        assert product.id == "descarteslabs:test"
        assert isinstance(product.created, datetime)
        assert isinstance(product.resolution_min, Resolution)

        with pytest.raises(AttributeValidationError):
            product.created = "2018-06-10T18:48:13.066192Z"

        assert isinstance(product.start_datetime, datetime)

    @responses.activate
    def test_list_no_results(self):
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 0},
                "data": [],
                "jsonapi": {"version": "1.0"},
                "links": {"self": "https://example.com/catalog/v2/products"},
            },
        )

        r = list(Product.search(client=self.client))
        assert r == []

    @responses.activate
    def test_save(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "owners": ["org:descarteslabs"],
                        "name": "My Test Product",
                        "readers": [],
                        "revisit_period_minutes_min": None,
                        "revisit_period_minutes_max": None,
                        "modified": "2019-06-10T18:48:13.066192Z",
                        "created": "2019-06-10T18:48:13.066192Z",
                        "start_datetime": "2019-01-01T00:00:00Z",
                        "writers": [],
                        "end_datetime": None,
                        "description": None,
                        "resolution_min": {"value": 10.0, "unit": "meters"},
                    },
                    "type": "product",
                    "id": "descarteslabs:test",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        p = Product(id="p1", name="Test Product", client=self.client)
        assert p.state == DocumentState.UNSAVED
        p.save()
        assert responses.calls[0].request.url == self.url + "/products"
        assert p.state == DocumentState.SAVED
        # id updated on initial save
        assert "p1" != p.id
        assert isinstance(p.start_datetime, datetime)

    @responses.activate
    def test_save_dupe(self):
        self.mock_response(
            responses.POST,
            {
                "errors": [
                    {
                        "status": "400",
                        "detail": "A document with id `descarteslabs:p1` already exists.",
                        "title": "Bad request",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=400,
        )
        p = Product(id="p", name="Test Product", client=self.client)
        with pytest.raises(BadRequestError):
            p.save()

    @responses.activate
    def test_an_update(self):
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
                    },
                    "type": "product",
                    "id": "descarteslabs:my-product",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        p1 = Product.get("descarteslabs:my-product", client=self.client)
        assert p1.state == DocumentState.SAVED

        p1_repr = repr(p1)
        match_str = """\
            Product: My Product
              id: descarteslabs:my-product
              created: Tue Jun 11 23:52:35 2019"""
        assert p1_repr.strip("\n") == textwrap.dedent(match_str)

        p1.description = "An updated description"
        assert p1.state == DocumentState.MODIFIED
        self.mock_response(
            responses.PATCH,
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
                        "description": "An updated description",
                    },
                    "type": "product",
                    "id": "descarteslabs:my-product",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        p1_repr = repr(p1)
        match_str = """\
            Product: My Product
              id: descarteslabs:my-product
              created: Tue Jun 11 23:52:35 2019
            * Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."""

        assert p1_repr.strip("\n") == textwrap.dedent(match_str)

        p1.save()
        assert json.loads(responses.calls[1].request.body.decode("utf-8")) == {
            "data": {
                "type": "product",
                "id": "descarteslabs:my-product",
                "attributes": {"description": "An updated description"},
            }
        }

    @responses.activate
    def test_delete(self):
        p = Product(
            id="descarteslabs:my-product",
            name="My Product",
            client=self.client,
            _saved=True,
        )
        self.mock_response(
            responses.DELETE,
            {
                "meta": {"message": "Object successfully deleted"},
                "jsonapi": {"version": "1.0"},
            },
        )

        deleted = p.delete()
        assert deleted

    @responses.activate
    def test_delete_non_existent(self):
        p = Product(
            id="ne-my-product", name="Non-existent", client=self.client, _saved=True
        )
        self.mock_response(
            responses.DELETE,
            {
                "errors": [
                    {
                        "detail": "Object not found: ne",
                        "status": "404",
                        "title": "Object not found",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=404,
        )

        with pytest.raises(NotFoundError):
            p.delete()

    @responses.activate
    def test_exists(self):
        # head request, no JSON is returned
        self.mock_response(responses.HEAD, {})
        assert Product.exists("my-id:id", client=self.client)
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/products/my-id:id"
        )

    @responses.activate
    def test_exists_false(self):
        self.mock_response(responses.HEAD, {}, status=404)
        assert not Product.exists("my-id:id", client=self.client)
        assert (
            responses.calls[0].request.url
            == "https://example.com/catalog/v2/products/my-id:id"
        )

    @responses.activate
    def test_get_unknown_attribute(self):
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
                    },
                    "type": "product",
                    "id": "descarteslabs:my-product",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        p = Product.get("descarteslabs:my-product", client=self.client)
        assert not hasattr(p, "foobar")

    @responses.activate
    def test_create_product_delete_task(self):
        p = Product(id="p1", name="Test Product", client=self.client)
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {"status": "RUNNING"},
                    "type": "product_delete_task",
                    "id": "descarteslabs:test-product",
                },
                "jsonapi": {"version": "1.0"},
            },
            status=201,
        )
        r = p.delete_related_objects()
        req = responses.calls[0].request
        assert r.status == TaskState.RUNNING
        assert (
            req.url
            == "https://example.com/catalog/v2/products/p1/delete_related_objects"
        )
        assert req.body == b'{"data": {"type": "product_delete_task"}}'

    @responses.activate
    def test_no_objects_to_delete(self):
        p = Product(id="p1", name="Test Product", client=self.client)
        self.mock_response(
            responses.POST,
            {
                "errors": [
                    {
                        "status": "204",
                        "detail": "A 'delete related objects' operation is not needed: p1",
                        "title": "No related objects found",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=204,
        )
        r = p.delete_related_objects()
        assert not r

    def test_abstract_status_class(self):
        with pytest.raises(TypeError):
            TaskStatus()

    @responses.activate
    def test_get_delete_status(self):
        p = Product(id="p1", name="Test Product", client=self.client)
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "status": "SUCCESS",
                        "start_datetime": "2019-08-10T00:10:17.528903Z",
                        "errors": None,
                        "duration_in_seconds": 0.36756521779382323,
                        "objects_deleted": 2,
                    },
                    "type": "product_delete_task",
                    "id": "p1",
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        r = p.get_delete_status()
        assert r.status == TaskState.SUCCEEDED
        assert isinstance(r, DeletionTaskStatus)

        status_repr = repr(r)
        match_str = """\
            p1 delete task status: SUCCESS
              - started: 2019-08-10T00:10:17.528903Z
              - took 0.3676 seconds
              - 2 objects deleted"""

        assert status_repr.strip("\n") == textwrap.dedent(match_str)

    @responses.activate
    def test_update_related_acls_task(self):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)

        self.mock_response(
            responses.POST,
            {
                "data": {
                    "type": "product_update_acls",
                    "attributes": {"status": "RUNNING"},
                    "id": "p1",
                },
                "jsonapi": {"version": "1.0"},
            },
            status=201,
        )
        r = p.update_related_objects_permissions(
            owners=["org:descarteslabs"], readers=["group:public"]
        )
        assert r.status == TaskState.RUNNING
        req = responses.calls[0].request
        assert (
            req.url
            == "https://example.com/catalog/v2/products/p1/update_related_objects_acls"
        )

        body = json.loads(ensure_str(req.body))["data"]["attributes"]
        assert body["readers"] == ["group:public"]
        assert body["owners"] == ["org:descarteslabs"]
        assert body["writers"] is None

    @responses.activate
    def test_update_acls_task_status(self):
        p = Product(
            id="p1",
            name="Test Product",
            readers=["group:public"],
            client=self.client,
            _saved=True,
        )

        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "product_update_acls",
                    "attributes": {
                        "start_datetime": "2019-09-17T21:53:07.348000Z",
                        "duration_in_seconds": 0.0153,
                        "status": "SUCCESS",
                        "objects_updated": 1,
                        "errors": None,
                    },
                    "id": "descarteslabs:prod4",
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        r = p.get_update_permissions_status()
        assert isinstance(r, UpdatePermissionsTaskStatus)
        assert r.status == TaskState.SUCCEEDED

        status_repr = repr(r)
        match_str = """\
            p1 update permissions task status: SUCCESS
              - started: 2019-09-17T21:53:07.348000Z
              - took 0.0153 seconds
              - 1 objects updated"""
        assert status_repr.strip("\n") == textwrap.dedent(match_str)

    @responses.activate
    @patch(
        "descarteslabs.catalog.product.UpdatePermissionsTaskStatus._POLLING_INTERVAL", 1
    )
    def test_wait_for_completion(self):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "product_update_acls",
                    "id": "p1",
                    "attributes": {"status": "RUNNING"},
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "product_update_acls",
                    "id": "p1",
                    "attributes": {"status": "RUNNING"},
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "product_update_acls",
                    "id": "p1",
                    "attributes": {
                        "status": "SUCCESS",
                        "errors": None,
                        "duration_in_seconds": 0.012133697,
                        "objects_updated": 1,
                        "start_datetime": "2019-09-18T00:27:43.230000Z",
                    },
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        update_status = p.get_update_permissions_status()
        assert update_status.status == TaskState.RUNNING
        update_status.wait_for_completion()
        assert update_status.status == TaskState.SUCCEEDED

    @responses.activate
    def test_image_uploads(self):
        product_id = "p1"

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
                    },
                    "type": "product",
                    "id": product_id,
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 1},
                "data": [
                    {
                        "attributes": {
                            "product_id": product_id,
                            "image_id": product_id + ":image",
                            "end_datetime": "2019-01-01T00:10:00Z",
                            "start_datetime": "2019-01-01T00:00:00Z",
                            "status": "success",
                        },
                        "id": "cDE=:umyimage",
                        "type": "image_upload",
                    },
                    {
                        "type": "image_upload",
                        "id": "ZGVzY2FydGVzbGFiczptb2xseS10ZXN0LXVwbG9hZA==:blue.tif",
                        "attributes": {
                            "status": "failure",
                            "product_id": product_id,
                            "image_id": product_id + ":image2",
                            "errors": [
                                {
                                    "component": "worker",
                                    "stacktrace": "Traceback",
                                    "error_type": "NotFoundError",
                                    "component_id": "a107d4d2_751402964324890",
                                }
                            ],
                        },
                    },
                ],
                "links": {"self": "https://www.example.com/catalog/v2/uploads"},
                "jsonapi": {"version": "1.0"},
            },
        )

        product = Product.get(product_id, client=self.client)

        uploads = list(product.image_uploads())

        assert len(uploads) == 2
        upload = uploads[0]
        assert upload.id == "cDE=:umyimage"
        assert upload.product_id == product_id
        assert upload.image_id == product_id + ":image"
        assert upload.status == ImageUploadStatus.SUCCESS
        failed_upload = uploads[1]
        assert failed_upload.status == ImageUploadStatus.FAILURE
        assert isinstance(failed_upload.errors, ListAttribute)
        assert failed_upload.errors[0].component == "worker"

        with pytest.raises(AttributeValidationError):
            failed_upload.errors.append({"error_type": "ImportError"})

        with pytest.raises(AttributeValidationError):
            failed_upload.errors[0].component = "task-controller"

    @responses.activate
    def test_derived_bands(self):
        self.mock_response(
            responses.PUT,
            {
                "data": [
                    {
                        "type": "derived_band",
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "writers": None,
                            "data_range": [0.0, 255.0],
                            "name": "derived:ndvi",
                            "data_type": "Byte",
                            "tags": None,
                            "readers": ["group:descarteslabs:engineering"],
                            "function_name": "test",
                            "extra_properties": {},
                            "description": None,
                            "physical_range": None,
                            "bands": ["red", "nir"],
                        },
                        "id": "derived:ndvi",
                    },
                    {
                        "type": "derived_band",
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "writers": None,
                            "data_range": [0.0, 255.0],
                            "name": "green",
                            "data_type": "Byte",
                            "tags": None,
                            "readers": ["group:descarteslabs:engineering"],
                            "function_name": "test",
                            "extra_properties": {},
                            "description": None,
                            "physical_range": None,
                            "bands": ["blue"],
                        },
                        "id": "derived:rsqrt",
                    },
                ],
                "links": {
                    "self": "https://www.example.com/catalog/v2/products/p1/relationships/derived_bands",
                    "related": None,
                    "first": "https://www.example.com/catalog/v2/products/p1/relationships/derived_bands",
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        derived_bands = list(p.derived_bands())
        assert responses.calls[
            0
        ].request.url == "{}/products/{}/relationships/derived_bands".format(
            self.url, p.id
        )
        assert len(derived_bands) == 2
        assert isinstance(derived_bands[0], DerivedBand)

    @responses.activate
    def test_derived_bands_filters(self):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        self.mock_response(
            responses.PUT,
            {
                "data": [
                    {
                        "type": "derived_band",
                        "attributes": {
                            "owners": ["org:descarteslabs"],
                            "writers": None,
                            "data_range": [0.0, 255.0],
                            "name": "green",
                            "data_type": "Byte",
                            "tags": None,
                            "readers": ["group:descarteslabs:engineering"],
                            "function_name": "test",
                            "extra_properties": {},
                            "description": None,
                            "physical_range": None,
                            "bands": ["blue"],
                        },
                        "id": "derived:rsqrt",
                    }
                ],
                "links": {
                    "self": "https://www.example.com/catalog/v2/products/p1/relationships/derived_bands",
                    "related": None,
                    "first": "https://www.example.com/catalog/v2/products/p1/relationships/derived_bands",
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        s = p.derived_bands().filter(properties.name == "green")
        filtered_derived_bands = list(s)
        req = responses.calls[0].request
        assert req.url == "{}/products/{}/relationships/derived_bands".format(
            self.url, p.id
        )
        assert s._serialize_filters() == [{"op": "eq", "name": "name", "val": "green"}]
        assert len(filtered_derived_bands) == 1

    @responses.activate
    def test_core_product(self):
        product_id = "p1"

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
                        "is_core": True,
                    },
                    "type": "product",
                    "id": "product_id",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        product = Product()
        product.id = product_id
        product.is_core = "True"

        self.assertEqual(product.serialize(), {"is_core": True})
        product.save()
        self.assertEqual(product.is_core, True)

    def test_namespace_id(self):
        class Client(object):
            class auth(object):
                namespace = "mynamespace"
                payload = {"org": "descarteslabs"}

        assert Product.namespace_id("foo", client=Client) == "descarteslabs:foo"
        assert (
            Product.namespace_id("descarteslabs:foo", client=Client)
            == "descarteslabs:foo"
        )
        assert (
            Product.namespace_id("descarteslabs:foo:bar:baz", client=Client)
            == "descarteslabs:foo:bar:baz"
        )

        del Client.auth.payload["org"]

        assert Product.namespace_id("foo", client=Client) == "mynamespace:foo"
