import responses
import pytest
from mock import patch

from .base import ClientTestCase
from ..catalog_base import DocumentState
from ..image_upload import (
    ImageUpload,
    ImageUploadOptions,
    ImageUploadType,
    ImageUploadStatus,
)
from ..image import Image
from ..attributes import AttributeValidationError


class TestImageUpload(ClientTestCase):
    def test_constructor(self):
        u = ImageUpload(
            id="upload_id",
            product_id="product_id",
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
        )
        assert u.id == "upload_id"
        assert u.product_id == "product_id"
        assert u.image_id == "product_id:image_name"
        assert u.image.id == "product_id:image_name"
        assert u.image_upload_options.upload_type == ImageUploadType.FILE
        assert u.state == DocumentState.UNSAVED

    def test_constructor_no_product_id(self):
        u = ImageUpload(
            id="upload_id",
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
        )
        assert u.id == "upload_id"
        assert u.image.id == u.image.id
        assert u.image_upload_options.upload_type == ImageUploadType.FILE
        assert u.state == DocumentState.UNSAVED

    def test_serialize(self):
        u = ImageUpload(
            id="upload_id",
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
        )
        serialized = u.serialize(jsonapi_format=True)
        self.assertDictEqual(
            dict(
                data=dict(
                    id="upload_id",
                    type=ImageUpload._doc_type,
                    attributes=dict(
                        image_upload_options=dict(upload_type="file"),
                        image_id="product_id:image_name",
                        image=dict(
                            data=dict(
                                id="product_id:image_name",
                                type=Image._doc_type,
                                attributes=dict(
                                    name="image_name", product_id="product_id"
                                ),
                            )
                        ),
                    ),
                )
            ),
            serialized,
        )

    @responses.activate
    def test_save(self):
        self.mock_response(
            # The initial upload request creation
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "resumable_urls": ["http://example.com/uploads/1"],
                        "status": "uploading",
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                    },
                    "type": "image_upload",
                    "id": "upload_id",
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        # The retrieval of the product when the product_id is set on the image
        self.mock_response(
            responses.GET,
            {
                "data": {"attributes": {}, "type": "product", "id": "product_id"},
                "jsonapi": {"version": "1.0"},
            },
        )
        # The update of the update request
        self.mock_response(
            responses.PATCH,
            {
                "data": {
                    "attributes": {
                        "status": "pending",
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "job_id": "job_id",
                    },
                    "type": "image_upload",
                    "id": "upload_id",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        u = ImageUpload(
            id="upload_id",
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
            client=self.client,
        )
        assert u.id == "upload_id"
        assert u.image_id == "product_id:image_name"
        assert u.image_upload_options.upload_type == ImageUploadType.FILE
        assert u.state == DocumentState.UNSAVED

        u.save()

        assert u.id == "upload_id"
        assert u.product_id == "product_id"
        assert u.image_id == "product_id:image_name"
        assert u.image_upload_options.upload_type == ImageUploadType.FILE
        assert u.resumable_urls == ["http://example.com/uploads/1"]
        assert u.status == ImageUploadStatus.UPLOADING
        assert u.state == DocumentState.SAVED

        u.status = ImageUploadStatus.PENDING
        assert u.state == DocumentState.MODIFIED

        u.save()

        assert u.status == ImageUploadStatus.PENDING
        assert u.job_id == "job_id"
        assert u.state == DocumentState.SAVED

    @responses.activate
    @patch("descarteslabs.catalog.image_upload.ImageUpload._POLLING_INTERVAL", 1)
    def test_wait_for_completion(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "resumable_urls": ["http://example.com/uploads/1"],
                        "status": "uploading",
                    },
                    "type": "image_upload",
                    "id": "upload_id",
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.PATCH,
            {
                "data": {
                    "attributes": {
                        "status": "pending",
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "job_id": "job_id",
                        "events": [],
                        "errors": [],
                    },
                    "type": "image_upload",
                    "id": "upload_id",
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(responses.GET, None, 404)
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "status": "success",
                        "job_id": "job_id",
                        "events": [],
                        "errors": [],
                    },
                    "type": "image_upload",
                    "id": "upload_id",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        u = ImageUpload(
            id="upload_id",
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
            client=self.client,
        )
        u.save()
        u.status = ImageUploadStatus.PENDING
        u.save()

        assert u.status == ImageUploadStatus.PENDING
        assert u.state == DocumentState.SAVED

        u.wait_for_completion(15)

        assert u.status == ImageUploadStatus.SUCCESS

    @responses.activate
    @patch("descarteslabs.catalog.image_upload.ImageUpload._POLLING_INTERVAL", 1)
    def test_reload_failed(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "attributes": {
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "resumable_urls": ["http://example.com/uploads/1"],
                        "status": "pending",
                    },
                    "type": "image_upload",
                    "id": "upload_id",
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        self.mock_response(
            responses.GET,
            {
                "meta": {"count": 1},
                "data": {
                    "type": "image_upload",
                    "id": "ZGVzY2FydGVzbGFiczptb2xseS10ZXN0LXVwbG9hZA==:blue.tif",
                    "attributes": {
                        "status": "failure",
                        "product_id": "product_id",
                        "image_id": "product_id:image2",
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
                "links": {"self": "https://www.example.com/catalog/v2/uploads"},
                "jsonapi": {"version": "1.0"},
            },
        )
        u = ImageUpload(
            id="upload_id",
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
            client=self.client,
        )

        assert u.errors is None
        with pytest.raises(AttributeValidationError):
            u.errors = [{"component": "task", "error_type": "TypeError"}]

        u.save()
        assert u.status == ImageUploadStatus.PENDING
        u.reload()

        assert u.status == ImageUploadStatus.FAILURE
        assert len(u.errors) == 1
        assert u.errors[0].component == "worker"

        with pytest.raises(AttributeValidationError):
            u.errors[0].component = "task-controller"

        with pytest.raises(AttributeValidationError):
            u.errors.pop()
