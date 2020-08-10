import warnings
import responses
from mock import patch

from datetime import datetime

from .base import ClientTestCase
from ..catalog_base import DocumentState
from ..image_upload import (
    ImageUpload,
    ImageUploadOptions,
    ImageUploadType,
    ImageUploadStatus,
    ImageUploadEventType,
    ImageUploadEventSeverity,
)
from ..image import Image
from ..attributes import utc


class TestImageUpload(ClientTestCase):
    def test_constructor(self):
        u = ImageUpload(
            product_id="product_id",
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
        )
        assert u.id is None
        assert u.created is None
        assert u.modified is None
        assert u.product_id == "product_id"
        assert u.image_id == "product_id:image_name"
        assert u.image.id == "product_id:image_name"
        assert u.image_upload_options.upload_type == ImageUploadType.FILE
        assert u.state == DocumentState.UNSAVED
        assert u.status is None
        assert not u.events

    def test_constructor_no_product_id(self):
        u = ImageUpload(
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
        )
        assert u.image.id == u.image.id
        assert u.image_upload_options.upload_type == ImageUploadType.FILE
        assert u.state == DocumentState.UNSAVED

    def test_serialize(self):
        u = ImageUpload(
            image=Image(name="image_name", product_id="product_id"),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
        )
        serialized = u.serialize(jsonapi_format=True)
        self.assertDictEqual(
            dict(
                data=dict(
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
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "resumable_urls": ["http://example.com/uploads/1"],
                        "status": ImageUploadStatus.TRANSFERRING.value,
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                    },
                    "relationships": {"events": {"data": []}},
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
        # The update of the upload request
        self.mock_response(
            responses.PATCH,
            {
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "status": ImageUploadStatus.PENDING.value,
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                    },
                    "relationships": {
                        "events": {"data": [{"type": "image_upload_event", "id": "1"}]}
                    },
                },
                "included": [
                    {
                        "type": "image_upload_event",
                        "id": "1",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas",
                            "component_id": "yaas-1",
                            "event_type": ImageUploadEventType.QUEUE.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "message-id=1",
                        },
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
        )
        # The cancel of the upload request
        self.mock_response(
            responses.PATCH,
            {
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "status": ImageUploadStatus.CANCELED.value,
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                    },
                    "relationships": {
                        "events": {
                            "data": [
                                {"type": "image_upload_event", "id": "1"},
                                {"type": "image_upload_event", "id": "2"},
                            ]
                        }
                    },
                },
                "included": [
                    {
                        "type": "image_upload_event",
                        "id": "1",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas",
                            "component_id": "yaas-1",
                            "event_type": ImageUploadEventType.QUEUE.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "message-id=1",
                        },
                    },
                    {
                        "type": "image_upload_event",
                        "id": "2",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas",
                            "component_id": "yaas-1",
                            "event_type": ImageUploadEventType.CANCEL.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "Canceled",
                        },
                    },
                ],
                "jsonapi": {"version": "1.0"},
            },
        )

        u = ImageUpload(
            image=Image(
                name="image_name", product_id="product_id", acquired="2020-01-01"
            ),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
            client=self.client,
        )
        assert u.image_id == "product_id:image_name"
        assert u.image_upload_options.upload_type == ImageUploadType.FILE
        assert u.state == DocumentState.UNSAVED

        u.save()

        assert u.id == "1"
        assert u.created == datetime(2020, 1, 1, 0, 0, 0, tzinfo=utc)
        assert u.modified == datetime(2020, 1, 1, 0, 0, 0, tzinfo=utc)
        assert u.product_id == "product_id"
        assert u.image_id == "product_id:image_name"
        assert u.image_upload_options.upload_type == ImageUploadType.FILE
        assert u.resumable_urls == ["http://example.com/uploads/1"]
        assert u.status == ImageUploadStatus.TRANSFERRING
        assert u.state == DocumentState.SAVED

        u.status = ImageUploadStatus.PENDING
        assert u.state == DocumentState.MODIFIED

        u.save()

        assert u.status == ImageUploadStatus.PENDING
        assert u.state == DocumentState.SAVED
        assert len(u.events) == 1
        assert u.events[0].event_datetime == datetime(2020, 1, 1, 0, 0, 0, tzinfo=utc)
        assert u.events[0].event_type == ImageUploadEventType.QUEUE
        assert u.events[0].severity == ImageUploadEventSeverity.INFO

        u.cancel()

        assert u.status == ImageUploadStatus.CANCELED
        assert u.state == DocumentState.SAVED
        assert len(u.events) == 2
        assert u.events[1].event_datetime == datetime(2020, 1, 1, 0, 0, 0, tzinfo=utc)
        assert u.events[1].event_type == ImageUploadEventType.CANCEL
        assert u.events[1].severity == ImageUploadEventSeverity.INFO

    @responses.activate
    @patch("descarteslabs.catalog.image_upload.ImageUpload._POLLING_INTERVALS", [1])
    def test_wait_for_completion(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "resumable_urls": ["http://example.com/uploads/1"],
                        "status": ImageUploadStatus.TRANSFERRING.value,
                    },
                    "relationships": {"events": {"data": []}},
                },
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.PATCH,
            {
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "status": ImageUploadStatus.PENDING.value,
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                    },
                    "relationships": {
                        "events": {"data": [{"type": "image_upload_event", "id": "1"}]}
                    },
                },
                "included": [
                    {
                        "type": "image_upload_event",
                        "id": "1",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas",
                            "component_id": "yaas-1",
                            "event_type": ImageUploadEventType.QUEUE.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "message-id=1",
                        },
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "status": ImageUploadStatus.RUNNING.value,
                    },
                    "relationships": {
                        "events": {
                            "data": [
                                {"type": "image_upload_event", "id": "1"},
                                {"type": "image_upload_event", "id": "2"},
                            ]
                        }
                    },
                },
                "included": [
                    {
                        "type": "image_upload_event",
                        "id": "1",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas",
                            "component_id": "yaas-1",
                            "event_type": ImageUploadEventType.QUEUE.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "message-id=1",
                        },
                    },
                    {
                        "type": "image_upload_event",
                        "id": "2",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas-worker",
                            "component_id": "yaas-worker-1",
                            "event_type": ImageUploadEventType.RUN.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "Starting job attempt 1",
                        },
                    },
                ],
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "status": ImageUploadStatus.SUCCESS.value,
                        "events": [],
                    },
                    "relationships": {
                        "events": {
                            "data": [
                                {"type": "image_upload_event", "id": "1"},
                                {"type": "image_upload_event", "id": "2"},
                                {"type": "image_upload_event", "id": "3"},
                            ]
                        }
                    },
                },
                "included": [
                    {
                        "type": "image_upload_event",
                        "id": "1",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas",
                            "component_id": "yaas-1",
                            "event_type": ImageUploadEventType.QUEUE.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "message-id=1",
                        },
                    },
                    {
                        "type": "image_upload_event",
                        "id": "2",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas-worker",
                            "component_id": "yaas-worker-1",
                            "event_type": ImageUploadEventType.RUN.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "Starting job attempt 1",
                        },
                    },
                    {
                        "type": "image_upload_event",
                        "id": "3",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas-worker",
                            "component_id": "yaas-worker-1",
                            "event_type": ImageUploadEventType.COMPLETE.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "Success",
                        },
                    },
                ],
                "jsonapi": {"version": "1.0"},
            },
        )
        self.mock_response(
            responses.GET,
            {
                "errors": [
                    {
                        "detail": "Something went wrong",
                        "status": "500",
                        "title": "Server Error",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=500,
        )

        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "image",
                    "id": "product_id:image_name",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "product_id": "product_id",
                        "name": "image_name",
                        "readers": [],
                        "writers": [],
                        "owners": ["org:descarteslabs"],
                        "acquired": "2020-01-01T00:00:00Z",
                        "geometry": {
                            "type": "Polygon",
                            "coordinates": [
                                [
                                    [-9.000262842437783, 46.9537091787344],
                                    [-8.325270159894608, 46.95172107428039],
                                    [-8.336543403548475, 46.925857032669434],
                                    [-9.000262842437783, 46.7807657614384],
                                    [-9.000262842437783, 46.9537091787344],
                                ]
                            ],
                        },
                    },
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        u = ImageUpload(
            image=Image(
                name="image_name", product_id="product_id", acquired="2020-01-01"
            ),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
            client=self.client,
        )
        u.save()
        u.status = ImageUploadStatus.PENDING
        u.save()

        assert u.status == ImageUploadStatus.PENDING
        assert u.state == DocumentState.SAVED

        with warnings.catch_warnings(record=True) as w:
            u.wait_for_completion(15)

            assert len(w) == 1
            assert "Something went wrong" in str(w[0].message)

        assert u.status == ImageUploadStatus.SUCCESS
        assert len(u.events) == 3
        assert [e.event_type for e in u.events] == [
            ImageUploadEventType.QUEUE,
            ImageUploadEventType.RUN,
            ImageUploadEventType.COMPLETE,
        ]

    @responses.activate
    @patch("descarteslabs.catalog.image_upload.ImageUpload._POLLING_INTERVALS", [1])
    def test_reload_failed(self):
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "product_id": "product_id",
                        "image_id": "product_id:image_name",
                        "status": ImageUploadStatus.PENDING.value,
                    },
                    "relationships": {
                        "events": {"data": [{"type": "image_upload_event", "id": "1"}]}
                    },
                },
                "included": [
                    {
                        "type": "image_upload_event",
                        "id": "1",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas",
                            "component_id": "yaas-1",
                            "event_type": ImageUploadEventType.QUEUE.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "message-id=1",
                        },
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
        )

        self.mock_response(
            responses.GET,
            {
                "meta": {"count": 1},
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2020-01-01T00:00:00.000000Z",
                        "modified": "2020-01-01T00:00:00.000000Z",
                        "product_id": "product_id",
                        "image_id": "product_id:image2",
                        "status": ImageUploadStatus.FAILURE.value,
                    },
                    "relationships": {
                        "events": {
                            "data": [
                                {"type": "image_upload_event", "id": "1"},
                                {"type": "image_upload_event", "id": "2"},
                                {"type": "image_upload_event", "id": "3"},
                            ]
                        }
                    },
                },
                "included": [
                    {
                        "type": "image_upload_event",
                        "id": "1",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas",
                            "component_id": "yaas-1",
                            "event_type": ImageUploadEventType.QUEUE.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "message-id=1",
                        },
                    },
                    {
                        "type": "image_upload_event",
                        "id": "2",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas-worker",
                            "component_id": "yaas-worker-1",
                            "event_type": ImageUploadEventType.RUN.value,
                            "severity": ImageUploadEventSeverity.INFO.value,
                            "message": "Starting job attempt 1",
                        },
                    },
                    {
                        "type": "image_upload_event",
                        "id": "3",
                        "attributes": {
                            "event_datetime": "2020-01-01T00:00:00.000000Z",
                            "component": "yaas-worker",
                            "component_id": "yaas-worker-1",
                            "event_type": ImageUploadEventType.COMPLETE.value,
                            "severity": ImageUploadEventSeverity.ERROR.value,
                            "message": "Failure",
                        },
                    },
                ],
                "jsonapi": {"version": "1.0"},
            },
        )
        u = ImageUpload(
            image=Image(
                name="image_name", product_id="product_id", acquired="2020-01-01"
            ),
            image_upload_options=ImageUploadOptions(upload_type=ImageUploadType.FILE),
            client=self.client,
        )

        u.save()
        assert u.status == ImageUploadStatus.PENDING
        u.reload()

        assert u.status == ImageUploadStatus.FAILURE
        assert len(u.events) == 3
        assert [e.event_type for e in u.events] == [
            ImageUploadEventType.QUEUE,
            ImageUploadEventType.RUN,
            ImageUploadEventType.COMPLETE,
        ]
