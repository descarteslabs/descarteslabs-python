import pytest
import json
from mock import patch
import responses
import textwrap
import datetime
from tempfile import NamedTemporaryFile
import os.path
import warnings

import shapely.geometry

from pytz import utc

import numpy as np

from descarteslabs.common.shapely_support import shapely_to_geojson

from .base import ClientTestCase
from ..attributes import (
    AttributeValidationError,
    ListAttribute,
    MappingAttribute,
    DocumentState,
)
from ..image import Image
from ..image_upload import ImageUploadStatus
from ..product import Product


class TestImage(ClientTestCase):
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-9.000262842437783, 46.9537091787344],
                [-8.325270159894608, 46.95172107428039],
                [-8.336543403548475, 46.925857032669434],
                [-8.39987774007129, 46.7807657614384],
                [-8.463235968271405, 46.63558741606639],
                [-8.75144712554016, 45.96528086358922],
                [-9.0002581299532, 45.9655511480415],
                [-9.000262842437783, 46.9537091787344],
            ]
        ],
    }

    @responses.activate
    def test_get(self):
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
                        "name": "myimage",
                        "product_id": "p1",
                        "geometry": self.geometry,
                        "c6s_dlsr": [],
                    },
                    "type": "image",
                    "id": "p1:myimage",
                },
                "included": [
                    {"attributes": {"name": "A product"}, "type": "product", "id": "p1"}
                ],
                "jsonapi": {"version": "1.0"},
            },
        )

        i = Image.get("p1:myimage", client=self.client)
        assert "p1" == i.product.id
        assert shapely.geometry.shape(self.geometry) == i.geometry

        i_repr = repr(i)
        match_str = """\
            Image: myimage
              id: p1:myimage
              product: p1
              created: Tue Jun 11 23:31:33 2019"""  # noqa
        assert i_repr.strip("\n") == textwrap.dedent(match_str)

    def test_set_geometry(self):
        shape = shapely.geometry.shape(self.geometry)
        i = Image(name="myimage", product_id="p1")
        i.geometry = self.geometry
        assert shape == i.geometry

        i.geometry = shape
        assert shape == i.geometry

        with pytest.raises(AttributeValidationError):
            i.geometry = {"type": "Polygon"}
        with pytest.raises(AttributeValidationError):
            i.geometry = 2

    def test_serialize_geometry(self):
        i = Image(name="myimage", product_id="p1", geometry=self.geometry)
        assert shapely_to_geojson(i.geometry) == i.serialize()["geometry"]

    @responses.activate
    def test_nanosecond_timestamp(self):
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
                        "name": "myimage",
                        "product_id": "p1",
                        "geometry": self.geometry,
                        "acquired": "2019-08-20T08:08:16.123456789Z",
                    },
                    "type": "image",
                    "id": "p1:myimage",
                },
                "included": [
                    {"attributes": {"name": "A product"}, "type": "product", "id": "p1"}
                ],
                "jsonapi": {"version": "1.0"},
            },
        )

        i = Image.get("p1:myimage", client=self.client)
        assert (
            datetime.datetime.strptime(
                "2019-08-20T08:08:16.123456Z", "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=utc)
            == i.acquired
        )

    def test_search_intersects(self):
        search = Image.search().intersects(self.geometry)
        _, request_params = search._to_request()
        assert self.geometry == json.loads(request_params["intersects"])

    @responses.activate
    def test_files_attribute(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "type": "image",
                    "relationships": {
                        "product": {"data": {"type": "product", "id": "prod1"}}
                    },
                    "attributes": {
                        "readers": ["group:public", "group:beta"],
                        "proj4": "+proj=utm +zone=29 +datum=WGS84 +units=m +no_defs ",
                        "satellite_id": "LANDSAT_8",
                        "product_id": "prod1",
                        "files": [
                            {
                                "href": "gs://descartes-files/01.jp2",
                                "size_bytes": 121091191,
                                "hash": "c1fe6f604b0bf1e7265d06ed0379bf0c",
                                "provider_href": None,
                                "provider_id": None,
                            },
                            {
                                "href": "gs://descartes-files/02.jp2",
                                "size_bytes": 53718630,
                                "hash": "37ea2fb38268875f7f473c52ba485bc6",
                                "provider_href": None,
                                "provider_id": None,
                            },
                        ],
                        "name": "id1",
                        "storage_state": "available",
                        "extra_properties": {},
                        "geometry": self.geometry,
                        "geotrans": [330592.5, 15.0, 0.0, 8918707.5, 0.0, -15.0],
                        "x_pixels": 18240,
                        "owners": ["org:descarteslabs"],
                        "published": "2017-05-16T18:45:16Z",
                        "acquired": "2017-05-16T14:07:26.108914Z",
                        "created": "2017-05-21T00:51:08Z",
                        "y_pixels": 18216,
                    },
                    "id": "prod1:id1",
                },
                "included": [
                    {
                        "type": "product",
                        "attributes": {
                            "readers": ["group:beta", "group:public"],
                            "owners": ["org:descarteslabs"],
                            "start_datetime": "2013-01-01T00:00:00Z",
                        },
                        "id": "prod1",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
        )

        i = Image.get("p1:myimage", client=self.client)
        assert len(i.files) == 2
        assert isinstance(i.files, ListAttribute)
        assert isinstance(i.files[0], MappingAttribute)

        file0 = i.files[0]
        assert file0.href == "gs://descartes-files/01.jp2"
        assert i.state == DocumentState.SAVED

        file0.href = "gs://new-bucket/file0.jp2"
        assert i.state == DocumentState.MODIFIED

        serialized = i.serialize(modified_only=True)
        assert list(serialized.keys()) == ["files"]
        assert serialized["files"] == [
            {
                "href": "gs://new-bucket/file0.jp2",
                "size_bytes": 121091191,
                "hash": "c1fe6f604b0bf1e7265d06ed0379bf0c",
                "provider_href": None,
                "provider_id": None,
            },
            {
                "href": "gs://descartes-files/02.jp2",
                "size_bytes": 53718630,
                "hash": "37ea2fb38268875f7f473c52ba485bc6",
                "provider_href": None,
                "provider_id": None,
            },
        ]

    @patch("descarteslabs.catalog.image.Image._gcs_upload_service")
    @patch("descarteslabs.catalog.image_upload.ImageUpload._POLLING_INTERVALS", [1])
    @responses.activate
    def test_upload(self, upload_mock):
        with NamedTemporaryFile(suffix=".tif", delete=False) as f:
            try:
                f.close()
                # this is copied from the upload impl, should go away with new ingest
                product_id = "p1"
                image_name = "image"
                image_id = "{}:{}".format(product_id, image_name)
                upload_url = "https:www.fake.com/1"

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
                    responses.HEAD,
                    {
                        "errors": [
                            {
                                "detail": "Object not found: {}".format(image_id),
                                "status": "404",
                                "title": "Object not found",
                            }
                        ],
                        "jsonapi": {"version": "1.0"},
                    },
                    status=404,
                )
                self.mock_response(
                    responses.POST,
                    {
                        "data": {
                            "type": "image_upload",
                            "id": "1",
                            "attributes": {
                                "created": "2019-01-02T03:04:05Z",
                                "modified": "2019-01-02T03:04:05Z",
                                "product_id": product_id,
                                "image_id": image_id,
                                "resumable_urls": [upload_url],
                                "status": ImageUploadStatus.TRANSFERRING.value,
                            },
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
                                "created": "2019-01-02T03:04:05Z",
                                "modified": "2019-01-02T03:04:06Z",
                                "product_id": product_id,
                                "image_id": image_id,
                                "status": ImageUploadStatus.PENDING.value,
                            },
                        },
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
                                "created": "2019-01-02T03:04:05Z",
                                "modified": "2019-01-02T03:04:06Z",
                                "product_id": product_id,
                                "image_id": image_id,
                                "status": ImageUploadStatus.SUCCESS.value,
                            },
                        },
                        "jsonapi": {"version": "1.0"},
                    },
                )
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
                                "name": image_name,
                                "product_id": product_id,
                                "acquired": "2001-01-01T00:00:00Z",
                                "geometry": self.geometry,
                            },
                            "type": "image",
                            "id": image_id,
                        },
                        "jsonapi": {"version": "1.0"},
                    },
                )

                image = Image(
                    name=image_name,
                    product_id=product_id,
                    acquired="2001-01-01",
                    client=self.client,
                )

                upload = image.upload(f.name)
            finally:
                # Manual cleanup required for Windows compatibility
                os.unlink(f.name)

        assert image.state == DocumentState.UNSAVED
        assert upload.id == "1"
        assert upload.product_id == product_id
        assert upload.image_id == image.id
        assert upload.status == ImageUploadStatus.PENDING
        upload_mock.session.put.assert_called_once()
        assert upload_mock.session.put.call_args[0][0] == upload_url

        upload.wait_for_completion(15)

        assert upload.status == ImageUploadStatus.SUCCESS

        # when the new ingest is completed, we may implement the reload
        # of the updated Image...

    @patch("descarteslabs.catalog.image.Image._gcs_upload_service")
    @patch("descarteslabs.catalog.image_upload.ImageUpload._POLLING_INTERVALS", [1])
    @responses.activate
    def test_upload_multi_file(self, upload_mock):
        with NamedTemporaryFile(suffix=".tif", delete=False) as f1:
            with NamedTemporaryFile(suffix=".tif", delete=False) as f2:
                try:
                    f1.close()
                    f2.close()
                    # this is copied from the upload impl, should go away with new ingest
                    product_id = "p1"
                    image_name = "image"
                    image_id = "{}:{}".format(product_id, image_name)
                    upload_url1 = "https:www.fake.com/1"
                    upload_url2 = "https:www.fake.com/2"

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
                        responses.HEAD,
                        {
                            "errors": [
                                {
                                    "detail": "Object not found: {}".format(image_id),
                                    "status": "404",
                                    "title": "Object not found",
                                }
                            ],
                            "jsonapi": {"version": "1.0"},
                        },
                        status=404,
                    )
                    self.mock_response(
                        responses.POST,
                        {
                            "data": {
                                "type": "image_upload",
                                "id": "1",
                                "attributes": {
                                    "created": "2019-01-02T03:04:05Z",
                                    "modified": "2019-01-02T03:04:05Z",
                                    "product_id": product_id,
                                    "image_id": image_id,
                                    "resumable_urls": [upload_url1, upload_url2],
                                    "status": ImageUploadStatus.TRANSFERRING.value,
                                },
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
                                    "created": "2019-01-02T03:04:05Z",
                                    "modified": "2019-01-02T03:04:05Z",
                                    "product_id": product_id,
                                    "image_id": image_id,
                                    "status": ImageUploadStatus.PENDING.value,
                                },
                            },
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
                                    "created": "2019-01-02T03:04:05Z",
                                    "modified": "2019-01-02T03:04:05Z",
                                    "product_id": product_id,
                                    "image_id": image_id,
                                    "status": ImageUploadStatus.SUCCESS.value,
                                },
                            },
                            "jsonapi": {"version": "1.0"},
                        },
                    )
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
                                    "name": image_name,
                                    "product_id": product_id,
                                    "acquired": "2001-01-01T00:00:00Z",
                                    "geometry": self.geometry,
                                },
                                "type": "image",
                                "id": image_id,
                            },
                            "jsonapi": {"version": "1.0"},
                        },
                    )

                    image = Image(
                        name=image_name,
                        product_id=product_id,
                        acquired="2001-01-01",
                        client=self.client,
                    )

                    upload = image.upload([f1.name, f2.name])
                finally:
                    # Manual cleanup required for Windows compatibility
                    os.unlink(f1.name)
                    os.unlink(f2.name)

        assert image.state == DocumentState.UNSAVED
        assert upload.id == "1"
        assert upload.product_id == product_id
        assert upload.image_id == image.id
        assert upload.status == ImageUploadStatus.PENDING
        assert len(upload_mock.session.put.call_args_list) == 2
        assert upload_mock.session.put.call_args_list[0][0][0] == upload_url1
        assert upload_mock.session.put.call_args_list[1][0][0] == upload_url2

        upload.wait_for_completion(15)

        assert upload.status == ImageUploadStatus.SUCCESS

        # when the new ingest is completed, we may implement the reload
        # of the updated Image...

    @patch("descarteslabs.catalog.Image._do_upload", return_value=True)
    def test_upload_warnings(self, *mocks):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        image = Image(id="p1:image", product=p, acquired="2012-05-06", projection="foo")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            image.upload("somefile")
            assert 1 == len(w)
            assert "cs_code" in str(w[0].message)

    @patch("descarteslabs.catalog.image.Image._gcs_upload_service")
    @patch("descarteslabs.catalog.image_upload.ImageUpload._POLLING_INTERVALS", [1])
    @responses.activate
    def test_upload_ndarray(self, upload_mock):
        # this is copied from the upload impl, should go away with new ingest
        product_id = "p1"
        image_name = "image"
        image_id = "{}:{}".format(product_id, image_name)
        upload_url = "https:www.fake.com/1"

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
            responses.HEAD,
            {
                "errors": [
                    {
                        "detail": "Object not found: {}".format(image_id),
                        "status": "404",
                        "title": "Object not found",
                    }
                ],
                "jsonapi": {"version": "1.0"},
            },
            status=404,
        )
        self.mock_response(
            responses.POST,
            {
                "data": {
                    "type": "image_upload",
                    "id": "1",
                    "attributes": {
                        "created": "2019-01-02T03:04:05Z",
                        "modified": "2019-01-02T03:04:05Z",
                        "product_id": product_id,
                        "image_id": image_id,
                        "resumable_urls": [upload_url],
                        "status": ImageUploadStatus.TRANSFERRING.value,
                    },
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
                        "created": "2019-01-02T03:04:05Z",
                        "modified": "2019-01-02T03:04:05Z",
                        "product_id": product_id,
                        "image_id": image_id,
                        "status": ImageUploadStatus.PENDING.value,
                    },
                },
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
                        "product_id": product_id,
                        "image_id": image_id,
                        "status": ImageUploadStatus.SUCCESS.value,
                    },
                },
                "jsonapi": {"version": "1.0"},
            },
        )
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
                        "name": image_name,
                        "product_id": product_id,
                        "acquired": "2001-01-01T00:00:00Z",
                        "geometry": self.geometry,
                    },
                    "type": "image",
                    "id": image_id,
                },
                "jsonapi": {"version": "1.0"},
            },
        )

        image = Image(
            name=image_name,
            product_id=product_id,
            acquired="2001-01-01",
            client=self.client,
        )

        ary = np.zeros((10, 10), dtype=np.dtype(np.uint16))
        raster_meta = {
            "geoTransform": [0, 0, 0, 0, 0, 0],
            "coordinateSystem": {"proj4": "proj4 string"},
        }
        upload = image.upload_ndarray(ary, raster_meta=raster_meta)

        assert image.state == DocumentState.UNSAVED
        assert upload.id == "1"
        assert upload.product_id == product_id
        assert upload.image_id == image.id
        assert upload.status == ImageUploadStatus.PENDING
        upload_mock.session.put.assert_called_once()
        assert upload_mock.session.put.call_args[0][0] == upload_url
        assert (
            self.get_request_body(2)["data"]["attributes"]["image_upload_options"][
                "upload_size"
            ]
            == 200
        )

        upload.wait_for_completion(15)

        assert upload.status == ImageUploadStatus.SUCCESS

    @patch("descarteslabs.catalog.Image._do_upload", return_value=True)
    def test_upload_ndarray_shape(self, *mocks):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        image = Image(
            id="p1:image",
            product=p,
            acquired="2012-05-06",
            geotrans=[42, 0, 0, 0, 0, 0],
            projection="foo",
        )

        pytest.raises(ValueError, image.upload_ndarray, np.zeros((100,)))
        pytest.raises(ValueError, image.upload_ndarray, np.zeros((100, 100, 1, 2)))

        array = np.zeros((1, 100, 100))
        with warnings.catch_warnings(record=True) as w:
            image.upload_ndarray(array)
            assert 0 == len(w)

        array = np.zeros((100, 100))
        with warnings.catch_warnings(record=True) as w:
            image.upload_ndarray(array)
            assert 0 == len(w)

        array = np.zeros((100, 100, 1))
        with warnings.catch_warnings(record=True) as w:
            image.upload_ndarray(array)
            assert 1 == len(w)

        array = np.zeros((1, 100, 100))
        image.cs_code = "FOO:1"
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            image.upload_ndarray(array)
            assert 1 == len(w)
            assert "cs_code" in str(w[0].message)

    @patch("descarteslabs.catalog.Image._do_upload", return_value=True)
    def test_upload_ndarray_dtype(self, *mocks):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        image = Image(
            id="p1:image",
            product=p,
            acquired="2012-05-06",
            geotrans=[42, 0, 0, 0, 0, 0],
            projection="foo",
        )

        pytest.raises(
            ValueError, image.upload_ndarray, np.zeros((1, 100, 100), np.int8)
        )
        pytest.raises(
            ValueError, image.upload_ndarray, np.zeros((1, 100, 100), np.int64)
        )
        pytest.raises(
            ValueError, image.upload_ndarray, np.zeros((1, 100, 100), np.uint64)
        )

    def test_upload_ndarray_bad_georef(self):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        image = Image(
            id="p1:image",
            product=p,
            acquired="2012-05-06",
            geotrans=[42, 0, 0, 0, 0, 0],
        )
        pytest.raises(ValueError, image.upload_ndarray, np.zeros((100, 100)))
