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

import datetime
import json
import os.path
import textwrap
import warnings
from tempfile import NamedTemporaryFile

import numpy as np
import pytest
import responses
import shapely.geometry
from unittest.mock import patch
from pytz import utc

from ...common.geo import AOI
from ...common.property_filtering import Properties
from ...common.shapely_support import shapely_to_geojson
from .. import image as image_module
from .. import image_upload as image_upload_module
from ..attributes import (
    AttributeValidationError,
    DocumentState,
    ListAttribute,
    MappingAttribute,
)
from ..image import Image
from ..image_upload import ImageUploadStatus
from ..product import Product
from .base import ClientTestCase
from .mock_data import (
    BANDS_BY_PRODUCT,
    _cached_bands_by_product,
    _image_get,
    _raster_ndarray,
)


class TestImage(ClientTestCase):
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [
                [-95.2989209, 42.7999878],
                [-93.1167728, 42.3858464],
                [-93.7138666, 40.703737],
                [-95.8364984, 41.1150618],
                [-95.2989209, 42.7999878],
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
                        "modified": "2019-06-11T23:31:33.714883Z",
                        "created": "2019-06-11T23:31:33.714883Z",
                        "name": "myimage",
                        "product_id": "p1",
                        "geometry": self.geometry,
                        "c6s_dlsr": [],
                        "x_pixels": 15696,
                        "y_pixels": 15960,
                        "geotrans": [258292.5, 15.0, 0.0, 4743307.5, 0.0, -15.0],
                        "cs_code": "EPSG:32615",
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
        # product bands request
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 1},
                "data": [
                    {
                        "attributes": {
                            "name": "myband",
                            "product_id": "p1",
                            "created": "2019-06-12T20:31:48.542725Z",
                            "resolution": {"value": 30, "unit": "meters"},
                            "type": "spectral",
                        },
                        "type": "band",
                        "id": "p1:myband",
                    }
                ],
                "jsonapi": {"version": "1.0"},
                "links": {
                    "self": "https://example.com/catalog/v2/bands",
                },
            },
        )
        # product derived bands request
        self.mock_response(
            responses.PUT,
            {
                "meta": {"count": 0},
                "data": [],
                "jsonapi": {"version": "1.0"},
                "links": {
                    "self": "https://example.com/catalog/v2/derived_bands",
                },
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

        assert isinstance(i.geocontext, AOI)
        # avoid differences between tuples and lists
        assert shapely.geometry.shape(i.__geo_interface__) == shapely.geometry.shape(
            self.geometry
        )

    def test_set_geometry(self):
        shape = shapely.geometry.shape(self.geometry)
        i = Image(name="myimage", product_id="p1")
        i.geometry = self.geometry
        assert shape == i.geometry

        i.geometry = shape
        assert shape == i.geometry

        with pytest.raises(AttributeValidationError):
            i.geometry = {"type": "Lollipop"}
        with pytest.raises(AttributeValidationError):
            i.geometry = 2

    def test_serialize_geometry(self):
        i = Image(name="myimage", product_id="p1", geometry=self.geometry)
        assert shapely_to_geojson(i.geometry) == i.serialize()["geometry"]

    def test_geocontext(self):
        # test doesn't fail with nothing
        geocontext = Image().geocontext
        assert geocontext == AOI(bounds_crs=None, align_pixels=False)

        # no geotrans
        geocontext = Image(cs_code="EPSG:4326").geocontext
        assert geocontext == AOI(crs="EPSG:4326", bounds_crs=None, align_pixels=False)

        # north-up geotrans - resolution
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter(
                "always"
            )  # otherwise, the duplicate warning is suppressed the second time
            # origin: (0, 0), pixel size: 2, rotation: 0 degrees
            geocontext = Image(
                cs_code="EPSG:4326", geotrans=[0, 2, 0, 0, 0, -2]
            ).geocontext
            assert len(w) == 0
        assert geocontext.resolution == 2

        # non-north-up geotrans - resolution
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # origin: (0, 0), pixel size: 2, rotation: 30 degrees
            geocontext = Image(
                cs_code="EPSG:4326",
                geotrans=(
                    0.0,
                    1.7320508075688774,
                    -1,
                    0.0,
                    1,
                    1.7320508075688774,
                ),
            ).geocontext
            warning = w[0]
            assert "The GeoContext will *not* return this Image's original data" in str(
                warning.message
            )
        assert geocontext.resolution == 2

        # north-up geotrans - bounds
        # origin: (10, 20), pixel size: 2, rotation: 0 degrees
        geocontext = Image(
            cs_code="EPSG:4326",
            geotrans=[10, 2, 0, 20, 0, -2],
            x_pixels=1,
            y_pixels=2,
        ).geocontext
        assert geocontext.bounds == (10, 16, 12, 20)

        # non-north-up geotrans - bounds
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            # origin: (0, 0), pixel size: 2, rotation: 45 degrees
            geocontext = Image(
                cs_code="EPSG:4326",
                geotrans=(
                    0.0,
                    np.sqrt(2),
                    np.sqrt(2),
                    0.0,
                    np.sqrt(2),
                    -np.sqrt(2),
                ),
                x_pixels=1,
                y_pixels=1,
            ).geocontext
            warning = w[0]
            assert "The GeoContext will *not* return this Image's original data" in str(
                warning.message
            )
        diagonal = np.sqrt(2**2 + 2**2)
        assert geocontext.bounds == (0, -diagonal / 2, diagonal, diagonal / 2)

    @responses.activate
    def test_nanosecond_timestamp(self):
        self.mock_response(
            responses.GET,
            {
                "data": {
                    "attributes": {
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
        search = (
            Image.search()
            .intersects(self.geometry)
            .filter(Properties().product_id == "p1")
        )
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

    @patch.object(image_module.Image, "_upload_service")
    @patch.object(image_upload_module.ImageUpload, "_POLLING_INTERVALS", [1])
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

    @patch.object(image_module.Image, "_upload_service")
    @patch.object(image_upload_module.ImageUpload, "_POLLING_INTERVALS", [1])
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
    @patch("descarteslabs.catalog.Image.exists", return_value=False)
    def test_upload_warnings(self, *mocks):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        image = Image(id="p1:image", product=p, acquired="2012-05-06", projection="foo")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            image.upload("somefile")
            assert 1 == len(w)
            assert "cs_code" in str(w[0].message)

    @patch.object(image_module.Image, "_upload_service")
    @patch.object(image_upload_module.ImageUpload, "_POLLING_INTERVALS", [1])
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
    @patch("descarteslabs.catalog.Image.exists", return_value=False)
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

    @patch("descarteslabs.catalog.Image.exists", return_value=False)
    def test_upload_ndarray_bad_georef(self, *mocks):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        image = Image(
            id="p1:image",
            product=p,
            acquired="2012-05-06",
            geotrans=[42, 0, 0, 0, 0, 0],
        )
        pytest.raises(ValueError, image.upload_ndarray, np.zeros((100, 100)))

    @patch("descarteslabs.catalog.Image._do_upload", return_value=True)
    @patch("descarteslabs.catalog.Image.exists", return_value=False)
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
    @patch("descarteslabs.catalog.Image.exists", return_value=False)
    @patch("numpy.save")
    def test_upload_ndarray_moves_band_axis(self, mock_np_save, *mocks):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        image = Image(
            id="p1:image",
            product=p,
            acquired="2012-05-06",
            geotrans=[42, 0, 0, 0, 0, 0],
            projection="foo",
        )

        array = np.zeros((1, 100, 100))
        with warnings.catch_warnings(record=True) as w:
            image.upload_ndarray(array)
            assert 0 == len(w)

        assert mock_np_save.called

        _, ndarray = mock_np_save.call_args[0]
        assert ndarray.shape == (100, 100, 1)

    @patch("descarteslabs.catalog.Image._do_upload", return_value=True)
    @patch("descarteslabs.catalog.Image.exists", return_value=False)
    @patch("numpy.save")
    def test_upload_ndarray_multiple(self, mock_np_save, *mocks):
        p = Product(id="p1", name="Test Product", client=self.client, _saved=True)
        image = Image(
            id="p1:image",
            product=p,
            acquired="2012-05-06",
            geotrans=[42, 0, 0, 0, 0, 0],
            projection="foo",
        )

        # try a non iterable
        class MyClass:
            pass

        with self.assertRaisesRegex(ValueError, "instance of ndarray or an Iterable"):
            image.upload_ndarray(MyClass())

        # try an interable custom class
        class MyArray:
            def __init__(self, *args):
                self.data = list(args)

            def __iter__(self):
                return (x for x in self.data)

            def __setitem__(self, key, value):
                self.data[key] = value

        array = np.zeros((1, 100, 100))
        array2 = np.zeros((1, 50, 50))
        image.upload_ndarray(MyArray(array, array2))

        # try iterable but not ndarrays
        with self.assertRaisesRegex(ValueError, "is not an ndarray"):
            image.upload_ndarray(["something", "something else"])

        # try a native list with ndarrays
        array = np.zeros((1, 75, 75))
        array2 = np.zeros((1, 25, 25))
        image.upload_ndarray([array, array2])

        assert mock_np_save.call_count == 4

        shapes = [
            ndarray.shape
            for (_tmp_file, ndarray), _kwargs in mock_np_save.call_args_list
        ]

        assert shapes == [
            (100, 100, 1),
            (50, 50, 1),
            (75, 75, 1),
            (25, 25, 1),
        ]

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_load_one_band(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr, info = image.ndarray("red", resolution=1000, raster_info=True)

        assert arr.shape == (1, 239, 235)
        assert arr.mask[0, 2, 2]
        assert not arr.mask[0, 115, 116]
        assert len(info["geoTransform"]) == 6

        with pytest.raises(TypeError):
            image.ndarray("blue", invalid_argument=True)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    def test_nonexistent_band_fails(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        with pytest.raises(ValueError):
            image.ndarray("blue yellow")

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_different_band_dtypes(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        with patch.object(
            BANDS_BY_PRODUCT["landsat:LC08:PRE:TOAR"]["green"], "data_type", "Int16"
        ):
            arr, info = image.ndarray("red green", resolution=600, mask_alpha=False)
            assert arr.dtype.type == np.int32

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_load_multiband(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = image.ndarray("red green blue", resolution=1000)

        assert arr.shape == (3, 239, 235)
        assert (arr.mask[:, 2, 2]).all()
        assert not (arr.mask[:, 115, 116]).all()

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_load_multiband_axis_last(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = image.ndarray("red green blue", resolution=1000, bands_axis=-1)

        assert arr.shape == (239, 235, 3)
        assert (arr.mask[2, 2, :]).all()
        assert not (arr.mask[115, 116, :]).all()

        with pytest.raises(ValueError):
            arr = image.ndarray("red green blue", resolution=1000, bands_axis=3)
        with pytest.raises(ValueError):
            arr = image.ndarray("red green blue", resolution=1000, bands_axis=-3)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_load_nomask(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = image.ndarray(
            ["red", "nir"],
            resolution=1000,
            mask_nodata=False,
            mask_alpha=False,
        )

        assert not hasattr(arr, "mask")
        assert arr.shape == (2, 239, 235)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_auto_mask_alpha_false(self):
        image = Image.get(
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"
        )
        arr = image.ndarray(
            ["Clear_sky_days", "Clear_sky_nights"],
            resolution=1000,
            mask_nodata=False,
        )

        assert not hasattr(arr, "mask")
        assert arr.shape == (2, 688, 473)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_mask_alpha_string(self):
        image = Image.get(
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"
        )
        arr = image.ndarray(
            ["Clear_sky_days", "Clear_sky_nights"],
            resolution=1000,
            mask_alpha="Clear_sky_nights",
            mask_nodata=False,
        )

        assert hasattr(arr, "mask")
        assert arr.shape == (2, 688, 473)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_mask_missing_alpha(self):
        image = Image.get(
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"
        )
        with pytest.raises(ValueError):
            image.ndarray(
                ["Clear_sky_days", "Clear_sky_nights"],
                resolution=1000,
                mask_alpha=True,
                mask_nodata=False,
            )

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_mask_missing_band(self):
        image = Image.get(
            "modis:mod11a2:006:meta_MOD11A2.A2017305.h09v05.006.2017314042814_v1"
        )
        with pytest.raises(ValueError):
            image.ndarray(
                ["Clear_sky_days", "Clear_sky_nights"],
                resolution=1000,
                mask_alpha="missing_band",
                mask_nodata=False,
            )

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def test_auto_mask_alpha_true(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        arr = image.ndarray(
            ["red", "green", "blue"], resolution=1000, mask_nodata=False
        )

        assert hasattr(arr, "mask")
        assert arr.shape == (3, 239, 235)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    def with_alpha(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")

        arr = image.ndarray(["red", "alpha"], resolution=1000)
        assert arr.shape == (2, 239, 235)
        assert (arr.mask == (arr.data[1] == 0)).all()

        arr = image.ndarray(["alpha"], resolution=1000, mask_nodata=False)
        assert arr.shape == (1, 239, 235)
        assert (arr.mask == (arr.data == 0)).all()

        with pytest.raises(ValueError):
            arr = image.ndarray("alpha red", resolution=1000)

    def test_coverage(self):
        geometry = shapely.geometry.Point(0.0, 0.0).buffer(1)

        image = Image(id="foo:bar", geometry=geometry)

        # same geometry (as a GeoJSON)
        assert image.coverage(geometry.__geo_interface__) == pytest.approx(
            1.0, abs=1e-6
        )

        # geom is larger
        geom_larger = shapely.geometry.Point(0.0, 0.0).buffer(2)
        assert image.coverage(geom_larger) == pytest.approx(0.25, abs=1e-6)

        # geom is smaller
        geom_smaller = shapely.geometry.Point(0.0, 0.0).buffer(0.5)
        assert image.coverage(geom_smaller) == pytest.approx(1.0, abs=1e-6)

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    @patch.object(image_module.Raster, "ndarray", _raster_ndarray)
    @patch.object(image_module, "download")
    def test_download(self, mock_download):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        image.download("red green blue", resolution=120.0)
        mock_download.assert_called_once()

    @patch.object(Image, "get", _image_get)
    @patch.object(
        image_module,
        "cached_bands_by_product",
        _cached_bands_by_product,
    )
    def test_scaling_parameters_display(self):
        image = Image.get("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        scales, data_type = image.scaling_parameters(
            "red green blue alpha", scaling="display"
        )
        assert scales == [(0, 4000, 0, 255), (0, 4000, 0, 255), (0, 4000, 0, 255), None]
        assert data_type == "Byte"
