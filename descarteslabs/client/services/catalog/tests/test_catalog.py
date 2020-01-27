# Copyright 2018-2020 Descartes Labs.
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

import numpy as np
import pytest
import unittest
import os
import sys
from mock import patch
from tempfile import NamedTemporaryFile
import responses
import json
import re

from descarteslabs.client.auth import Auth
from descarteslabs.client.services.catalog import Catalog


@patch.object(Auth, "token", "token")
@patch.object(Auth, "namespace", "foo")
class TestCatalog(unittest.TestCase):
    instance = None

    def setUp(self):
        self.url = "http://www.example.com/metadata/v1/catalog"
        self.instance = Catalog(url=self.url)
        self.match_url = re.compile(self.url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)

    @staticmethod
    def validate_ndarray_callback(request):
        np.load(request.body)
        return (200, {}, "")

    @patch(
        "descarteslabs.client.services.catalog.Catalog._do_upload",
        return_value=(False, "upload_id", None),
    )
    def test_upload_ndarray_dtype(self, _do_upload):
        unsupported_dtypes = ["uint64"]

        for dtype in unsupported_dtypes:
            with pytest.raises(TypeError):
                self.instance.upload_ndarray(
                    np.zeros((10, 10), dtype=dtype), "product", "key"
                )

        for dtype in Catalog.UPLOAD_NDARRAY_SUPPORTED_DTYPES:
            self.instance.upload_ndarray(
                np.zeros((10, 10), dtype=dtype), "product", "key"
            )

    def test_upload_invalid_id(self):
        with pytest.raises(TypeError):
            self.instance.upload_ndarray(
                np.zeros((10, 10)),
                # invalid product id
                {"foo": "bar"},
                "key",
            )

    @unittest.skipIf(sys.version_info.major == 3, "Test only makes sense in py2")
    def test_upload_image_deprecated_file_type(self):
        # in py2 NamedTemporaryFile produces a file object not a IOBase object
        with NamedTemporaryFile() as tmp:
            with pytest.raises(Exception):
                self.instance.upload_image(tmp, "product")

    def test_upload_image_bad_path(self):
        name = None
        with NamedTemporaryFile() as tmp:
            name = tmp.file
        with pytest.raises(Exception):
            self.instance.upload_image(name, "product")

    def test_upload_image_multi_file_no_list(self):
        with NamedTemporaryFile() as tmp:
            with pytest.raises(ValueError):
                self.instance.upload_image(tmp.name, "product", multi=True)

    def test_upload_image_multi_file_no_image_id(self):
        with NamedTemporaryFile() as tmp:
            with pytest.raises(ValueError):
                self.instance.upload_image([tmp.name, tmp.name], "product", multi=True)

    @responses.activate
    def test_upload_image(self):
        product = "foo:product_id"
        gcs_upload_url = "https://gcs_upload_url.com"
        upload_url = (
            "http://www.example.com/metadata/v1/catalog/products/{}/images/upload/{}"
        )
        with NamedTemporaryFile(delete=False) as tmp:
            try:
                tmp.write(b"foo")
                tmp.close()
                responses.add(
                    responses.POST,
                    upload_url.format(product, os.path.basename(tmp.name)),
                    body=gcs_upload_url,
                )
                responses.add(responses.PUT, gcs_upload_url)
                self.instance.upload_image(tmp.name, product)
            finally:
                # Manual cleanup required for Windows compatibility
                os.unlink(tmp.name)

    @responses.activate
    def test_upload_ndarray(self):
        product = "foo:product_id"
        gcs_upload_url = "https://gcs_upload_url.com"
        upload_url = (
            "http://www.example.com/metadata/v1/catalog/products/{}/images/upload/key"
        )
        responses.add(responses.POST, upload_url.format(product), body=gcs_upload_url)
        responses.add_callback(
            responses.PUT, gcs_upload_url, callback=self.validate_ndarray_callback
        )
        self.instance.upload_ndarray(np.zeros((10, 10)), product, "key")

    # tests verifying storage state kwarg is applied correctly
    @responses.activate
    def test_add_image_default(self):
        self.mock_response(responses.POST, json={})
        self.instance.add_image("product", "fake_image_id")
        request = responses.calls[0].request
        assert json.loads(request.body.decode("utf-8"))["storage_state"] == "available"


if __name__ == "__main__":
    unittest.main()
