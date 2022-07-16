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

import base64
import json
import re
import time
import unittest

import blosc
import numpy as np
import pytest
import responses
from descarteslabs.auth import Auth
from descarteslabs.exceptions import ServerError

from ..raster import Raster, as_json_string

a_geometry = {
    "coordinates": (
        (
            (-95.66055514862535, 41.24469400862013),
            (-94.74931826062456, 41.26199387228942),
            (-94.76311013534223, 41.95357639323731),
            (-95.69397431605952, 41.93542085595837),
            (-95.66055514862535, 41.24469400862013),
        ),
    ),
    "type": "Polygon",
}


class RasterTest(unittest.TestCase):
    def setUp(self):
        payload = (
            base64.b64encode(
                json.dumps(
                    {
                        "aud": "ZOBAi4UROl5gKZIpxxlwOEfx8KpqXf2c",
                        "exp": time.time() + 3600,
                    }
                ).encode()
            )
            .decode()
            .strip("=")
        )
        public_token = f"header.{payload}.signature"

        self.url = "http://example.com/raster"
        self.raster = Raster(
            url=self.url, auth=Auth(jwt_token=public_token, token_info_path=None)
        )
        self.match_url = re.compile(self.url)

    def mock_response(self, method, json, status=200, **kwargs):
        responses.add(method, self.match_url, json=json, status=status, **kwargs)

    def create_blosc_response(self, metadata, *arrays):
        narray = len(arrays)
        shape = arrays[0].shape
        array_meta = {
            "shape": (shape[0] * narray, shape[1], shape[2]),
            "dtype": arrays[0].dtype.name,
            "chunks": narray,
        }

        parts = [
            json.dumps(metadata) + "\n",
            json.dumps(array_meta) + "\n",
        ]

        for i, array in enumerate(arrays):
            # special stopping key to emulate failure
            if isinstance(array, str):
                parts.append(array)
                break

            chunk_meta = {"offset": [i * shape[0], 0, 0], "shape": list(array.shape)}

            array_ptr = array.__array_interface__["data"][0]
            blosc_data = blosc.compress_ptr(
                array_ptr, array.size, array.dtype.itemsize
            ).decode("utf-8")

            mask = np.zeros(array.shape[1:]).astype(bool)
            mask_ptr = mask.__array_interface__["data"][0]
            mask_data = blosc.compress_ptr(
                mask_ptr, mask.size, mask.dtype.itemsize
            ).decode("utf-8")

            parts.append(json.dumps(chunk_meta) + "\n")
            parts.append(blosc_data + mask_data)

        return "".join(parts)

    @responses.activate
    def test_ndarray_blosc(self):
        expected_metadata = {"foo": "bar"}
        expected_array = np.zeros((1, 2, 2))
        content = self.create_blosc_response(expected_metadata, expected_array)
        self.mock_response(responses.POST, json=None, body=content, stream=True)
        array, meta = self.raster.ndarray(["fakeid"])
        assert expected_metadata == meta
        np.testing.assert_array_equal(expected_array.transpose((1, 2, 0)), array)

    @responses.activate
    def test_ndarray_multi_blosc(self):
        expected_metadata = {"foo": "bar"}
        expected_array = np.zeros((2, 2, 2))
        content = self.create_blosc_response(
            expected_metadata, expected_array[0:1, :, :], expected_array[1:2, :, :]
        )
        self.mock_response(responses.POST, json=None, body=content, stream=True)
        array, meta = self.raster.ndarray(["fakeid"])
        assert expected_metadata == meta
        np.testing.assert_array_equal(expected_array.transpose((1, 2, 0)), array)

    @responses.activate
    def test_ndarray_multi_blosc_failure(self):
        expected_metadata = {"foo": "bar"}
        expected_array = np.zeros((2, 2, 2))

        content = self.create_blosc_response(
            expected_metadata, expected_array[0:1, :, :], ""
        )
        self.mock_response(responses.POST, json=None, body=content, stream=True)
        with self.assertRaises(ServerError):
            self.raster.ndarray(["fakeid"])

        content = self.create_blosc_response(
            expected_metadata, expected_array[0:1, :, :], "{"
        )
        self.mock_response(responses.POST, json=None, body=content, stream=True)
        with self.assertRaises(ServerError):
            self.raster.ndarray(["fakeid"])

    @responses.activate
    def do_stack(self, **stack_args):
        expected_metadata = {"foo": "bar"}
        expected_array = np.zeros((1, 2, 2))
        content = self.create_blosc_response(expected_metadata, expected_array)
        self.mock_response(responses.POST, json=None, body=content, stream=True)
        stack, meta = self.raster.stack(
            [["fakeid"], ["fakeid2"]], order="gdal", **stack_args
        )

        np.testing.assert_array_equal(expected_array, stack[0, :])
        np.testing.assert_array_equal(expected_array, stack[1, :])
        assert [expected_metadata] * 2 == meta

    def test_stack_threaded_blosc(self):
        self.do_stack(
            resolution=60,
            srs="EPSG:32615",
            bounds=(277280.0, 4569600.0, 354080.0, 4646400.0),
            bands=["red"],
        )

    def test_stack_dltile_blosc(self):
        self.do_stack(dltile="128:16:960.0:15:-2:37", bands=["red"])

    def test_stack_underspecified(self):
        keys = ["landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"]
        place = "north-america_united-states_iowa"
        bounds = (
            -95.69397431605952,
            41.24469400862013,
            -94.74931826062456,
            41.95357639323731,
        )
        resolution = 960
        dimensions = (128, 128)

        with pytest.raises(ValueError):
            self.raster.stack(keys)
        with pytest.raises(ValueError):
            self.raster.stack(keys, resolution=resolution)
        with pytest.raises(ValueError):
            self.raster.stack(keys, dimensions=dimensions)
        with pytest.raises(ValueError):
            self.raster.stack(keys, bounds=bounds)
        with pytest.raises(ValueError):
            self.raster.stack(keys, resolution=resolution, place=place)


class UtilitiesTest(unittest.TestCase):
    def test_as_json_string(self):
        d = {"a": "b"}
        truth = json.dumps(d)

        assert as_json_string(d) == truth
        s = '{"a": "b"}'
        assert as_json_string(s) == truth
        assert as_json_string(None) is None


if __name__ == "__main__":
    unittest.main()
