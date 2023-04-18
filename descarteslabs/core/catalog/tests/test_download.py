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

import io
import pytest
import unittest
from unittest.mock import MagicMock, mock_open, patch

from descarteslabs.exceptions import NotFoundError, BadRequestError
from ...common.geo import AOI
from .. import helpers


class TestFormat(unittest.TestCase):
    def test__get_format(self):
        assert helpers.get_format("png") == "PNG"
        with pytest.raises(ValueError):
            helpers.get_format("foo")

    def test__format_from_path(self):
        assert helpers.format_from_path("foo/bar.tif") == "GTiff"
        assert helpers.format_from_path("foo/bar.baz.jpg") == "JPEG"
        assert helpers.format_from_path("spam.png") == "PNG"
        with pytest.raises(ValueError):
            helpers.format_from_path("foo")


@patch.object(helpers, "open", new_callable=mock_open)
@patch.object(helpers.os, "makedirs", new_callable=MagicMock)
# wrap return value in lambda so individual tests can safely mutate it
@patch.object(
    helpers.Raster,
    "raster",
    new_callable=lambda: MagicMock(
        side_effect=lambda *args, **kwargs: {
            "files": {"foo:bar_nir-yellow.tiff": b"i'm a geotiff!"}
        },
    ),
)
class TestDownload(unittest.TestCase):
    id = "foo:bar"
    bands = ["nir", "yellow"]
    geocontext = AOI(bounds=[30, 40, 50, 60], resolution=2, crs="EPSG:4326")

    def download(self, dest, format="tif"):
        return helpers.download(
            inputs=[self.id],
            bands_list=self.bands,
            geocontext=self.geocontext,
            data_type="UInt16",
            dest=dest,
            format=format,
        )

    def download_mosaic(self, dest, format="tif"):
        return helpers.download(
            inputs=[self.id, self.id],
            bands_list=self.bands,
            geocontext=self.geocontext,
            data_type="UInt16",
            dest=dest,
            format=format,
        )

    def test_format_from_ext(self, mock_raster, mock_makedirs, mock_open):
        dest = "foo.jpg"
        self.download(dest)
        mock_raster.assert_called_once()
        called_format = mock_raster.call_args[1]["output_format"]
        assert called_format == "JPEG"

    def test_different_format_and_ext(self, mock_raster, mock_makedirs, mock_open):
        dest = "foo.tif"
        self.download(dest, format="jpg")
        mock_raster.assert_called_once()
        called_format = mock_raster.call_args[1]["output_format"]
        assert called_format == "GTiff"

    def test_to_file(self, mock_raster, mock_makedirs, mock_open):
        file = io.BytesIO()
        with pytest.raises(TypeError):
            self.download(file, format="jpg")

    def test_to_file_invalid_format(self, mock_raster, mock_makedirs, mock_open):
        file = io.BytesIO()
        with pytest.raises(ValueError):
            self.download(file, format="foo")

    def test_to_path(self, mock_raster, mock_makedirs, mock_open):
        path = "foo/bar.tif"
        result = self.download(path)
        assert result == path
        mock_makedirs.assert_called_once_with("foo")

    def test_to_existing_path(self, mock_raster, mock_makedirs, mock_open):
        path = "../bar.tif"
        self.download(path)
        mock_makedirs.assert_not_called()

    def test_default_filename_single_scene(self, mock_raster, mock_makedirs, mock_open):
        result = self.download(None)
        assert result == "{id}-{bands}.tif".format(
            id=self.id, bands="-".join(self.bands)
        )
        result = self.download(None, format="jpg")
        assert result == "{id}-{bands}.jpg".format(
            id=self.id, bands="-".join(self.bands)
        )
        with pytest.raises(ValueError):
            self.download(None, format="baz")

    def test_default_filename_mosaic(self, mock_raster, mock_makedirs, mock_open):
        result = self.download_mosaic(None)
        assert result == "mosaic-{bands}.tif".format(bands="-".join(self.bands))
        result = self.download_mosaic(None, format="jpg")
        assert result == "mosaic-{bands}.jpg".format(bands="-".join(self.bands))
        with pytest.raises(ValueError):
            self.download_mosaic(None, format="baz")

    def test_to_pathlib(self, mock_raster, mock_makedirs, mock_open):
        import pathlib

        path = pathlib.Path("foo/bar.tif")
        self.download(path)
        mock_makedirs.assert_called_once_with("foo")

    def test_raster_not_found(self, mock_raster, mock_makedirs, mock_open):
        mock_raster.side_effect = NotFoundError("there is no foo")
        with pytest.raises(
            NotFoundError, match="does not exist in the Descartes catalog"
        ):
            self.download("file.tif")

    def test_raster_bad_request(self, mock_raster, mock_makedirs, mock_open):
        mock_raster.side_effect = BadRequestError("what is a foo")
        with pytest.raises(BadRequestError, match="Error with request"):
            self.download("file.tif")
