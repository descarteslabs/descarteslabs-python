import pytest
import unittest
import mock
import io

from descarteslabs.exceptions import NotFoundError, BadRequestError
from .. import geocontext
from .. import _download


class TestFormat(unittest.TestCase):
    def test__get_format(self):
        assert _download._get_format("png") == "PNG"
        with pytest.raises(ValueError):
            _download._get_format("foo")

    def test__format_from_path(self):
        assert _download._format_from_path("foo/bar.tif") == "GTiff"
        assert _download._format_from_path("foo/bar.baz.jpg") == "JPEG"
        assert _download._format_from_path("spam.png") == "PNG"
        with pytest.raises(ValueError):
            _download._format_from_path("foo")


@mock.patch.object(_download, "open", new_callable=mock.mock_open)
@mock.patch.object(_download.os, "makedirs", new_callable=mock.MagicMock)
# wrap return value in lambda so individual tests can safely mutate it
@mock.patch.object(
    _download.Raster,
    "raster",
    new_callable=lambda: mock.MagicMock(
        side_effect=lambda *args, **kwargs: {
            "files": {"foo:bar_nir-yellow.tiff": b"i'm a geotiff!"}
        },
    ),
)
class TestDownload(unittest.TestCase):
    id = "foo:bar"
    bands = ["nir", "yellow"]
    ctx = geocontext.AOI(bounds=[30, 40, 50, 60], resolution=2, crs="EPSG:4326")

    def download(self, dest, format="tif"):
        return _download._download(
            inputs=[self.id],
            bands_list=self.bands,
            ctx=self.ctx,
            dtype="UInt16",
            dest=dest,
            format=format,
        )

    def download_mosaic(self, dest, format="tif"):
        return _download._download(
            inputs=[self.id, self.id],
            bands_list=self.bands,
            ctx=self.ctx,
            dtype="UInt16",
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
