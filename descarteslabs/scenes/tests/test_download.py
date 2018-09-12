import unittest
import mock
import sys
import six

from descarteslabs.client.exceptions import NotFoundError, BadRequestError
from descarteslabs.scenes import geocontext
from descarteslabs.scenes import _download


class TestFormat(unittest.TestCase):
    def test__get_format(self):
        self.assertEqual(_download._get_format("png"), "PNG")
        with self.assertRaises(ValueError):
            _download._get_format("foo")

    def test__format_from_path(self):
        self.assertEqual(_download._format_from_path("foo/bar.tif"), "GTiff")
        self.assertEqual(_download._format_from_path("foo/bar.baz.jpg"), "JPEG")
        self.assertEqual(_download._format_from_path("spam.png"), "PNG")
        with self.assertRaises(ValueError):
            _download._format_from_path("foo")


@mock.patch("descarteslabs.scenes._download.open", new_callable=mock.mock_open)
@mock.patch("descarteslabs.scenes._download.os.makedirs")
# wrap return value in lambda so individual tests can safely mutate it
@mock.patch("descarteslabs.scenes._download.Raster.raster", side_effect=lambda *args, **kwargs: {
    "files": {
        "foo:bar_nir-yellow.tiff": b"i'm a geotiff!"
    }
})
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
        self.assertEqual(called_format, "JPEG")
        mock_open.assert_called_once_with(dest, "wb")

    def test_different_format_and_ext(self, mock_raster, mock_makedirs, mock_open):
        dest = "foo.tif"
        self.download(dest, format="jpg")
        mock_raster.assert_called_once()
        called_format = mock_raster.call_args[1]["output_format"]
        self.assertEqual(called_format, "GTiff")
        mock_open.assert_called_once_with(dest, "wb")

    def test_to_file(self, mock_raster, mock_makedirs, mock_open):
        file = six.BytesIO()
        result = self.download(file, format="jpg")
        self.assertIsNone(result)
        self.assertEqual(file.getvalue(), b"i'm a geotiff!")
        mock_open.assert_not_called()
        mock_makedirs.assert_not_called()
        mock_raster.assert_called_once()
        called_format = mock_raster.call_args[1]["output_format"]
        self.assertEqual(called_format, "JPEG")

    def test_to_file_invalid_format(self, mock_raster, mock_makedirs, mock_open):
        file = six.BytesIO()
        with self.assertRaises(ValueError):
            self.download(file, format="foo")

    def test_to_path(self, mock_raster, mock_makedirs, mock_open):
        path = "foo/bar.tif"
        result = self.download(path)
        self.assertEqual(result, path)
        mock_open.assert_called_once_with(path, "wb")
        mock_open().write.assert_called_once_with(b"i'm a geotiff!")
        mock_makedirs.assert_called_once_with("foo")

    def test_to_existing_path(self, mock_raster, mock_makedirs, mock_open):
        path = "../bar.tif"
        self.download(path)
        mock_open.assert_called_once_with(path, "wb")
        mock_makedirs.assert_not_called()

    def test_default_filename_single_scene(self, mock_raster, mock_makedirs, mock_open):
        result = self.download(None)
        self.assertEqual(result, "{id}-{bands}.tif".format(id=self.id, bands="-".join(self.bands)))
        result = self.download(None, format="jpg")
        self.assertEqual(result, "{id}-{bands}.jpg".format(id=self.id, bands="-".join(self.bands)))
        with self.assertRaises(ValueError):
            self.download(None, format="baz")

    def test_default_filename_mosaic(self, mock_raster, mock_makedirs, mock_open):
        result = self.download_mosaic(None)
        self.assertEqual(result, "mosaic-{bands}.tif".format(bands="-".join(self.bands)))
        result = self.download_mosaic(None, format="jpg")
        self.assertEqual(result, "mosaic-{bands}.jpg".format(bands="-".join(self.bands)))
        with self.assertRaises(ValueError):
            self.download_mosaic(None, format="baz")

    @unittest.skipIf(sys.version_info[:2] < (3, 6), "PathLike ABC introduced in 3.6")
    def test_to_pathlib(self, mock_raster, mock_makedirs, mock_open):
        import pathlib
        path = pathlib.Path("foo/bar.tif")
        self.download(path)
        mock_open.assert_called_once_with(path, "wb")
        mock_makedirs.assert_called_once_with("foo")
        mock_open().write.assert_called_once_with(b"i'm a geotiff!")

    def test_weird_response(self, mock_raster, mock_makedirs, mock_open):
        mock_raster.side_effect = lambda *args, **kwargs: {
            "files": {
                "file1": "",
                "file2": "",
            }
        }
        with self.assertRaisesRegexp(RuntimeError, "multiple files"):
            self.download("file.tif")

        mock_raster.side_effect = lambda *args, **kwargs: {
            "files": {}
        }
        with self.assertRaisesRegexp(RuntimeError, "missing results"):
            self.download("file.tif")

    def test_raster_not_found(self, mock_raster, mock_makedirs, mock_open):
        mock_raster.side_effect = NotFoundError("there is no foo")
        with self.assertRaisesRegexp(NotFoundError, "does not exist in the Descartes catalog"):
            self.download("file.tif")

    def test_raster_bad_request(self, mock_raster, mock_makedirs, mock_open):
        mock_raster.side_effect = BadRequestError("what is a foo")
        with self.assertRaisesRegexp(BadRequestError, "Error with request"):
            self.download("file.tif")
