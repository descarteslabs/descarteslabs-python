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

import json
import os
import random
import struct
import time
from concurrent import futures

import blosc
import numpy as np
from descarteslabs.auth import Auth
from descarteslabs.config import get_settings
from descarteslabs.exceptions import ServerError
from PIL import Image
from tqdm import tqdm
from urllib3.exceptions import IncompleteRead, ProtocolError

from ....common.dltile import Tile
from ...deprecation import deprecate
from ..service.service import Service
from .geotiff_utils import make_geotiff

DEFAULT_MAX_WORKERS = 8


def as_json_string(str_or_dict):
    if not str_or_dict:
        return str_or_dict
    elif isinstance(str_or_dict, dict):
        return json.dumps(str_or_dict)
    else:
        return str_or_dict


def read_blosc_buffer(data):
    header = data.read(16)
    if len(header) != 16:
        raise ServerError(
            f"Received incomplete header (got {len(header)} bytes, expected 16)"
        )

    _, size, _, compressed_size = struct.unpack("<IIII", header)
    body = data.read(compressed_size - 16)

    return size, header + body


def read_tiled_blosc_array(metadata, data, progress=None):
    output = np.ma.zeros(metadata["shape"], dtype=np.dtype(metadata["dtype"]))
    output.mask = True

    progbar = (
        tqdm(
            desc="Rasterizing",
            total=output.nbytes,
            unit_scale=True,
            unit_divisor=1024,
            unit="B",
            disable=False if progress is True else None,
        )
        if progress is not False
        else None
    )
    for _ in range(metadata["chunks"]):
        chunk_metadata_bytes = data.readline()
        # the service instance may have gotten killed such that we
        # see no transport error, but we do not get the complete
        # chunk metadata. Handle all the variants as a retryable error.
        # Note that although there are different paths to an exception,
        # they all represent the same problem: we did not receive a
        # complete and valid chunk metadata JSON dict.
        errmsg = "Did not receive complete chunk metadata"
        try:
            chunk_metadata_str = chunk_metadata_bytes.decode("utf-8")
        except UnicodeDecodeError:
            # incomplete bytes don't decode
            raise ServerError(errmsg)
        try:
            chunk_metadata = json.loads(chunk_metadata_str.strip())
        except json.JSONDecodeError:
            # incomplete JSON string doesn't decode (including empty string)
            raise ServerError(errmsg)

        if "error" in chunk_metadata:
            # The server encountered an error
            raise ServerError(chunk_metadata["error"])

        chunk = np.empty(chunk_metadata["shape"], dtype=output.dtype)
        raw_size, buffer = read_blosc_buffer(data)

        if raw_size != chunk.nbytes:
            raise ServerError(
                "Did not receive complete chunk (got {}, expected {})".format(
                    raw_size, chunk.nbytes
                )
            )

        blosc.decompress_ptr(buffer, chunk.__array_interface__["data"][0])

        start_band, y_off, x_off = chunk_metadata["offset"]
        output.data[
            start_band : start_band + chunk.shape[0],
            y_off : y_off + chunk.shape[1],
            x_off : x_off + chunk.shape[2],
        ] = chunk

        mask_chunk = np.empty(chunk_metadata["shape"], dtype=output.mask.dtype)
        raw_size, buffer = read_blosc_buffer(data)

        if raw_size != mask_chunk.nbytes:
            raise ServerError(
                "Did not receive complete chunk (got {}, expected {})".format(
                    raw_size, mask_chunk.nbytes
                )
            )

        blosc.decompress_ptr(buffer, mask_chunk.__array_interface__["data"][0])

        start_band, y_off, x_off = chunk_metadata["offset"]
        output.mask[
            start_band : start_band + mask_chunk.shape[0],
            y_off : y_off + mask_chunk.shape[1],
            x_off : x_off + mask_chunk.shape[2],
        ] = mask_chunk

        if progbar is not None:
            progbar.update(raw_size)

    if progbar is not None:
        progbar.close()

    return output


def yield_chunks(metadata, data, progress, nodata):
    dtype = np.dtype(metadata["dtype"])
    chunk_iter = range(metadata["chunks"])
    if progress:
        chunk_iter = tqdm(chunk_iter, total=metadata["chunks"] - 1)

    for _ in chunk_iter:
        d = data.readline()
        chunk_metadata = json.loads(d.decode("utf-8").strip())

        if "error" in chunk_metadata:
            raise ServerError(chunk_metadata["error"])

        # Initialize chunk
        raw_size, buffer = read_blosc_buffer(data)
        chunk = np.ma.empty(chunk_metadata["shape"], dtype=dtype)

        if raw_size != chunk.nbytes:
            raise ServerError(
                "Did not receive complete chunk (got {}, expected {})".format(
                    raw_size, chunk.nbytes
                )
            )
        blosc.decompress_ptr(buffer, chunk.__array_interface__["data"][0])

        mask_raw_size, mask_buffer = read_blosc_buffer(data)
        if nodata is not None:
            mask_chunk = np.ma.empty(chunk_metadata["shape"], dtype=bool)
            if mask_raw_size != mask_chunk.nbytes:
                raise ServerError(
                    "Did not receive complete mask chunk (got {}, expected {})".format(
                        raw_size, chunk.nbytes
                    )
                )
            blosc.decompress_ptr(mask_buffer, mask_chunk.__array_interface__["data"][0])
            chunk[mask_chunk] = nodata

        if chunk.shape[0] == 1:
            yield np.squeeze(chunk, axis=0)
        else:
            yield np.transpose(chunk, [1, 2, 0])


def _retry(req, headers=None):
    # this provides a nominal 60 seconds of retry
    DELAY = 0.5
    MULTIPLIER = 2
    JITTER = 1.0
    MAX_DELAY = 30.0
    MAX_RETRIES = 8

    retry_count = 0

    # Should always be present outside of tests
    if headers is None:
        headers = {}

    while True:
        headers["x-retry-count"] = str(retry_count)

        try:
            return req(headers=headers)
        except (IncompleteRead, ProtocolError, ServerError):
            # IncompleteRead: Response length doesn’t match expected Content-Length
            # ProtocolError: Something unexpected happened mid-request/response
            # ServerError: the usual retryable bad status >= 500. Normally won't
            # occur thanks to the client Retry configuration.
            if retry_count == MAX_RETRIES:
                raise
        # MaxRetry and all other ClientError types will be raised to our caller

        if retry_count:
            # see https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/
            # this is a variation on "Full Jitter" with JITTER as a tuning parameter.
            delay = min(DELAY * MULTIPLIER ** (retry_count - 1), MAX_DELAY)
            delay = random.uniform(1.0 - JITTER, 1.0) * delay
            time.sleep(delay)
        retry_count += 1


class Raster(Service):
    """
    The Raster API retrieves data from the Descartes Labs Catalog. Direct use of
    the Raster API is not recommended. Consider using the Descartes Labs Scenes API instead.
    """

    # https://requests.readthedocs.io/en/master/user/advanced/#timeouts
    CONNECT_TIMEOUT = 9.5
    READ_TIMEOUT = 300

    TIMEOUT = (CONNECT_TIMEOUT, READ_TIMEOUT)

    def __init__(self, url=None, auth=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().raster_url

        super(Raster, self).__init__(url, auth=auth)

    @deprecate(deprecated=["place"])
    def raster(
        self,
        inputs,
        bands=None,
        scales=None,
        data_type=None,
        output_format="GTiff",
        srs=None,
        dimensions=None,
        resolution=None,
        bounds=None,
        bounds_srs=None,
        cutline=None,
        place=None,
        align_pixels=False,
        resampler=None,
        dltile=None,
        processing_level=None,
        outfile_basename=None,
        headers=None,
        progress=None,
        nodata=None,
        _retry=_retry,
        **pass_through_params,
    ):
        """Given a list of :class:`Metadata <descarteslabs.client.services.metadata.Metadata>` identifiers,
        retrieve a translated and warped mosaic as an image file.

        :param inputs: List of :class:`Metadata` identifiers.
        :param bands: List of requested bands. If the last item in the list is an alpha
            band (with data range `[0, 1]`) it affects rastering of all other bands:
            When rastering multiple images, they are combined image-by-image only where
            each respective image's alpha band is `1` (pixels where the alpha band is not
            `1` are "transparent" in the overlap between images). If a pixel is fully
            masked considering all combined alpha bands it will be `0` in all non-alpha
            bands. Not specifying bands returns all bands in the product.
        :param scales: List of tuples specifying the scaling to be applied to each band.
            A tuple has 4 elements in the order ``(src_min, src_max, out_min, out_max)``,
            meaning values in the source range ``src_min`` to ``src_max`` will be scaled
            to the output range ``out_min`` to ``out_max``. A tuple with 2 elements
            ``(src_min, src_max)`` is also allowed, in which case the output range
            defaults to ``(0, 255)`` (a useful default for the common output type
            ``Byte``).  If no scaling is desired for a band, use ``None``.  This tuple
            format and behaviour is identical to GDAL's scales during translation.
            Example argument: ``[(0, 10000, 0, 127), (0, 1, 0, 1), (0, 10000)]`` - the first
            band will have source values 0-10000 scaled to 0-127, the second band will
            not be scaled, the third band will have 0-10000 scaled to 0-255.
        :param str output_format: Output format (one of ``GTiff``, ``PNG``, ``JPEG``).
            The default is ``GTiff``.
        :param str data_type: Output data type (one of ``Byte``, ``UInt16``, ``Int16``,
            ``UInt32``, ``Int32``, ``Float32``, ``Float64``).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units. Incompatible with
            `dimensions`
        :param tuple dimensions: Desired output (width, height) in pixels within which
            the raster should fit; i.e. the longer side of the raster will be min(dimensions).
            Incompatible with `resolution`.
        :param str cutline: A GeoJSON object to be used as a cutline, or WKT string.
                            GeoJSON coordinates must be in WGS84 lat-lon.
        :param str place: A slug identifier to be used as a cutline.
        :param tuple bounds: ``(min_x, min_y, max_x, max_y)`` in target SRS.
        :param str bounds_srs:
            Override the coordinate system in which bounds are expressed.
            If not given, bounds are assumed to be expressed in the output SRS.
        :param bool align_pixels: Align pixels to the target coordinate system.
        :param str resampler: Resampling algorithm to be used during warping (``near``,
            ``bilinear``, ``cubic``, ``cubicsplice``, ``lanczos``, ``average``, ``mode``,
            ``max``, ``min``, ``med``, ``q1``, ``q3``).
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str processing_level: How the processing level of the underlying data
            should be adjusted, one of ``toa`` (top of atmosphere) and ``surface``. For
            products that support it, ``surface`` applies Descartes Labs' general surface
            reflectance algorithm to the output.
        :param str outfile_basename: Overrides default filename using this string as a base.
        :param bool progress: Display a progress bar.
        :param None or number: A nodata value to use in the file where pixels are masked.
            Only used for non-JPEG geotiff files.

        :return: A tuple of (`filename`, ``metadata`` dictionary).
            The dictionary contains details about the raster operation that happened.
            These details can be useful for debugging but shouldn't otherwise be relied on
            (there are no guarantees that certain keys will be present).
        """

        params = self._construct_npz_params(
            inputs=inputs,
            bands=bands,
            scales=scales,
            data_type=data_type,
            srs=srs,
            resolution=resolution,
            dimensions=dimensions,
            cutline=cutline,
            place=place,
            bounds=bounds,
            bounds_srs=bounds_srs,
            align_pixels=align_pixels,
            resampler=resampler,
            dltile=dltile,
            processing_level=processing_level,
            output_window=None,
            pass_through_params=pass_through_params,
        )

        if outfile_basename is None:
            outfile_basename = params["ids"][0]

        file_ext = {
            "GTiff": ".tif",
            "JPEG": ".jpeg",
            "PNG": ".png",
        }

        if output_format not in file_ext:
            raise ValueError("output_format must be one of GTiff, JPEG, PNG")
        ext = file_ext[output_format]

        outfile = outfile_basename + ext

        def retry_req(headers):
            r = self.session.post(
                "/npz", headers=headers or {}, json=params, stream=True
            )
            metadata = json.loads(r.raw.readline().decode("utf-8").strip())
            blosc_meta = json.loads(r.raw.readline().decode("utf-8").strip())

            chunk_iter = yield_chunks(blosc_meta, r.raw, progress, nodata)

            if "id" not in metadata:
                metadata["id"] = inputs[0]

            try:
                if output_format == "GTiff":
                    make_geotiff(
                        outfile, chunk_iter, metadata, blosc_meta, None, nodata
                    )
                elif output_format == "JPEG":
                    make_geotiff(
                        outfile, chunk_iter, metadata, blosc_meta, "JPEG", None
                    )
                elif output_format == "PNG":
                    tif_out = outfile_basename + ".tif"
                    try:
                        make_geotiff(
                            tif_out, chunk_iter, metadata, blosc_meta, "PNG", None
                        )
                        try:
                            im = Image.open(tif_out)
                            im.save(outfile)
                        except Exception:
                            raise RuntimeError("Cannot save PNG image")
                    finally:
                        if os.path.isfile(tif_out):
                            os.remove(tif_out)
            except Exception:
                if os.path.isfile(outfile):
                    os.remove(outfile)
                raise

            return (outfile, metadata)

        return _retry(retry_req, headers=headers)

    @deprecate(deprecated=["place"])
    def ndarray(
        self,
        inputs,
        bands=None,
        scales=None,
        data_type=None,
        srs=None,
        resolution=None,
        dimensions=None,
        cutline=None,
        place=None,
        bounds=None,
        bounds_srs=None,
        align_pixels=False,
        resampler=None,
        order="image",
        dltile=None,
        processing_level=None,
        output_window=None,
        headers=None,
        progress=None,
        masked=True,
        _retry=_retry,
        **pass_through_params,
    ):
        """Retrieve a raster as a NumPy array.

        :param inputs: List of :class:`Metadata` identifiers.
        :param bands: List of requested bands. If the last item in the list is an alpha
            band (with data range `[0, 1]`) it affects rastering of all other bands:
            When rastering multiple images, they are combined image-by-image only where
            each respective image's alpha band is `1` (pixels where the alpha band is not
            `1` are "transparent" in the overlap between images). If a pixel is fully
            masked considering all combined alpha bands it will be `0` in all non-alpha
            bands. Not specifying bands returns all bands in the product.
        :param scales: List of tuples specifying the scaling to be applied to each band.
            A tuple has 4 elements in the order ``(src_min, src_max, out_min, out_max)``,
            meaning values in the source range ``src_min`` to ``src_max`` will be scaled
            to the output range ``out_min`` to ``out_max``. A tuple with 2 elements
            ``(src_min, src_max)`` is also allowed, in which case the output range
            defaults to ``(0, 255)`` (a useful default for the common output type
            ``Byte``).  If no scaling is desired for a band, use ``None``. This tuple
            format and behaviour is identical to GDAL's scales during translation.
            Example argument: ``[(0, 10000, 0, 127), (0, 1, 0, 1), (0, 10000)]`` - the first
            band will have source values 0-10000 scaled to 0-127, the second band will
            not be scaled, the third band will have 0-10000 scaled to 0-255.
        :param str data_type: Output data type (one of ``Byte``, ``UInt16``, ``Int16``,
            ``UInt32``, ``Int32``, ``Float32``, ``Float64``).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units. Incompatible with
            `dimensions`
        :param tuple dimensions: Desired output (width, height) in pixels within which
            the raster should fit; i.e. the longer side of the raster will be min(dimensions).
            Incompatible with `resolution`.
        :param str cutline: A GeoJSON object to be used as a cutline, or WKT string.
                            GeoJSON coordinates must be in WGS84 lat-lon.
        :param str place: A slug identifier to be used as a cutline.
        :param tuple bounds: ``(min_x, min_y, max_x, max_y)`` in target SRS.
        :param str bounds_srs:
            Override the coordinate system in which bounds are expressed.
            If not given, bounds are assumed to be expressed in the output SRS.
        :param bool align_pixels: Align pixels to the target coordinate system.
        :param str resampler: Resampling algorithm to be used during warping (``near``,
            ``bilinear``, ``cubic``, ``cubicsplice``, ``lanczos``, ``average``, ``mode``,
            ``max``, ``min``, ``med``, ``q1``, ``q3``).
        :param str order: Order of the returned array. `image` returns arrays as
            ``(row, column, band)`` while `gdal` returns arrays as ``(band, row, column)``.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str processing_level: How the processing level of the underlying data
            should be adjusted, one of ``toa`` (top of atmosphere) and ``surface``. For
            products that support it, ``surface`` applies Descartes Labs' general surface
            reflectance algorithm to the output.
        :param bool masked: Whether to return a masked array or a regular Numpy array.
        :param bool progress: Display a progress bar.

        :return: A tuple of ``(np_array, metadata)``. The first element (``np_array``) is
            the rastered scene as a NumPy array. The second element (``metadata``) is a
            dictionary containing details about the raster operation that happened. These
            details can be useful for debugging but shouldn't otherwise be relied on (there
            are no guarantees that certain keys will be present).
        """

        params = self._construct_npz_params(
            inputs=inputs,
            bands=bands,
            scales=scales,
            data_type=data_type,
            srs=srs,
            resolution=resolution,
            dimensions=dimensions,
            cutline=cutline,
            place=place,
            bounds=bounds,
            bounds_srs=bounds_srs,
            align_pixels=align_pixels,
            resampler=resampler,
            dltile=dltile,
            processing_level=processing_level,
            output_window=output_window,
            pass_through_params=pass_through_params,
        )

        def retry_req(headers):
            r = self.session.post(
                "/npz", headers=headers or {}, json=params, stream=True
            )
            metadata = json.loads(r.raw.readline().decode("utf-8").strip())
            array_meta = json.loads(r.raw.readline().decode("utf-8").strip())
            array = read_tiled_blosc_array(array_meta, r.raw, progress=progress)
            return array, metadata

        array, metadata = _retry(retry_req, headers=headers)

        if not masked:
            array = array.data

        if len(array.shape) > 2:
            if order == "image":
                return array.transpose((1, 2, 0)), metadata
            elif order == "gdal":
                return array, metadata
        else:
            return array, metadata

    def _serial_ndarray(self, id_groups, *args, **kwargs):
        for i, id_group in enumerate(id_groups):
            arr, meta = self.ndarray(id_group, *args, **kwargs)
            yield i, arr, meta

    def _threaded_ndarray(self, id_groups, *args, **kwargs):
        """
        Thread ndarray calls by id group, keeping the same `args` and
        `kwargs` for each raster.ndarray call.
        """
        max_workers = kwargs.pop(
            "max_workers", min(len(id_groups), DEFAULT_MAX_WORKERS)
        )
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_ndarrays = {}
            for i, id_group in enumerate(id_groups):
                future_ndarrays[
                    executor.submit(self.ndarray, id_group, *args, **kwargs)
                ] = i

            for future in futures.as_completed(future_ndarrays):
                i = future_ndarrays[future]
                arr, meta = future.result()
                yield i, arr, meta

    @deprecate(deprecated=["place"])
    def stack(
        self,
        inputs,
        bands=None,
        scales=None,
        data_type="UInt16",
        srs=None,
        resolution=None,
        dimensions=None,
        cutline=None,
        place=None,
        bounds=None,
        bounds_srs=None,
        align_pixels=False,
        resampler=None,
        order="image",
        dltile=None,
        processing_level=None,
        max_workers=None,
        masked=True,
        progress=None,
        **pass_through_params,
    ):
        """Retrieve a stack of rasters as a 4-D NumPy array.

        To ensure every raster in the stack has the same shape and covers the same
        spatial extent, you must either:

        * set ``dltile``, or
        * set [``resolution`` or ``dimensions``], ``srs``, and ``bounds``

        :param inputs: List, or list of lists, of :class:`Metadata` identifiers.
            The stack will follow the same order as this list.
            Each element in the list is treated as a separate input to ``raster.ndarray``,
            so if a list of lists is given, each sublist's identifiers will be mosaiced together
            to become a single level in the stack.
        :param bands: List of requested bands. If the last item in the list is an alpha
            band (with data range `[0, 1]`) it affects rastering of all other bands:
            When rastering multiple images, they are combined image-by-image only where
            each respective image's alpha band is `1` (pixels where the alpha band is not
            `1` are "transparent" in the overlap between images). If a pixel is fully
            masked considering all combined alpha bands it will be `0` in all non-alpha
            bands. Not specifying bands returns all bands in the product.
        :param scales: List of tuples specifying the scaling to be applied to each band.
            A tuple has 4 elements in the order ``(src_min, src_max, out_min, out_max)``,
            meaning values in the source range ``src_min`` to ``src_max`` will be scaled
            to the output range ``out_min`` to ``out_max``. A tuple with 2 elements
            ``(src_min, src_max)`` is also allowed, in which case the output range
            defaults to ``(0, 255)`` (a useful default for the common output type
            ``Byte``).  If no scaling is desired for a band, use ``None``. This tuple
            format and behaviour is identical to GDAL's scales during translation.
            Example argument: ``[(0, 10000, 0, 127), (0, 1, 0, 1), (0, 10000)]`` - the first
            band will have source values 0-10000 scaled to 0-127, the second band will
            not be scaled, the third band will have 0-10000 scaled to 0-255.
        :param str data_type: Output data type (one of ``Byte``, ``UInt16``, ``Int16``,
            ``UInt32``, ``Int32``, ``Float32``, ``Float64``).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units. Incompatible with
            `dimensions`
        :param tuple dimensions: Desired output (width, height) in pixels within which
            the raster should fit; i.e. the longer side of the raster will be min(dimensions).
            Incompatible with `resolution`.
        :param str cutline: A GeoJSON object to be used as a cutline, or WKT string.
                            GeoJSON coordinates must be in WGS84 lat-lon.
        :param str place: A slug identifier to be used as a cutline.
        :param tuple bounds: ``(min_x, min_y, max_x, max_y)`` in target SRS.
        :param str bounds_srs:
            Override the coordinate system in which bounds are expressed.
            If not given, bounds are assumed to be expressed in the output SRS.
        :param bool align_pixels: Align pixels to the target coordinate system.
        :param str resampler: Resampling algorithm to be used during warping (``near``,
            ``bilinear``, ``cubic``, ``cubicsplice``, ``lanczos``, ``average``, ``mode``,
            ``max``, ``min``, ``med``, ``q1``, ``q3``).
        :param str order: Order of the returned array. `image` returns arrays as
            ``(scene, row, column, band)`` while `gdal` returns arrays as ``(scene, band, row, column)``.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str processing_level: How the processing level of the underlying data
            should be adjusted, one of ``toa`` (top of atmosphere) and ``surface``. For
            products that support it, ``surface`` applies Descartes Labs' general surface
            reflectance algorithm to the output.
        :param int max_workers: Maximum number of threads over which to
            parallelize individual ndarray calls. If `None`, will be set to the minimum
            of the number of inputs and `DEFAULT_MAX_WORKERS`.
        :param bool masked: Whether to return a masked array or a regular Numpy array.
        :param bool progress: Display a progress bar.

        :return: A tuple of ``(stack, metadata)``.

            * ``stack``: 4D ndarray. The axes are ordered ``(scene, band, y, x)``
              (or ``(scene, y, x, band)`` if ``order="gdal"``). The scenes in the outermost
              axis are in the same order as the list of identifiers given as ``inputs``.
            * ``metadata``: List[dict] of the rasterization metadata for each element in ``inputs``.
              As with the metadata returned by :meth:`ndarray` and :meth:`raster`, these dictionaries
              contain useful information about the raster, such as its geotransform matrix and WKT
              of its coordinate system, but there are no guarantees that certain keys will be present.
        """
        if not isinstance(inputs, (list, tuple)):
            raise TypeError(
                "Inputs must be a list or tuple, instead got '{}'".format(type(inputs))
            )

        if dltile is None:
            if resolution is None and dimensions is None:
                raise ValueError("Must set `resolution` or `dimensions`")
            if srs is None:
                raise ValueError("Must set `srs`")
            if bounds is None:
                raise ValueError("Must set `bounds`")

        if place is not None:
            from ..places import Places

            shape = Places(auth=self.auth, _suppress_deprecation_warnings=True).shape(
                place, geom="low"
            )
            cutline = json.dumps(shape["geometry"])

        params = dict(
            bands=bands,
            scales=scales,
            data_type=data_type,
            srs=srs,
            resolution=resolution,
            dimensions=dimensions,
            cutline=cutline,
            bounds=bounds,
            bounds_srs=bounds_srs,
            align_pixels=align_pixels,
            resampler=resampler,
            order=order,
            dltile=dltile,
            processing_level=processing_level,
            max_workers=max_workers,
            masked=masked,
            progress=progress,
            **pass_through_params,
        )

        full_stack = None
        metadata = [None] * len(inputs)
        for i, arr, meta in self._threaded_ndarray(inputs, **params):
            if len(arr.shape) == 2:
                if order == "image":
                    arr = np.expand_dims(arr, -1)
                elif order == "gdal":
                    arr = np.expand_dims(arr, 0)
                else:
                    raise ValueError(
                        "Unknown order '{}'; should be one of 'image' or 'gdal'".format(
                            order
                        )
                    )
            if full_stack is None:
                stack_shape = (len(inputs),) + arr.shape
                if masked:
                    full_stack = np.ma.empty(stack_shape, dtype=arr.dtype)
                else:
                    full_stack = np.empty(stack_shape, dtype=arr.dtype)

            full_stack[i] = arr
            metadata[i] = meta

        return full_stack, metadata

    def _construct_npz_params(
        self,
        inputs,
        bands,
        scales,
        data_type,
        srs,
        resolution,
        dimensions,
        cutline,
        place,
        bounds,
        bounds_srs,
        align_pixels,
        resampler,
        dltile,
        processing_level,
        output_window,
        pass_through_params,
    ):

        cutline = as_json_string(cutline)

        if place is not None:
            from ..places import Places

            shape = Places(auth=self.auth, _suppress_deprecation_warnings=True).shape(
                place, geom="low"
            )
            cutline = json.dumps(shape["geometry"])

        if type(inputs) is str:
            inputs = [inputs]

        params = {
            "ids": inputs,
            "bands": bands,
            "scales": scales,
            "ot": data_type,
            "srs": srs,
            "resolution": resolution,
            "shape": cutline,
            "outputBounds": bounds,
            "outputBoundsSRS": bounds_srs,
            "outsize": dimensions,
            "targetAlignedPixels": align_pixels,
            "resampleAlg": resampler,
            "processing_level": processing_level,
            "output_window": output_window,
            "of": "blosc",
        }
        params.update(pass_through_params)

        if dltile is not None:
            if isinstance(dltile, dict):
                tile_key = dltile["properties"]["key"]
            else:
                tile_key = dltile

            tile_params = Tile.from_key(tile_key).geocontext["properties"]
            params["outputBounds"] = tile_params["outputBounds"]
            params["resolution"] = tile_params["resolution"]
            params["srs"] = tile_params["cs_code"]

        return params

    def dltile(self, key):
        """
        This method has been removed and is no longer included in the documentation.
        """
        raise NotImplementedError(
            "dltile is removed, use descarteslabs.scenes.DLTile.from_key instead"
        )

    def dltile_from_latlon(self, lat, lon, resolution, tilesize, pad):
        """
        This method has been removed and is no longer included in the documentation.
        """
        raise NotImplementedError(
            "dltile_from_latlon is removed, use descarteslabs.scenes.DLTile.from_latlon instead"
        )

    def dltiles_from_shape(self, resolution, tilesize, pad, shape):
        """
        This method has been removed and is no longer included in the documentation.
        """
        raise NotImplementedError(
            "dltiles_from_shape is removed, use descarteslabs.scenes.DLTile.from_shape instead"
        )

    def iter_tiles_from_shape(self, resolution, tilesize, pad, shape):
        """
        This method has been removed and is no longer included in the documentation.
        """
        raise NotImplementedError(
            "iter_tiles_from_shape is removed, use descarteslabs.scenes.DLTile.iter_from_shape instead"
        )
