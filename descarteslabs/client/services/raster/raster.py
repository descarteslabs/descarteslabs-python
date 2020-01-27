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
import struct
from io import BytesIO
import logging
import warnings

import six

from descarteslabs.client.addons import ThirdParty, blosc, concurrent, numpy as np
from descarteslabs.client.auth import Auth
from descarteslabs.client.services.places import Places
from descarteslabs.client.services.service.service import Service
from descarteslabs.client.exceptions import ServerError
from descarteslabs.common.dotdict import DotDict
from descarteslabs.common.shapely_support import shapely_to_geojson


DEFAULT_MAX_WORKERS = 8
MAXIMUM_MAX_WORKERS = 25


def as_json_string(str_or_dict):
    if not str_or_dict:
        return str_or_dict
    elif isinstance(str_or_dict, dict):
        return json.dumps(str_or_dict)
    else:
        return str_or_dict


def read_blosc_buffer(data):
    header = data.read(16)

    _, size, _, compressed_size = struct.unpack("<IIII", header)
    body = data.read(compressed_size - 16)

    return size, header + body


def read_blosc_array(metadata, data):
    output = np.empty(metadata["shape"], dtype=np.dtype(metadata["dtype"]))
    ptr = output.__array_interface__["data"][0]

    for _ in metadata["chunks"]:
        raw_size, buffer = read_blosc_buffer(data)
        blosc.decompress_ptr(buffer, ptr)
        ptr += raw_size

    bytes_received = ptr - output.__array_interface__["data"][0]

    if bytes_received != output.nbytes:
        raise ServerError(
            "Did not receive complete array (got {}, expected {})".format(
                bytes_received, output.nbytes
            )
        )

    return output


def read_blosc_string(metadata, data):
    output = b""

    for _ in metadata["chunks"]:
        _, buffer = read_blosc_buffer(data)
        output += blosc.decompress(buffer)

    return output


class Raster(Service):
    """
    The Raster API has two purposes: retrieving data from the Descartes Labs Catalog,
    and providing a consistent way of splitting that data into smaller tiles.
    """

    TIMEOUT = (9.5, 300)

    def __init__(self, url=None, auth=None, retries=None):
        """
        :param str url: A HTTP URL pointing to a version of the storage service
            (defaults to current version)
        :param Auth auth: A custom user authentication (defaults to the user
            authenticated locally by token information on disk or by environment
            variables)
        :param urllib3.util.retry.Retry retries: A custom retry configuration
            used for all API requests (defaults to a reasonable amount of retries)
        """
        if auth is None:
            auth = Auth()

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_RASTER_URL",
                "https://platform.descarteslabs.com/raster/v1",
            )

        super(Raster, self).__init__(url, auth=auth, retries=retries)

    def iter_dltiles_from_shape(self, resolution, tilesize, pad, shape, maxtiles=None):
        """
        Iterates over all DLTiles as GeoJSONs features that intersect the
        GeoJSON geometry ``shape`` or Shapely shape object.

        :param float resolution: Resolution of DLTile
        :param int tilesize: Number of valid pixels per DLTile
        :param int pad: Number of ghost pixels per DLTile (overlap among tiles)
        :param str shape: A GeoJSON geometry or Shapely shape specifying a shape over
            which to intersect DLTiles.

        :return: An iterator over GeoJSON features representing
            intersecting DLTiles
        :rtype: generator(DotDict)

        Example::

            >>> iowa_geom = {
            ...     "coordinates": [[
            ...         [-96.498997, 42.560832],
            ...         [-95.765645, 40.585208],
            ...         [-91.729115, 40.61364],
            ...         [-91.391613, 40.384038],
            ...         [-90.952233, 40.954047],
            ...         [-91.04589, 41.414085],
            ...         [-90.343228, 41.587833],
            ...         [-90.140613, 41.995999],
            ...         [-91.065059, 42.751338],
            ...         [-91.217706, 43.50055],
            ...         [-96.599191, 43.500456],
            ...         [-96.498997, 42.560832]
            ...     ]],
            ...     "type": "Polygon"
            ... }
            >>> next(Raster().iter_dltiles_from_shape(30.0, 2048, 16, iowa_geom))
            {
              'geometry': {
                'coordinates': [
                  [
                    [-96.81264975325402, 41.045203319986356],
                    [-96.07101667769108, 41.02873098016475],
                    [-96.04576296033223, 41.59007261142797],
                    [-96.79377566762066, 41.6068715494603...],
                    ...
                  ]
                ],
                'type': 'Polygon'
              },
              'properties': {
                'cs_code': 'EPSG:32614',
                'geotrans': [
                  683840.0,
                  30.0,
                  0,
                  4608480.0,
                  ...
                ],
                'key': '2048:16:30.0:14:3:74',
                'outputBounds': [683840.0, 4546080.0, 746240.0, 4608480.0],
                'pad': 16,
                'proj4': '+proj=utm +zone=14 +datum=WGS84 +units=m +no_defs ',
                'resolution': 30.0,
                'ti': 3,
                'tilesize': 2048,
                'tj': 74,
                'wkt': 'PROJCS["WGS 84 / UTM zone 14N",GEOGCS["WGS...Northing",NORTH],AUTHORITY["EPSG","32614"]]',
                'zone': 14
              },
              'type': 'Feature'
            }
        """

        if maxtiles is not None:
            warnings.warn(
                "The maxtiles argument is deprecated and will be removed in a future "
                "release.",
                DeprecationWarning,
            )

        shape = shapely_to_geojson(shape)
        shape = as_json_string(shape)
        params = {
            "resolution": resolution,
            "tilesize": tilesize,
            "pad": pad,
            "shape": shape,
            "maxtiles": 5000,
        }

        while True:
            r = self.session.post("/dlkeys/from_shape", json=params)
            fc = DotDict(r.json())
            for t in fc.features:
                yield t
            iterstate = fc.get("iterstate", None)
            if iterstate:
                params["start_zone"] = iterstate.start_zone
                params["start_ti"] = iterstate.start_ti
                params["start_tj"] = iterstate.start_tj
            else:
                break

    def dltiles_from_shape(self, resolution, tilesize, pad, shape):
        """
        Return a feature collection of DLTile GeoJSONs that intersect
        a GeoJSON geometry ``shape`` or a Shapely shape object.

        :param float resolution: Resolution of DLTile
        :param int tilesize: Number of valid pixels per DLTile
        :param int pad: Number of ghost pixels per DLTile (overlap among tiles)
        :param str shape: A GeoJSON geometry specifying a shape over
            which to intersect DLTiles

        :return: GeoJSON FeatureCollection of intersecting DLTile geometries.
        :rtype: DotDict

        :raises descarteslabs.client.exceptions.BadRequestError: if the given
            parameters would generate too many tiles - use
            :meth:`Raster.iter_dltiles_from_shape` to iterate over more

        Example::

            >>> from descarteslabs.client.services import Raster
            >>> iowa_geom = {
            ...     "coordinates": [[
            ...         [-96.498997, 42.560832],
            ...         [-95.765645, 40.585208],
            ...         [-91.729115, 40.61364],
            ...         [-91.391613, 40.384038],
            ...         [-90.952233, 40.954047],
            ...         [-91.04589, 41.414085],
            ...         [-90.343228, 41.587833],
            ...         [-90.140613, 41.995999],
            ...         [-91.065059, 42.751338],
            ...         [-91.217706, 43.50055],
            ...         [-96.599191, 43.500456],
            ...         [-96.498997, 42.560832]
            ...     ]],
            ...     "type": "Polygon"
            ... }
            >>> tiles = Raster().dltiles_from_shape(30.0, 2048, 16, iowa_geom)
            >>> tiles['features'][0]
            {
              'geometry': {
                'coordinates': [
                  [
                    [-96.81264975325402, 41.045203319986356],
                    [-96.07101667769108, 41.02873098016475],
                    [-96.04576296033223, 41.59007261142797],
                    [-96.79377566762066, 41.6068715494603...],
                    ...
                  ]
                ],
                'type': 'Polygon'
              },
              'properties': {
                'cs_code': 'EPSG:32614',
                'geotrans': [
                  683840.0,
                  30.0,
                  0,
                  4608480.0,
                  ...
                ],
                'key': '2048:16:30.0:14:3:74',
                'outputBounds': [683840.0, 4546080.0, 746240.0, 4608480.0],
                'pad': 16,
                'proj4': '+proj=utm +zone=14 +datum=WGS84 +units=m +no_defs ',
                'resolution': 30.0,
                'ti': 3,
                'tilesize': 2048,
                'tj': 74,
                'wkt': 'PROJCS["WGS 84 / UTM zone 14N",GEOGCS["WGS...Northing",NORTH],AUTHORITY["EPSG","32614"]]',
                'zone': 14
              },
              'type': 'Feature'
            }
        """
        shape = shapely_to_geojson(shape)
        return DotDict(
            type="FeatureCollection",
            features=list(
                self.iter_dltiles_from_shape(resolution, tilesize, pad, shape)
            ),
        )

    def dltile_from_latlon(self, lat, lon, resolution, tilesize, pad):
        """
        Return a DLTile GeoJSON Feature that covers a latitude/longitude

        :param float lat: Requested latitude
        :param float lon: Requested longitude
        :param float resolution: Resolution of DLTile
        :param int tilesize: Number of valid pixels per DLTile
        :param int pad: Number of ghost pixels per DLTile (overlap among tiles)

        :return: A DLTile GeoJSON Feature
        :rtype: DotDict

        Example::

            >>> from descarteslabs.client.services import Raster
            >>> Raster().dltile_from_latlon(45, 60, 15.0, 1024, 16)
            {
              'geometry': {
                'coordinates': [
                  [
                    [59.88428127486419, 44.8985115884728...],
                    [60.08463455818353, 44.90380671613201],
                    [60.077403974563175, 45.046212550598135],
                    [59.87655568675822, 45.040891215906676],
                    ...
                  ]
                ],
                'type': 'Polygon'
              },
              'properties': {
                'cs_code': 'EPSG:32641',
                'geotrans': [
                  254000.0,
                  15.0,
                  0,
                  4992240.0,
                  ...
                ],
                'key': '1024:16:15.0:41:-16:324',
                'outputBounds': [254000.0, 4976400.0, 269840.0, 4992240.0],
                'pad': 16,
                'proj4': '+proj=utm +zone=41 +datum=WGS84 +units=m +no_defs ',
                'resolution': 15.0,
                'ti': -16,
                'tilesize': 1024,
                'tj': 324,
                'wkt': 'PROJCS["WGS 84 / UTM zone 41N",GEOGCS["WGS...Northing",NORTH],AUTHORITY["EPSG","32641"]]',
                'zone': 41
              },
              'type': 'Feature'
            }
        """
        params = {"resolution": resolution, "tilesize": tilesize, "pad": pad}

        r = self.session.get("/dlkeys/from_latlon/%f/%f" % (lat, lon), params=params)

        return DotDict(r.json())

    def dltile(self, key):
        """
        Given a DLTile key, return a DLTile GeoJSON Feature

        :param str key: A DLTile key that identifies a DLTile

        :return: A DLTile GeoJSON Feature
        :rtype: DotDict

        :raises descarteslabs.client.exceptions.BadRequestError: if the given key
            is not a valid DLTile key

        Example::

            >>> from descarteslabs.client.services import Raster
            >>> Raster().dltile("1024:16:15.0:41:-16:324")
            {
              'geometry': {
                'coordinates': [
                  [
                    [59.88428127486419, 44.8985115884728...],
                    [60.08463455818353, 44.90380671613201],
                    [60.077403974563175, 45.046212550598135],
                    [59.87655568675822, 45.040891215906676],
                    ...
                  ]
                ],
                'type': 'Polygon'
              },
              'properties': {
                'cs_code': 'EPSG:32641',
                'geotrans': [
                  254000.0,
                  15.0,
                  0,
                  4992240.0,
                  ...
                ],
                'key': '1024:16:15.0:41:-16:324',
                'outputBounds': [254000.0, 4976400.0, 269840.0, 4992240.0],
                'pad': 16,
                'proj4': '+proj=utm +zone=41 +datum=WGS84 +units=m +no_defs ',
                'resolution': 15.0,
                'ti': -16,
                'tilesize': 1024,
                'tj': 324,
                'wkt': 'PROJCS["WGS 84 / UTM zone 41N",GEOGCS["WGS...Northing",NORTH],AUTHORITY["EPSG","32641"]]',
                'zone': 41
              },
              'type': 'Feature'
            }
        """

        if not key:
            raise ValueError("Invalid key")

        r = self.session.get("/dlkeys/%s" % key)

        return DotDict(r.json())

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
        save=False,
        outfile_basename=None,
        **pass_through_params
    ):
        """
        Given a list of catalog image identifiers, retrieve a translated and
        warped mosaic as an image file.

        :param list(str) inputs: List of catalog image identifiers.
        :param list(str) bands: List of requested bands. If the last item in the list is an alpha
            band (with data range ``[0, 1]``) it affects rastering of all other bands:
            When rastering multiple images, they are combined image-by-image only where
            each respective image's alpha band is ``1`` (pixels where the alpha band is not
            ``1`` are "transparent" in the overlap between images). If a pixel is fully
            masked considering all combined alpha bands it will be ``0`` in all non-alpha
            bands. Not specifying bands returns all bands in the product.
        :param list(tuple()) scales: List of tuples specifying the scaling to be applied to each band.
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
        :param str data_type: Output data type (one of ``Byte``, ``UInt16``, ``Int16``,
            ``UInt32``, ``Int32``, ``Float32``, ``Float64``).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units. Incompatible with
            ``dimensions``
        :param tuple dimensions: Desired output ``(width, height)`` in pixels within which
            the raster should fit; i.e. the longer side of the raster will be min(dimensions).
            Incompatible with ``resolution``.
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
        :param bool save: Write resulting files to disk. Default: False
        :param str outfile_basename: If ``save`` is True, override default filename using
            this string as a base.

        :return: A dictionary with two keys, ``files`` and ``metadata``.

            * ``files``: a dictionary mapping file names to binary data for files (at the
              moment there will always be only a single file with the appropriate file
              extension based on the ``output_format`` requested)
            * ``metadata``: a dictionary containing details about the raster operation
              that happened. These details can be useful for debugging but shouldn't
              otherwise be relied on (there are no guarantees that certain keys will be
              present).

        :rtype: DotDict
        """
        cutline = as_json_string(cutline)

        if place:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom="low")
            cutline = json.dumps(shape["geometry"])

        params = {
            "ids": inputs,
            "bands": bands,
            "scales": scales,
            "ot": data_type,
            "of": output_format,
            "srs": srs,
            "resolution": resolution,
            "shape": cutline,
            "outputBounds": bounds,
            "outputBoundsSRS": bounds_srs,
            "outsize": dimensions,
            "targetAlignedPixels": align_pixels,
            "resampleAlg": resampler,
            "processing_level": processing_level,
        }
        params.update(pass_through_params)

        if dltile is not None:
            if isinstance(dltile, dict):
                params["dltile"] = dltile["properties"]["key"]
            else:
                params["dltile"] = dltile

        r = self.session.post("/raster", json=params)

        raw = BytesIO(r.content)

        json_resp = json.loads(raw.readline().decode("utf-8").strip())

        num_files = json_resp["files"]
        json_resp["files"] = {}

        for _ in range(num_files):
            file_meta = json.loads(raw.readline().decode("utf-8").strip())

            fn = file_meta["name"]
            data = raw.read(file_meta["length"])

            if outfile_basename:
                _, ext = os.path.splitext(os.path.basename(fn))
                outfilename = "{}{}".format(outfile_basename, ext)
            else:
                outfilename = fn

            json_resp["files"][outfilename] = data

        if save:
            for filename, data in six.iteritems(json_resp["files"]):
                with open(filename, "wb") as f:
                    f.write(data)

        return DotDict(json_resp)

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
        **pass_through_params
    ):
        """Retrieve a raster as a NumPy array.

        :param list(str) inputs: List of catalog image identifiers.
        :param list(str) bands: List of requested bands. If the last item in the list is an alpha
            band (with data range ``[0, 1]``) it affects rastering of all other bands:
            When rastering multiple images, they are combined image-by-image only where
            each respective image's alpha band is ``1`` (pixels where the alpha band is not
            ``1`` are "transparent" in the overlap between images). If a pixel is fully
            masked considering all combined alpha bands it will be ``0`` in all non-alpha
            bands. Not specifying bands returns all bands in the product.
        :param list(tuple()) scales: List of tuples specifying the scaling to be applied to each band.
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
        :param str data_type: Output data type (one of ``Byte``, ``UInt16``, ``Int16``,
            ``UInt32``, ``Int32``, ``Float32``, ``Float64``).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units. Incompatible with
            ``dimensions``
        :param tuple dimensions: Desired output ``(width, height)`` in pixels within which
            the raster should fit; i.e. the longer side of the raster will be min(dimensions).
            Incompatible with ``resolution``.
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
        :param str order: Order of the returned array. ``image`` returns arrays as
            ``(row, column, band)`` while ``gdal`` returns arrays as ``(band, row, column)``.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str processing_level: How the processing level of the underlying data
            should be adjusted, one of ``toa`` (top of atmosphere) and ``surface``. For
            products that support it, ``surface`` applies Descartes Labs' general surface
            reflectance algorithm to the output.

        :return: A tuple of ``(np_array, metadata)``.

            * ``np_array``: the rastered scene as a NumPy array
            * ``metadata``: a dictionary containing details about the raster operation
              that happened. These details can be useful for debugging but shouldn't
              otherwise be relied on (there are no guarantees that certain keys will be
              present).

        :rtype: tuple(numpy.ndarray, DotDict)
        """
        cutline = as_json_string(cutline)

        if place is not None:
            places = Places(auth=self.auth)
            shape = places.shape(place, geom="low")
            cutline = json.dumps(shape["geometry"])

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
        }
        params.update(pass_through_params)

        if dltile is not None:
            if isinstance(dltile, dict):
                params["dltile"] = dltile["properties"]["key"]
            else:
                params["dltile"] = dltile

        can_blosc = not isinstance(blosc, ThirdParty)

        if can_blosc:
            params["of"] = "blosc"
        else:
            params["of"] = "npz"

        r = self.session.post("/npz", json=params, stream=True)

        if can_blosc:
            metadata = json.loads(r.raw.readline().decode("utf-8").strip())
            array_meta = json.loads(r.raw.readline().decode("utf-8").strip())
            array = read_blosc_array(array_meta, r.raw)
        else:
            npz = np.load(BytesIO(r.content))
            array = npz["data"]
            metadata = json.loads(npz["metadata"].tostring().decode("utf-8"))

        if len(array.shape) > 2:
            if order == "image":
                return array.transpose((1, 2, 0)), metadata
            elif order == "gdal":
                return array, metadata
        else:
            return array, DotDict(metadata)

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
        if max_workers is None:
            max_workers = DEFAULT_MAX_WORKERS

        if max_workers > MAXIMUM_MAX_WORKERS:
            logging.warning(
                "max_workers greater than {max_workers}. Setting max workers from {original} to {max_workers}".format(
                    max_workers=MAXIMUM_MAX_WORKERS, original=max_workers
                )
            )
            max_workers = MAXIMUM_MAX_WORKERS

        try:
            futures = concurrent.futures
        except ImportError:
            logging.warning(
                "Failed to import concurrent.futures. ndarray calls will be serial"
            )
            # Fallback to serial execution
            for i, arr, meta in self._serial_ndarray(id_groups, *args, **kwargs):
                yield i, arr, meta
            return

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
        **pass_through_params
    ):
        """
        Retrieve a stack of rasters as a 4-D NumPy array.

        To ensure every raster in the stack has the same shape and covers the same
        spatial extent, you must either:

        * set ``dltile``, or
        * set [``resolution`` or ``dimensions``], ``srs``, and ``bounds``

        :param list|list(list) inputs: List, or list of lists, of catalog image identifiers.
            The stack will follow the same order as this list.
            Each element in the list is treated as a separate input to :meth:`ndarray`,
            so if a list of lists is given, each sublist's identifiers will be mosaicked together
            to become a single level in the stack.
        :param list(str) bands: List of requested bands. If the last item in the list is an alpha
            band (with data range ``[0, 1]``) it affects rastering of all other bands:
            When rastering multiple images, they are combined image-by-image only where
            each respective image's alpha band is ``1`` (pixels where the alpha band is not
            ``1`` are "transparent" in the overlap between images). If a pixel is fully
            masked considering all combined alpha bands it will be ``0`` in all non-alpha
            bands. Not specifying bands returns all bands in the product.
        :param list(tuple()) scales: List of tuples specifying the scaling to be applied to each band.
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
        :param str data_type: Output data type (one of ``Byte``, ``UInt16``, ``Int16``,
            ``UInt32``, ``Int32``, ``Float32``, ``Float64``).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units. Incompatible with
            ``dimensions``
        :param tuple dimensions: Desired output ``(width, height)`` in pixels within which
            the raster should fit; i.e. the longer side of the raster will be min(dimensions).
            Incompatible with ``resolution``.
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
        :param str order: Order of the returned array. ``image`` returns arrays as
            ``(scene, row, column, band)`` while ``gdal`` returns arrays as ``(scene, band, row, column)``.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str processing_level: How the processing level of the underlying data
            should be adjusted, one of ``toa`` (top of atmosphere) and ``surface``. For
            products that support it, ``surface`` applies Descartes Labs' general surface
            reflectance algorithm to the output.
        :param int max_workers: Maximum number of threads over which to
            parallelize individual ndarray calls. If ``None``, will be set to the minimum
            of the number of inputs and ``DEFAULT_MAX_WORKERS``. Maximum number of workers allowed
            is 25.

        :return: A tuple of ``(stack, metadata)``.

            * ``stack``: 4D ndarray. The axes are ordered ``(scene, y, x, band)``
              (or ``(scene, band, y, x)`` if ``order="gdal"``). The scenes in the outermost
              axis are in the same order as the list of identifiers given as ``inputs``.
            * ``metadata``: list of the rasterization metadata for each element in ``inputs``.
              As with the metadata returned by :meth:`ndarray` and :meth:`raster`, these dictionaries
              contain useful information about the raster, such as its geotransform matrix and WKT
              of its coordinate system, but there are no guarantees that certain keys will be present.

        :rtype: tuple(numpy.ndarray, list(DotDict))
        """
        if not isinstance(inputs, (list, tuple)):
            raise TypeError(
                "Inputs must be a list or tuple, instead got '{}'".format(type(inputs))
            )

        params = dict(
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
            order=order,
            dltile=dltile,
            processing_level=processing_level,
            max_workers=max_workers,
            **pass_through_params
        )

        if dltile is None:
            if resolution is None and dimensions is None:
                raise ValueError("Must set `resolution` or `dimensions`")
            if cutline is None:
                if bounds is None:
                    raise ValueError("Must set `bounds`")
                if srs is None:
                    raise ValueError("Must set `srs`")

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
                full_stack = np.empty(stack_shape, dtype=arr.dtype)
            full_stack[i] = arr
            metadata[i] = meta

        return full_stack, metadata
