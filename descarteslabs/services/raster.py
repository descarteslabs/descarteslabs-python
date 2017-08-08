# Copyright 2017 Descartes Labs.
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
import os
from io import BytesIO
import json
import warnings

from descarteslabs.addons import ThirdParty, blosc, numpy as np
from descarteslabs.utilities import as_json_string, read_blosc_array, read_blosc_string
from .service import Service
from .places import Places
import six


RASTER_BANDS_WARNING = """
Band retrieval through the raster service is deprecated, and will be
removed eventually. Use the corresponding methods on the metadata
service instead."""


class Raster(Service):
    """Raster"""
    TIMEOUT = (9.5, 300)

    def __init__(self, url=None, token=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        warnings.simplefilter('always', DeprecationWarning)
        if url is None:
            url = os.environ.get("DESCARTESLABS_RASTER_URL", "https://platform-services.descarteslabs.com/raster/v2")

        Service.__init__(self, url, token)

    def get_bands_by_key(self, key):
        """
        For a given source id, return the available bands.

        :param str key: A :class:`Metadata` identifiers.

        :return: A dictionary of band entries and their metadata.
        """
        warnings.warn(RASTER_BANDS_WARNING, DeprecationWarning)
        r = self.session.get('/bands/key/%s' % key)

        return r.json()

    def get_bands_by_constellation(self, const):
        """
        For a given constellation id, return the available bands.

        :param str const: A constellation name/abbreviation.

        :return: A dictionary of band entries and their metadata.
        """
        warnings.warn(RASTER_BANDS_WARNING, DeprecationWarning)
        r = self.session.get('/bands/constellation/%s' % const)
        return r.json()

    def dltiles_from_shape(self, resolution, tilesize, pad, shape):
        """
        Return a feature collection of DLTile GeoJSONs that intersect
        a GeoJSON Geometry `shape`.

        :param float resolution: Resolution of DLTile
        :param int tilesize: Number of valid pixels per DLTile
        :param int pad: Number of ghost pixels per DLTile (overlap among tiles)
        :param str shape: A GeoJSON geometry specifying a shape over
            which to intersect DLTiles.

        :return: GeoJSON FeatureCollection of intersecting DLTile geometries.

        Example::

            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> iowa = dl.places.shape("north-america_united-states_iowa")
            >>> tiles = dl.raster.dltiles_from_shape(30.0, 2048, 16, iowa)
            >>> pprint(tiles['features'][0])
            {'geometry': {'coordinates': [[[-96.81264975325391, 41.04520331997488],
                                           [-96.07101667769165, 41.02873098011615],
                                           [-96.04576296033328, 41.590072611375206],
                                           [-96.79377566762062, 41.606871549447824],
                                           [-96.81264975325391, 41.04520331997488]]],
                          'type': 'Polygon'},
             'properties': {'cs_code': 'EPSG:32614',
                            'key': '2048:16:30.0:14:3:74',
                            'outputBounds': [683840.0, 4546080.0, 746240.0, 4608480.0],
                            'pad': 16,
                            'resolution': 30.0,
                            'ti': 3,
                            'tilesize': 2048,
                            'tj': 74,
                            'zone': 14},
             'type': 'Feature'}

        """

        shape = as_json_string(shape)
        params = {
            'resolution': resolution,
            'tilesize': tilesize,
            'pad': pad,
            'shape': shape,
        }

        r = self.session.post('/dlkeys/from_shape', json=params)
        return r.json()

    def dltile_from_latlon(self, lat, lon, resolution, tilesize, pad):
        """
        Return a DLTile GeoJSON Feature that covers a latitude/longitude

        :param float lat: Requested latitude
        :param float lon: Requested longitude
        :param float resolution: Resolution of DLTile
        :param int tilesize: Number of valid pixels per DLTile
        :param int pad: Number of ghost pixels per DLTile (overlap among tiles)

        :return: A DLTile GeoJSON Feature

        Example::

            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> pprint(dl.raster.dltile_from_latlon(45, 60, 15.0, 1024, 16))
            {'geometry': {'coordinates': [[[59.88428127486957, 44.89851158838881],
                                           [60.084634558186266, 44.903806716073376],
                                           [60.07740397456606, 45.04621255053833],
                                           [59.87655568676388, 45.04089121582091],
                                           [59.88428127486957, 44.89851158838881]]],
                          'type': 'Polygon'},
            'properties': {'cs_code': 'EPSG:32641',
                           'key': '1024:16:15.0:41:-16:324',
                           'outputBounds': [254000.0, 4976400.0, 269840.0, 4992240.0],
                           'pad': 16,
                           'resolution': 15.0,
                           'ti': -16,
                           'tilesize': 1024,
                           'tj': 324,
                           'zone': 41},
            'type': 'Feature'}
        """
        params = {
            'resolution': resolution,
            'tilesize': tilesize,
            'pad': pad,
        }

        r = self.session.get('/dlkeys/from_latlon/%f/%f' % (lat, lon), params=params)

        return r.json()

    def dltile(self, key):
        """
        Given a DLTile key, return a DLTile GeoJSON Feature

        :param str key: A DLTile key that identifies a DLTile

        :return: A DLTile GeoJSON Feature

        Example::

            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> pprint(dl.raster.dltile("1024:16:15.0:41:-16:324"))
            {'geometry': {'coordinates': [[[59.88428127486957, 44.89851158838881],
                                           [60.084634558186266, 44.903806716073376],
                                           [60.07740397456606, 45.04621255053833],
                                           [59.87655568676388, 45.04089121582091],
                                           [59.88428127486957, 44.89851158838881]]],
                          'type': 'Polygon'},
             'properties': {'cs_code': 'EPSG:32641',
                            'key': '1024:16:15.0:41:-16:324',
                            'outputBounds': [254000.0, 4976400.0, 269840.0, 4992240.0],
                            'pad': 16,
                            'resolution': 15.0,
                            'ti': -16,
                            'tilesize': 1024,
                            'tj': 324,
                            'zone': 41},
             'type': 'Feature'}
        """

        r = self.session.get('/dlkeys/%s' % key)

        return r.json()

    def dlkeys_from_shape(self, resolution, tilesize, pad, shape):
        """
        Deprecated. See dltiles_from_shape
        """
        warnings.warn("dlkey methods have been renamed to dltile",
                      DeprecationWarning, stacklevel=2)
        return self.dltiles_from_shape(resolution, tilesize, pad, shape)

    def dlkey_from_latlon(self, lat, lon, resolution, tilesize, pad):
        """
        Deprecated. See dltile_from_latlon
        """
        warnings.warn("dlkey methods have been renamed to dltile",
                      DeprecationWarning, stacklevel=2)
        return self.dltile_from_latlon(lat, lon, resolution, tilesize, pad)

    def dlkey(self, key):
        """
        Deprecated. See dltile
        """
        warnings.warn("dlkey methods have been renamed to dltile",
                      DeprecationWarning, stacklevel=2)
        return self.dltile(key)

    def raster(
            self,
            inputs,
            bands=None,
            scales=None,
            data_type=None,
            output_format='GTiff',
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
            save=False,
            outfile_basename=None
    ):
        """Given a list of :class:`Metadata <descarteslabs.services.Metadata>` identifiers,
        retrieve a translated and warped mosaic.

        :param inputs: List of :class:`Metadata` identifiers.
        :param bands: List of requested bands.
        :param scales: List of tuples specifying the scaling to be applied to each band.
            If no scaling is desired for a band, use ``None`` where appropriate. If a
            tuple contains four elements, the last two will be used as the output range.
            For example, ``(0, 10000, 0, 128)`` would scale the source values 0-10000 to
            be returned as 0-128 in the output.
        :param str output_format: Output format (`GTiff`, `PNG`, ...).
        :param str data_type: Output data type (`Byte`, `UInt8`, `UInt16`, `Float32`, etc).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units. Incompatible with
            `dimensions`
        :param tuple dimensions: Desired output (width, height) in pixels. Incompatible with
            `resolution`
        :param str cutline: A GeoJSON feature or geometry to be used as a cutline.
        :param str place: A slug identifier to be used as a cutline.
        :param tuple bounds: ``(min_x, min_y, max_x, max_y)`` in target SRS.
        :param str bounds_srs: Override the coordinate system in which bounds are expressed.
        :param bool align_pixels: Align pixels to the target coordinate system.
        :param str resampler: Resampling algorithm to be used during warping (``near``,
            ``bilinear``, ``cubic``, ``cubicsplice``, ``lanczos``, ``average``, ``mode``,
            ``max``, ``min``, ``med``, ``q1``, ``q3``).
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param bool save: Write resulting files to disk. Default: False
        :param str outfile_basename: If 'save' is True, override default filename using
            this string as a base.
        """
        cutline = as_json_string(cutline)

        if place:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom='low')
            cutline = json.dumps(shape['geometry'])

        params = {
            'keys': inputs,
            'bands': bands,
            'scales': scales,
            'ot': data_type,
            'of': output_format,
            'srs': srs,
            'resolution': resolution,
            'shape': cutline,
            'outputBounds': bounds,
            'outputBoundsSRS': bounds_srs,
            'outsize': dimensions,
            'targetAlignedPixels': align_pixels,
            'resampleAlg': resampler,
        }

        if dltile is not None:
            if isinstance(dltile, dict):
                params['dltile'] = dltile['properties']['key']
            else:
                params['dltile'] = dltile

        r = self.session.post('/raster', json=params)

        raw = BytesIO(r.content)

        json_resp = json.loads(raw.readline().decode('utf-8').strip())

        num_files = json_resp['files']
        json_resp['files'] = {}

        for _ in range(num_files):
            file_meta = json.loads(raw.readline().decode('utf-8').strip())

            fn = file_meta['filename']
            data = raw.read(file_meta['length'])

            if outfile_basename:
                outfilename = "{}.{}".format(
                    outfile_basename,
                    ".".join(os.path.basename(fn).split(".")[1:])
                )
            else:
                outfilename = fn

            json_resp['files'][outfilename] = data

        if save:
            for filename, data in six.iteritems(json_resp['files']):
                with open(filename, "wb") as f:
                    f.write(data)

        return json_resp

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
            order='image',
            dltile=None,
    ):
        """Retrieve a raster as a NumPy array.

        :param inputs: List of :class:`Metadata` identifiers.
        :param bands: List of requested bands.
        :param scales: List of tuples specifying the scaling to be applied to each band.
            If no scaling is desired for a band, use ``None`` where appropriate. If a
            tuple contains four elements, the last two will be used as the output range.
            For example, ``(0, 10000, 0, 128)`` would scale the source values 0-10000 to
            be returned as 0-128 in the output.
        :param str data_type: Output data type (`Byte`, `UInt8`, `UInt16`, `Float32`, etc).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units. Incompatible with
            `dimensions`
        :param tuple dimensions: Desired output (width, height) in pixels. Incompatible with
            `resolution`
        :param str cutline: A GeoJSON feature or geometry to be used as a cutline.
        :param str place: A slug identifier to be used as a cutline.
        :param tuple bounds: ``(min_x, min_y, max_x, max_y)`` in target SRS.
        :param str bounds_srs: Override the coordinate system in which bounds are expressed.
        :param bool align_pixels: Align pixels to the target coordinate system.
        :param str resampler: Resampling algorithm to be used during warping (``near``,
            ``bilinear``, ``cubic``, ``cubicsplice``, ``lanczos``, ``average``, ``mode``,
            ``max``, ``min``, ``med``, ``q1``, ``q3``).
        :param str order: Order of the returned array. `image` returns arrays as
            ``(row, column, band)`` while `gdal` returns arrays as ``(band, row, column)``.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.

        """
        cutline = as_json_string(cutline)

        if place is not None:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom='low')
            cutline = json.dumps(shape['geometry'])

        params = {
            'keys': inputs,
            'bands': bands,
            'scales': scales,
            'ot': data_type,
            'srs': srs,
            'resolution': resolution,
            'shape': cutline,
            'outputBounds': bounds,
            'outputBoundsSRS': bounds_srs,
            'outsize': dimensions,
            'targetAlignedPixels': align_pixels,
            'resampleAlg': resampler,
        }

        if dltile is not None:
            if isinstance(dltile, dict):
                params['dltile'] = dltile['properties']['key']
            else:
                params['dltile'] = dltile

        can_blosc = not isinstance(blosc, ThirdParty)

        if can_blosc:
            params['of'] = 'blosc'
        else:
            params['of'] = 'npz'

        r = self.session.post('/npz', json=params, stream=True)

        if can_blosc:
            metadata = json.loads(r.raw.readline().decode('utf-8').strip())
            array_meta = json.loads(r.raw.readline().decode('utf-8').strip())
            array = read_blosc_array(array_meta, r.raw)
        else:
            npz = np.load(BytesIO(r.content))
            array = npz['data']
            metadata = json.loads(npz['metadata'].tostring().decode('utf-8'))

        if len(array.shape) > 2:
            if order == 'image':
                return array.transpose((1, 2, 0)), metadata
            elif order == 'gdal':
                return array, metadata
        else:
            return array, metadata
