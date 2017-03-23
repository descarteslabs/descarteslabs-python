from .service import Service
from .places import Places
from descarteslabs.addons import FeatureArray
from descarteslabs.addons import numpy as np
import base64
import json


class Raster(Service):
    """Raster"""
    TIMEOUT = 300

    def __init__(
            self,
            url='https://platform-services.descarteslabs.com/raster',
            token=None
    ):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        Service.__init__(self, url, token)

    def get_bands_by_key(self, key):
        r = self.session.get('%s/bands/key/%s' % (self.url, key), timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def get_bands_by_constellation(self, const):
        r = self.session.get('%s/bands/constellation/%s' % (self.url, const), timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def dlkeys_from_shape(self, resolution, tilesize, pad, shape):
        params = {
            'resolution': resolution,
            'tilesize': tilesize,
            'pad': pad,
            'shape': shape,
        }

        r = self.session.post('%s/dlkeys/from_shape' % (self.url), json=params, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def dlkey_from_latlon(self, lat, lon, resolution, tilesize, pad):
        params = {
            'resolution': resolution,
            'tilesize': tilesize,
            'pad': pad,
        }

        r = self.session.get('%s/dlkeys/from_latlon/%f/%f' % (self.url, lat, lon),
                             params=params, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def dlkey(self, key):

        r = self.session.get('%s/dlkeys/%s' % (self.url, key), timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def raster(
            self,
            keys=None,
            bands=None,
            scales=None,
            ot=None,
            of='GTiff',
            srs=None,
            resolution=None,
            shape=None,
            location=None,
            outputBounds=None,
            outputBoundsSRS=None,
            outsize=None,
            targetAlignedPixels=False,
            resampleAlg=None,
    ):
        """
        Given a list of :class:`Metadata <descarteslabs.services.Metadata>` identifiers,
        retrieve a translated and warped mosaic.

        :param keys: List of :class:`Metadata` identifiers.
        :param bands: List of requested bands.
        :param scales: List of tuples specifying the scaling to be applied to each band.
            If no scaling is desired for a band, use ``None`` where appropriate. If a
            tuple contains four elements, the last two will be used as the output range.
            For example, ``(0, 10000, 0, 128)`` would scale the source values 0-10000 to
            be returned as 0-128 in the output.
        :param str of: Output format (`GTiff`, `PNG`, ...).
        :param str ot: Output data type (`Byte`, `UInt8`, `UInt16`, `Float32`, etc).
        :param str srs: Output spatial reference system definition understood by GDAL.
        :param float resolution: Desired resolution in output SRS units.
        :param tuple outsize: Desired output (width, height) in pixels.
        :param str shape: A GeoJSON feature to be used as a cutline.
        :param str location: A slug identifier to be used as a cutline.
        :param tuple outputBounds: ``(min_x, min_y, max_x, max_y)`` in target SRS.
        :param str outputBoundsSRS: Override the coordinate system in which bounds are expressed.
        :param bool targetAlignedPixels: Align pixels to the target coordinate system.
        :param str resampleAlg: Resampling algorithm to be used during warping (``near``,
            ``bilinear``, ``cubic``, ``cubicsplice``, ``lanczos``, ``average``, ``mode``,
            ``max``, ``min``, ``med``, ``q1``, ``q3``).
        """

        if location is not None:
            places = Places()
            shape = places.shape(location, geom='low')
            shape = json.dumps(shape['geometry'])

        params = {
            'keys': keys,
            'bands': bands,
            'scales': scales,
            'ot': ot,
            'of': of,
            'srs': srs,
            'resolution': resolution,
            'shape': shape,
            'outputBounds': outputBounds,
            'outputBoundsSRS': outputBoundsSRS,
            'outsize': outsize,
            'targetAlignedPixels': targetAlignedPixels,
            'resampleAlg': resampleAlg,
        }

        r = self.session.post('%s/raster' % (self.url), json=params, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        json_resp = r.json()
        # Decode base64
        for k in json_resp['files'].keys():
            json_resp['files'][k] = base64.b64decode(json_resp['files'][k])

        return json_resp

    def ndarray(
            self,
            keys=None,
            bands=None,
            scales=None,
            ot=None,
            srs=None,
            resolution=None,
            shape=None,
            location=None,
            outputBounds=None,
            outputBoundsSRS=None,
            outsize=None,
            targetAlignedPixels=False,
            resampleAlg=None,
            order='image',
    ):
        """
        Retrieve a raster as a NumPy array.

        See :meth:`raster` for more information.

        :param str order: Order of the returned array. `image` returns arrays as
            ``(row, column, band)`` while `gdal` returns arrays as ``(band, row, column)``.

        """

        if location is not None:
            places = Places()
            shape = places.shape(location, geom='low')
            shape = json.dumps(shape['geometry'])

        params = {
            'keys': keys,
            'bands': bands,
            'scales': scales,
            'ot': ot,
            'srs': srs,
            'resolution': resolution,
            'shape': shape,
            'outputBounds': outputBounds,
            'outputBoundsSRS': outputBoundsSRS,
            'outsize': outsize,
            'targetAlignedPixels': targetAlignedPixels,
            'resampleAlg': resampleAlg,
        }

        r = self.session.post('%s/featurearray' % (self.url), json=params, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        fa = FeatureArray.deserialize(r.content, base64.b64decode)

        if order == 'image':
            return fa.transpose((1, 2, 0)).view(np.ndarray)
        elif order == 'gdal':
            return fa.view(np.ndarray)
