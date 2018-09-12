# Copyright 2018 Descartes Labs.
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
from warnings import warn, simplefilter
from six import string_types
from descarteslabs.client.services.service import Service
from descarteslabs.client.services.places import Places
from descarteslabs.client.auth import Auth
from descarteslabs.client.deprecation import check_deprecated_kwargs
from descarteslabs.client.services.raster import Raster
from descarteslabs.client.services.metadata.metadata_filtering import GLOBAL_PROPERTIES
from descarteslabs.common.property_filtering.filtering import AndExpression
from descarteslabs.common.dotdict import DotDict, DotList


SOURCES_DEPRECATION_MESSAGE = (
    "Metadata.sources() has been deprecated and will be removed in "
    "future versions of the library. Please use "
    "Metadata.available_products() or Metadata.products() instead. "
)


class Metadata(Service):
    """
    Image Metadata Service

    Any methods that take start and end timestamps accept most common date/time
    formats as a string. If no explicit timezone is given, the timestamp is assumed
    to be in UTC. For example ``'2012-06-01'`` means June 1st 2012 00:00 in UTC,
    ``'2012-06-01 00:00+02:00'`` means June 1st 2012 00:00 in GMT+2.
    """

    TIMEOUT = (9.5, 120)

    properties = GLOBAL_PROPERTIES

    def __init__(self, url=None, auth=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        if auth is None:
            auth = Auth()

        simplefilter('always', DeprecationWarning)
        if url is None:
            url = os.environ.get("DESCARTESLABS_METADATA_URL",
                                 "https://platform.descarteslabs.com/metadata/v1")

        super(Metadata, self).__init__(url, auth=auth)
        self._raster = Raster(auth=self.auth)

    def sources(self):
        warn(SOURCES_DEPRECATION_MESSAGE, DeprecationWarning)

        r = self.session.get('/sources')
        return DotList(r.json())

    def bands(
        self,
        products=None,
        limit=None,
        offset=None,
        wavelength=None,
        resolution=None,
        tags=None,
        bands=None,
        **kwargs
    ):
        """Search for imagery data bands that you have access to.

        :param list(str) products: A list of product(s) to return bands for.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.
        :param float wavelength: A wavelength in nm e.g 700 that the band sensor must measure.
        :param int resolution: The resolution in meters per pixel e.g 30 of the data available in this band.
        :param list(str) tags: A list of tags that the band must have in its own tag list.


        """
        params = ['limit', 'offset', 'products',
                  'wavelength', 'resolution', 'tags']

        args = locals()
        kwargs = dict(kwargs, **{
            param: args[param]
            for param in params
            if args[param] is not None
        })

        r = self.session.post('/bands/search', json=kwargs)
        return DotList(r.json())

    def derived_bands(self, bands=None, require_bands=None, limit=None, offset=None, **kwargs):
        """Search for predefined derived bands that you have access to.

        :param list(str) bands: Limit the derived bands to ones that can be
                                computed using this list of spectral bands.
                                e.g ["red", "nir", "swir1"]
        :param bool require_bands: Control whether searched bands must contain
                                   all the spectral bands passed in the bands param.
                                   Defaults to False.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.
        """
        params = ['bands', 'limit', 'offset']

        args = locals()
        kwargs = dict(kwargs, **{
            param: args[param]
            for param in params
            if args[param] is not None
        })

        r = self.session.post('/bands/derived/search', json=kwargs)
        return DotList(r.json())

    def get_bands_by_id(self, id_):
        """
        For a given source id, return the available bands.

        :param str id_: A :class:`Metadata` identifier.

        :return: A dictionary of band entries and their metadata.
        """
        r = self.session.get('/bands/id/{}'.format(id_))

        return DotDict(r.json())

    def get_bands_by_product(self, product_id):
        """
        All bands (includig derived bands) available in a product.

        :param str product_id: A product identifier.

        :return: A dictionary mapping band IDs to dictionaries of their metadata.
        """
        r = self.session.get('/bands/all/{}'.format(product_id))

        return DotDict(r.json())

    def products(self, bands=None, limit=None, offset=None, owner=None, text=None, **kwargs):
        """Search products that are available on the platform.

        :param list(str) bands: Band name(s) e.g ["red", "nir"] to filter products by.
                                Note that products must match all bands that are passed.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.
        :param str owner: Filter products by the owner's uuid.
        :param str text: Filter products by string match.

        """
        params = ['limit', 'offset', 'bands', 'owner', 'text']

        args = locals()
        kwargs = dict(kwargs, **{
            param: args[param]
            for param in params
            if args[param] is not None
        })
        check_deprecated_kwargs(kwargs, {"band": "bands"})

        r = self.session.post('/products/search', json=kwargs)

        return DotList(r.json())

    def available_products(self):
        """Get the list of product identifiers you have access to.

        Example::
            >>> from descarteslabs.client.services import Metadata
            >>> products = Metadata().available_products()
            >>> products  # doctest: +SKIP
            ['landsat:LC08:PRE:TOAR']

        """
        r = self.session.get('/products')

        return DotList(r.json())

    def summary(self, products=None, sat_ids=None, date='acquired', part=None,
                place=None, geom=None, start_datetime=None, end_datetime=None, cloud_fraction=None,
                cloud_fraction_0=None, fill_fraction=None, q=None, pixels=None,
                dltile=None, **kwargs):
        """Get a summary of the results for the specified spatio-temporal query.

        :param list(str) products: Product identifier(s).
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str part: Part of the date to aggregate over (e.g. `day`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param expr q: Expression for filtering the results. See :py:attr:`descarteslabs.utilities.properties`.
        :param bool pixels: Whether to include pixel counts in summary calculations.
        :param str dltile: A dltile key used to specify the resolution, bounds, and srs.

        Example usage::

            >>> from descarteslabs.client.services import Metadata
            >>> Metadata().summary(place='north-america_united-states_iowa', \
                    products=['landsat:LC08:PRE:TOAR'], start_datetime='2016-07-06', \
                    end_datetime='2016-07-07', part='hour', pixels=True)
            {
              'bytes': 93298309,
              'count': 1,
              'items': [
                {
                  'bytes': 93298309,
                  'count': 1,
                  'date': '2016-07-06T16:00:00',
                  'pixels': 250508160,
                  'timestamp': 1467820800
                }
              ],
              'pixels': 250508160,
              'products': ['landsat:LC08:PRE:TOAR']
            }
        """
        check_deprecated_kwargs(kwargs, {
            "product": "products",
            "const_id": "const_ids",
            "sat_id": "sat_ids",
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        })

        if place:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom='low')
            geom = json.dumps(shape['geometry'])

        if dltile is not None:
            if isinstance(dltile, string_types):
                dltile = self._raster.dltile(dltile)
            if isinstance(dltile, dict):
                geom = dltile['geometry']

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        if sat_ids:
            if isinstance(sat_ids, string_types):
                sat_ids = [sat_ids]

            kwargs['sat_ids'] = sat_ids

        if products:
            if isinstance(products, string_types):
                products = [products]

            kwargs['products'] = products

        if date:
            kwargs['date'] = date

        if part:
            kwargs['part'] = part

        if geom:
            kwargs['geom'] = geom

        if start_datetime:
            kwargs['start_datetime'] = start_datetime

        if end_datetime:
            kwargs['end_datetime'] = end_datetime

        if cloud_fraction is not None:
            kwargs['cloud_fraction'] = cloud_fraction

        if cloud_fraction_0 is not None:
            kwargs['cloud_fraction_0'] = cloud_fraction_0

        if fill_fraction is not None:
            kwargs['fill_fraction'] = fill_fraction

        if q is not None:
            if not isinstance(q, list):
                q = [q]
            kwargs['query_expr'] = AndExpression(q).serialize()

        if pixels:
            kwargs['pixels'] = pixels

        r = self.session.post('/summary', json=kwargs)
        return DotDict(r.json())

    def search(self, products=None, sat_ids=None, date='acquired', place=None,
               geom=None, start_datetime=None, end_datetime=None, cloud_fraction=None,
               cloud_fraction_0=None, fill_fraction=None, q=None, limit=100,
               fields=None, dltile=None, sort_field=None, sort_order="asc", randomize=None,
               continuation_token=None, **kwargs):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. For accessing more than 10000 results, see :py:func:`features`.

        :param list(str) products: Product Identifier(s).
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param expr q: Expression for filtering the results. See :py:attr:`descarteslabs.utilities.properties`.
        :param int limit: Number of items to return up to the maximum of 10000.
        :param list(str) fields: Properties to return.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.
        :param bool randomize: Randomize the results. You may also use an `int` or `str` as an explicit seed.

        return: GeoJSON ``FeatureCollection``

        Example::

            >>> from descarteslabs.client.services import Metadata
            >>> scenes = Metadata().search(place='north-america_united-states_iowa', \
                                         products=['landsat:LC08:PRE:TOAR'], \
                                         start_datetime='2016-07-01', \
                                         end_datetime='2016-07-31T23:59:59')
            >>> len(scenes['features'])  # doctest: +SKIP
            2
        """
        check_deprecated_kwargs(kwargs, {
            "product": "products",
            "const_id": "const_ids",
            "sat_id": "sat_ids",
            "start_time": "start_datetime",
            "end_time": "end_datetime",
            "offset": None,
        })

        if place:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom='low')
            geom = json.dumps(shape['geometry'])

        if dltile is not None:
            if isinstance(dltile, string_types):
                dltile = self._raster.dltile(dltile)
            if isinstance(dltile, dict):
                geom = dltile['geometry']

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        kwargs.update({'date': date, 'limit': limit})

        if sat_ids:
            if isinstance(sat_ids, string_types):
                sat_ids = [sat_ids]

            kwargs['sat_ids'] = sat_ids

        if products:
            if isinstance(products, string_types):
                products = [products]

            kwargs['products'] = products

        if geom:
            kwargs['geom'] = geom

        if start_datetime:
            kwargs['start_datetime'] = start_datetime

        if end_datetime:
            kwargs['end_datetime'] = end_datetime

        if cloud_fraction is not None:
            kwargs['cloud_fraction'] = cloud_fraction

        if cloud_fraction_0 is not None:
            kwargs['cloud_fraction_0'] = cloud_fraction_0

        if fill_fraction is not None:
            kwargs['fill_fraction'] = fill_fraction

        if fields is not None:
            kwargs['fields'] = fields

        if q is not None:
            if not isinstance(q, list):
                q = [q]
            kwargs['query_expr'] = AndExpression(q).serialize()

        if sort_field is not None:
            kwargs['sort_field'] = sort_field

        if sort_order is not None:
            kwargs['sort_order'] = sort_order

        if randomize is not None:
            kwargs['random_seed'] = randomize

        if continuation_token is not None:
            kwargs['continuation_token'] = continuation_token

        r = self.session.post('/search', json=kwargs)

        fc = {'type': 'FeatureCollection', "features": r.json()}

        if 'x-continuation-token' in r.headers:
            fc['properties'] = {
                'continuation_token': r.headers['x-continuation-token']}

        return DotDict(fc)

    def ids(self, products=None, sat_ids=None, date='acquired', place=None,
            geom=None, start_datetime=None, end_datetime=None, cloud_fraction=None,
            cloud_fraction_0=None, fill_fraction=None, q=None, limit=100,
            dltile=None, sort_field=None, sort_order=None, randomize=None, **kwargs):
        """Search metadata given a spatio-temporal query. All parameters are
        optional.

        :param list(str) products: Products identifier(s).
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param expr q: Expression for filtering the results. See :py:attr:`descarteslabs.utilities.properties`.
        :param int limit: Number of items to return.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.
        :param bool randomize: Randomize the results. You may also use an `int` or `str` as an explicit seed.

        :return: List of image identifiers.

        Example::

            >>> from descarteslabs.client.services import Metadata
            >>> ids = Metadata().ids(place='north-america_united-states_iowa', \
                                 products=['landsat:LC08:PRE:TOAR'], \
                                 start_datetime='2016-07-01', \
                                 end_datetime='2016-07-31T23:59:59')
            >>> len(ids)  # doctest: +SKIP
            1

            >>> ids  # doctest: +SKIP
            ['landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1', 'landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1']

        """
        result = self.search(sat_ids=sat_ids, products=products, date=date,
                             place=place, geom=geom, start_datetime=start_datetime,
                             end_datetime=end_datetime, cloud_fraction=cloud_fraction,
                             cloud_fraction_0=cloud_fraction_0, fill_fraction=fill_fraction,
                             q=q, limit=limit, fields=[], dltile=dltile,
                             sort_field=sort_field, sort_order=sort_order, randomize=randomize, **kwargs)

        return DotList(feature['id'] for feature in result['features'])

    def features(self, products=None, sat_ids=None, date='acquired', place=None,
                 geom=None, start_datetime=None, end_datetime=None, cloud_fraction=None,
                 cloud_fraction_0=None, fill_fraction=None, q=None, fields=None,
                 batch_size=1000, dltile=None, sort_field=None, sort_order='asc',
                 randomize=None, **kwargs):
        """Generator that efficiently scrolls through the search results.

        :param int batch_size: Number of features to fetch per request.

        :return: Generator of GeoJSON ``Feature`` objects.

        Example::

            >>> from descarteslabs.client.services import Metadata
            >>> features = Metadata().features("landsat:LC08:PRE:TOAR", \
                            start_datetime='2016-01-01', \
                            end_datetime="2016-03-01")
            >>> total = 0
            >>> for f in features: \
                    total += 1

            >>> total # doctest: +SKIP
            31898
        """

        continuation_token = None

        while True:
            result = self.search(sat_ids=sat_ids, products=products,
                                 date=date, place=place, geom=geom,
                                 start_datetime=start_datetime, end_datetime=end_datetime,
                                 cloud_fraction=cloud_fraction,
                                 cloud_fraction_0=cloud_fraction_0,
                                 fill_fraction=fill_fraction, q=q,
                                 fields=fields, limit=batch_size, dltile=dltile,
                                 sort_field=sort_field, sort_order=sort_order,
                                 randomize=randomize, continuation_token=continuation_token, **kwargs)

            if not result['features']:
                break

            for feature in result['features']:
                yield feature

            continuation_token = result['properties'].get('continuation_token')
            if not continuation_token:
                break

    def get(self, image_id):
        """Get metadata of a single image.

        :param str image_id: Image identifier.

        Example::

            >>> from descarteslabs.client.services import Metadata
            >>> meta = Metadata().get('landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1')
            >>> keys = list(meta.keys())
            >>> keys.sort()
            >>> keys
            ['acquired', 'area', 'bits_per_pixel', 'bright_fraction', 'bucket', 'cloud_fraction',
             'cloud_fraction_0', 'cs_code', 'descartes_version', 'file_md5s', 'file_sizes', 'files',
             'fill_fraction', 'geolocation_accuracy', 'geometry', 'geotrans', 'id', 'identifier', 'key',
             'processed', 'product', 'projcs', 'published', 'raster_size', 'reflectance_scale', 'roll_angle',
             'sat_id', 'solar_azimuth_angle', 'solar_elevation_angle', 'sw_version', 'terrain_correction',
             'tile_id']
        """
        r = self.session.get('/get/{}'.format(image_id))
        return DotDict(r.json())

    def get_by_ids(self, ids, fields=None, ignore_not_found=True, **kwargs):
        """Get metadata for multiple images by id. The response contains found images in the
        order of the given ids.

        :param list(str) ids: Image identifiers.
        :param list(str) fields: Properties to return.
        :param bool ignore_not_found: For image id lookups that fail: if :py:obj:`True`, ignore;
                                      if :py:obj:`False`, raise :py:exc:`NotFoundError`. Default is :py:obj:`True`.

        :return: List of image metadata.
        :rtype: list(dict)
        """
        kwargs['ids'] = ids
        kwargs['ignore_not_found'] = ignore_not_found
        if fields is not None:
            kwargs['fields'] = fields

        r = self.session.post('/batch/images', json=kwargs)
        return DotList(r.json())

    def get_product(self, product_id):
        """Get information about a single product.

        :param str product_id: Product Identifier.

        """
        r = self.session.get('/products/{}'.format(product_id))
        return DotDict(r.json())

    def get_band(self, band_id):
        """Get information about a single band.

        :param str band_id: Band Identifier.

        """
        r = self.session.get('/bands/{}'.format(band_id))
        return DotDict(r.json())

    def get_derived_band(self, derived_band_id):
        """Get information about a single product.

        :param str derived_band_id: Derived band identifier.

        """
        r = self.session.get('/bands/derived/{}'.format(derived_band_id))
        return DotDict(r.json())
