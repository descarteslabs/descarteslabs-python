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

import json
import os
from warnings import warn, simplefilter
from six import string_types
from .service import Service
from .places import Places

CONST_ID_DEPRECATION_MESSAGE = (
        "Keyword arg `const_id' has been deprecated and will be removed in "
        "future versions of this software. Use the `products` "
        "argument instead. Product identifiers can be found with the "
        " products() method."
    )


class Metadata(Service):
    TIMEOUT = (9.5, 120)
    """Image Metadata Service https://iam.descarteslabs.com/service/runcible"""

    def __init__(self, url=None, token=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        simplefilter('always', DeprecationWarning)
        if url is None:
            url = os.environ.get("DESCARTESLABS_METADATA_URL",
                                 "https://platform-services-dev.descarteslabs.com/metadata/dev")

        Service.__init__(self, url, token)

    def sources(self):
        """Get a list of image sources.

        Example::
            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> sources = dl.metadata.sources()
            >>> pprint(sources)
            [{'product': 'landsat:LC08:PRE:TOAR', 'sat_id': 'LANDSAT_8'}]

        """
        r = self.session.get('%s/sources' % self.url, timeout=self.TIMEOUT)

        return r.json()

    def products(self):
        """Get the list of product identifiers you have access to.

        Example::
            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> products = dl.metadata.products()
            >>> pprint(products)
            ['landsat:LC08:PRE:TOAR']

        """
        r = self.session.get('%s/products/all' % self.url, timeout=self.TIMEOUT)

        return r.json()

    def summary(self, products=None, const_id=None, sat_id=None, date='acquired', part=None,
                place=None, geom=None, start_time=None, end_time=None, cloud_fraction=None,
                cloud_fraction_0=None, fill_fraction=None, pixels=None, params=None):
        """Get a summary of the results for the specified spatio-temporal query.

        :param list(str) products: Product identifier(s).
        :param list(str) const_id: Constellation identifier(s).
        :param list(str) sat_id: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str part: Part of the date to aggregate over (e.g. `day`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_time: Desired starting date and time (inclusive).
        :param str end_time: Desired ending date and time (inclusive).
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param bool pixels: Whether to include pixel counts in summary calculations.
        :param str params: JSON of additional query parameters.

        Example usage::

            >>> import descarteslabs as dl
            >>> from pprint import  pprint
            >>> pprint(dl.metadata.summary(place='north-america_united-states_iowa', \
                    products=['landsat:LC08:PRE:TOAR'], start_time='2016-07-06', \
                    end_time='2016-07-07', part='hour', pixels=True))
            {'bytes': 93298309,
             'count': 1,
             'items': [{'bytes': 93298309,
                        'count': 1,
                        'date': '2016-07-06T16:00:00',
                        'pixels': 250508160,
                        'timestamp': 1467820800}],
             'pixels': 250508160,
             'products': ['landsat:LC08:PRE:TOAR']}
        """
        if place:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom='low')
            geom = json.dumps(shape['geometry'])

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        kwargs = {}

        if sat_id:

            if isinstance(sat_id, string_types):
                sat_id = [sat_id]

            kwargs['sat_id'] = sat_id

        if products:

            if isinstance(products, string_types):
                products = [products]

            kwargs['products'] = products

        if const_id:
            warn(CONST_ID_DEPRECATION_MESSAGE, DeprecationWarning)
            if isinstance(const_id, string_types):
                const_id = [const_id]

            kwargs['const_id'] = const_id

        if date:
            kwargs['date'] = date

        if part:
            kwargs['part'] = part

        if geom:
            kwargs['geom'] = geom

        if start_time:
            kwargs['start_time'] = start_time

        if end_time:
            kwargs['end_time'] = end_time

        if cloud_fraction is not None:
            kwargs['cloud_fraction'] = cloud_fraction

        if cloud_fraction_0 is not None:
            kwargs['cloud_fraction_0'] = cloud_fraction_0

        if fill_fraction is not None:
            kwargs['fill_fraction'] = fill_fraction

        if pixels:
            kwargs['pixels'] = pixels

        if params:
            kwargs['params'] = json.dumps(params)

        r = self.session.post('%s/summary' % self.url, json=kwargs, timeout=self.TIMEOUT)

        return r.json()

    def search(self, products=None, const_id=None, sat_id=None, date='acquired', place=None,
               geom=None, start_time=None, end_time=None, cloud_fraction=None,
               cloud_fraction_0=None, fill_fraction=None, params=None,
               limit=100, offset=0):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. Results are paged using limit/offset.

        :param list(str) products: Product Identifier(s).
        :param list(str) const_id: Constellation Identifier(s).
        :param list(str) sat_id: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_time: Desired starting date and time (inclusive).
        :param str end_time: Desired ending date and time (inclusive).
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param str params: JSON of additional query parameters.
        :param int limit: Number of items to return.
        :param int offset: Number of items to skip.

        return: GeoJSON ``FeatureCollection``

        Example::

            >>> import descarteslabs as dl
            >>> scenes = dl.metadata.search(place='north-america_united-states_iowa', \
                                         products=['landsat:LC08:PRE:TOAR'], \
                                         start_time='2016-07-01', \
                                         end_time='2016-07-31T23:59:59')
            >>> len(scenes['features'])
            1
        """
        if place:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom='low')
            geom = json.dumps(shape['geometry'])

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        kwargs = {}

        kwargs['limit'] = limit
        kwargs['offset'] = offset

        if sat_id:

            if isinstance(sat_id, string_types):
                sat_id = [sat_id]

            kwargs['sat_id'] = sat_id

        if products:

            if isinstance(products, string_types):
                products = [products]

            kwargs['products'] = products

        if const_id:
            warn(CONST_ID_DEPRECATION_MESSAGE, DeprecationWarning)

            if isinstance(const_id, string_types):
                const_id = [const_id]

            kwargs['const_id'] = const_id

        if geom:
            kwargs['geom'] = geom

        if start_time:
            kwargs['start_time'] = start_time

        if end_time:
            kwargs['end_time'] = end_time

        if cloud_fraction is not None:
            kwargs['cloud_fraction'] = cloud_fraction

        if cloud_fraction_0 is not None:
            kwargs['cloud_fraction_0'] = cloud_fraction_0

        if fill_fraction is not None:
            kwargs['fill_fraction'] = fill_fraction

        if params:
            kwargs['params'] = json.dumps(params)

        r = self.session.post('%s/search' % self.url, json=kwargs, timeout=self.TIMEOUT)

        features = r.json()

        result = {'type': 'FeatureCollection'}

        result['features'] = sorted(
            features, key=lambda f: f['properties']['acquired']
        )

        return result

    def keys(self, products=None, const_id=None, sat_id=None, date='acquired', place=None,
             geom=None, start_time=None, end_time=None, cloud_fraction=None,
             cloud_fraction_0=None, fill_fraction=None, params=None, limit=100,
             offset=0):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. Results are paged using limit/offset.

        :param list(str) products: Products identifier(s).
        :param list(str) const_id: Constellation identifier(s).
        :param list(str) sat_id: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_time: Desired starting date and time (inclusive).
        :param str end_time: Desired ending date and time (inclusive).
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param str params: JSON of additional query parameters.
        :param int limit: Number of items to return.
        :param int offset: Number of items to skip.

        :return: List of image identifiers.

        Example::

            >>> import descarteslabs as dl
            >>> keys = dl.metadata.keys(place='north-america_united-states_iowa', \
                                 products=['landsat:LC08:PRE:TOAR'], \
                                 start_time='2016-07-01', \
                                 end_time='2016-07-31T23:59:59')
            >>> len(keys)
            1

            >>> keys
            ['meta_LC80270312016188_v1']

        """
        result = self.search(sat_id=sat_id, products=products, const_id=const_id, date=date,
                             place=place, geom=geom, start_time=start_time,
                             end_time=end_time, cloud_fraction=cloud_fraction,
                             cloud_fraction_0=cloud_fraction_0, fill_fraction=fill_fraction,
                             params=params, limit=limit, offset=offset)

        return [feature['key'] for feature in result['features']]

    def features(self, products=None, const_id=None, sat_id=None, date='acquired', place=None,
                 geom=None, start_time=None, end_time=None, cloud_fraction=None,
                 cloud_fraction_0=None, fill_fraction=None, params=None,
                 limit=100):
        """Generator that combines summary and search to page through results.

        :param int limit: Number of features to fetch per request.

        :return: Generator of GeoJSON ``Feature`` objects.
        """
        summary = self.summary(sat_id=sat_id, products=products, const_id=None, date=date,
                               place=place, geom=geom, start_time=start_time,
                               end_time=end_time, cloud_fraction=cloud_fraction,
                               cloud_fraction_0=cloud_fraction_0, fill_fraction=fill_fraction,
                               params=params)

        offset = 0

        count = summary['count']

        while offset < count:

            features = self.search(sat_id=sat_id, products=products, const_id=None,
                                   date=date, place=place, geom=geom,
                                   start_time=start_time, end_time=end_time,
                                   cloud_fraction=cloud_fraction,
                                   cloud_fraction_0=cloud_fraction_0,
                                   fill_fraction=fill_fraction, params=params,
                                   limit=limit, offset=offset)

            offset = limit + offset

            for feature in features['features']:
                yield feature

    def get(self, key):
        """Get metadata of a single image.

        :param str key: Image identifier.

        Example::

            >>> import descarteslabs as dl
            >>> meta = dl.metadata.get('meta_LC80270312016188_v1')
            >>> keys = list(meta.keys())
            >>> keys.sort()
            >>> keys
            ['acquired', 'area', 'bits_per_pixel', 'bright_fraction', 'bucket', 'cloud_fraction', \
             'cloud_fraction_0', 'cs_code', 'descartes_version', 'file_md5s', 'file_sizes', 'files', \
             'fill_fraction', 'geolocation_accuracy', 'geometry', 'geotrans', 'identifier', 'key', 'processed', \
             'product', 'projcs', 'published', 'raster_size', 'reflectance_scale', 'roll_angle', 'sat_id', \
             'solar_azimuth_angle', 'solar_elevation_angle', 'sw_version', 'terrain_correction', 'tile_id']
        """
        r = self.session.get('%s/get/%s' % (self.url, key), timeout=self.TIMEOUT)

        return r.json()
