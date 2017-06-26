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
import descarteslabs as dl

CONST_ID_DEPRECATION_MESSAGE = (
    "Keyword arg `const_id' has been deprecated and will be removed in "
    "future versions of this software. Use the `products` "
    "argument instead. Product identifiers can be found with the "
    " products() method."
)


class Metadata(Service):
    TIMEOUT = (9.5, 120)
    """Image Metadata Service"""

    def __init__(self, url=None, token=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        simplefilter('always', DeprecationWarning)
        if url is None:
            url = os.environ.get("DESCARTESLABS_METADATA_URL",
                                 "https://platform-services.descarteslabs.com/metadata/v1")

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

    def bands(self, limit=None, offset=None, wavelength=None, resolution=None, tags=None):
        """Seach for imagery data bands that you have access to.

        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.
        :param float wavelenth: A wavelength in nm e.g 700 that the band sensor must measure.
        :param int resolution: The resolution in meters per pixel e.g 30 of the data available in this band.
        :param list(str) tags: A list of tags that the band must have in its own tag list.


        """
        params = ['limit', 'offset', 'wavelength', 'resolution', 'tags']

        args = locals()
        kwargs = {
            param: args[param]
            for param in params
            if args[param] is not None
        }
        r = self.session.post('%s/bands/search' % self.url, json=kwargs, timeout=self.TIMEOUT)

        return r.json()

    def products(self, band=None, limit=None, offset=None):
        """Search products that are available on the platform.

        :param list(str) band: Band name e.g "red" to filter products by.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.

        Example::
            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> products = dl.metadata.products(limit=1)
            >>> pprint(products)
                [{'Orbit': 'sun-synchronous',
                    'Spectral Bands': '6',
                    'description': 'Landsat 5 thematic mapper imagery, processed by Descartes '
                                'Labs into Top-of-atmosphere-reflectance.\\n'
                                '\\n'
                                'Landsat 5 was a low Earth orbit satellite launched on March '
                                '1, 1984 to collect imagery of the surface of Earth. A '
                                'continuation of the Landsat Program, Landsat 5 was jointly '
                                'managed by the U.S. Geological Survey (USGS) and the '
                                'National Aeronautics and Space Administration (NASA). Data '
                                "from Landsat 5 was collected and distributed from the USGS's "
                                'Center for Earth Resources Observation and Science (EROS).\\n'
                                'After 29 years in space, Landsat 5 was officially '
                                'decommissioned on June 5, 2013.[2] Near the end of its '
                                "mission, Landsat 5's use was hampered by equipment failures, "
                                'and it was largely superseded by Landsat 7 and Landsat 8 '
                                'Mission scientists anticipated the satellite will re-enter '
                                "Earth's atmosphere and disintegrate around 2034.\\n"
                                ' \\n'
                                'This information uses material from the Wikipedia article <a '
                                'href="https://en.wikipedia.org/wiki/Landsat_5">"Landsat_5"</a>, '
                                'which is released under the <a '
                                'href="https://creativecommons.org/licenses/by-sa/3.0/">Creative '
                                'Commons Attribution-Share-Alike License 3.0</a>.\\n',
                    'end_date': '11/1/2011',
                    'native_bands': ['red', 'green', 'blue', 'nir', 'swir1', 'swir2'],
                    'orbit': 'sun-synchronous',
                    'processing_level': 'TOAR',
                    'product': 'landsat:LT05:PRE:TOAR',
                    'resolution': 30,
                    'revisit': 16,
                    'sensor': 'Thematic Mapper (TM)',
                    'spectral bands': 6,
                    'start_date': '3/1/1984',
                    'swath': '185km',
                    'title': 'Landsat 5'}]
        """
        params = ['limit', 'offset', 'band']

        args = locals()
        kwargs = {
            param: args[param]
            for param in params
            if args[param] is not None
        }

        r = self.session.post('%s/products/search' % self.url, json=kwargs, timeout=self.TIMEOUT)

        return r.json()

    def available_products(self):
        """Get the list of product identifiers you have access to.

        Example::
            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> products = dl.metadata.available_products()
            >>> pprint(products)
            ['landsat:LC08:PRE:TOAR']

        """
        r = self.session.get('%s/products' % self.url, timeout=self.TIMEOUT)

        return r.json()

    def summary(self, products=None, const_id=None, sat_id=None, date='acquired', part=None,
                place=None, geom=None, start_time=None, end_time=None, cloud_fraction=None,
                cloud_fraction_0=None, fill_fraction=None, pixels=None, params=None,
                dltile=None):
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
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.

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

        if dltile is not None:
            if isinstance(dltile, string_types):
                dltile = dl.raster.dltile(dltile)
            if isinstance(dltile, dict):
                geom = dltile['geometry']

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
               limit=100, offset=0, fields=None, dltile=None, sort_field=None, sort_order="asc"):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. Results are paged using limit and offset. Please note offset
        plus limit cannot exceed 10000.

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
        :param int limit: Number of items to return. (max of 10000)
        :param int offset: Number of items to skip.
        :param list(str) fields: Properties to return.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.

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

        if dltile is not None:
            if isinstance(dltile, string_types):
                dltile = dl.raster.dltile(dltile)
            if isinstance(dltile, dict):
                geom = dltile['geometry']

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        kwargs = {'date': date, 'limit': limit, 'offset': offset}

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

        if fields is not None:
            kwargs['fields'] = fields

        if sort_field is not None:
            kwargs['sort_field'] = sort_field

            if sort_order is not None:
                kwargs['sort_order'] = sort_order

        r = self.session.post('%s/search' % self.url, json=kwargs, timeout=self.TIMEOUT)

        return {'type': 'FeatureCollection', "features": r.json()}

    def ids(self, products=None, const_id=None, sat_id=None, date='acquired', place=None,
            geom=None, start_time=None, end_time=None, cloud_fraction=None,
            cloud_fraction_0=None, fill_fraction=None, params=None, limit=100,
            offset=0, dltile=None, sort_field=None, sort_order='asc'):
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
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.

        :return: List of image identifiers.

        Example::

            >>> import descarteslabs as dl
            >>> ids = dl.metadata.ids(place='north-america_united-states_iowa', \
                                 products=['landsat:LC08:PRE:TOAR'], \
                                 start_time='2016-07-01', \
                                 end_time='2016-07-31T23:59:59')
            >>> len(ids)
            1

            >>> ids
            ['landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1']

        """
        result = self.search(sat_id=sat_id, products=products, const_id=const_id, date=date,
                             place=place, geom=geom, start_time=start_time,
                             end_time=end_time, cloud_fraction=cloud_fraction,
                             cloud_fraction_0=cloud_fraction_0, fill_fraction=fill_fraction,
                             params=params, limit=limit, offset=offset, fields=[], dltile=dltile,
                             sort_field=sort_field, sort_order=sort_order)

        return [feature['id'] for feature in result['features']]

    def keys(self, products=None, const_id=None, sat_id=None, date='acquired', place=None,
             geom=None, start_time=None, end_time=None, cloud_fraction=None,
             cloud_fraction_0=None, fill_fraction=None, params=None, limit=100,
             offset=0, dltile=None, sort_field=None, sort_order='asc'):
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
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.

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
                             params=params, limit=limit, offset=offset, fields=["key"], dltile=dltile,
                             sort_field=sort_field, sort_order=sort_order)

        return [feature['key'] for feature in result['features']]

    def features(self, products=None, const_id=None, sat_id=None, date='acquired', place=None,
                 geom=None, start_time=None, end_time=None, cloud_fraction=None,
                 cloud_fraction_0=None, fill_fraction=None, params=None,
                 limit=100, dltile=None, sort_field=None, sort_order='asc'):

        """Generator that combines summary and search to page through results.

        :param int limit: Number of features to fetch per request.

        :return: Generator of GeoJSON ``Feature`` objects.
        """
        summary = self.summary(sat_id=sat_id, products=products, const_id=None, date=date,
                               place=place, geom=geom, start_time=start_time,
                               end_time=end_time, cloud_fraction=cloud_fraction,
                               cloud_fraction_0=cloud_fraction_0, fill_fraction=fill_fraction,
                               params=params, dltile=dltile)

        offset = 0

        count = summary['count']

        while offset < count:

            features = self.search(sat_id=sat_id, products=products, const_id=None,
                                   date=date, place=place, geom=geom,
                                   start_time=start_time, end_time=end_time,
                                   cloud_fraction=cloud_fraction,
                                   cloud_fraction_0=cloud_fraction_0,
                                   fill_fraction=fill_fraction, params=params,
                                   limit=limit, offset=offset,
                                   dltile=dltile, sort_field=sort_field,
                                   sort_order=sort_order)

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
            ['acquired', 'area', 'bits_per_pixel', 'bright_fraction', 'bucket', 'cloud_fraction',
             'cloud_fraction_0', 'cs_code', 'descartes_version', 'file_md5s', 'file_sizes', 'files',
             'fill_fraction', 'geolocation_accuracy', 'geometry', 'geotrans', 'id', 'identifier', 'key',
             'processed', 'product', 'projcs', 'published', 'raster_size', 'reflectance_scale', 'roll_angle',
             'sat_id', 'solar_azimuth_angle', 'solar_elevation_angle', 'sw_version', 'terrain_correction',
             'tile_id']
        """
        r = self.session.get('%s/get/%s' % (self.url, key), timeout=self.TIMEOUT)

        return r.json()

    def get_product(self, product_id):
        """Get information about a single product.

        :param str product_id: Product Identifier.

        Example::

            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> product = dl.metadata.get_product('landsat:LC08:PRE:TOAR')
            >>> pprint(product)
            {'Orbit': 'sun-synchronous',
                'Spectral Bands': '7',
                'description': 'Landsat 8 Operational Land Imager imagery, processed by '
                                'Descartes Labs into Top-of-atmosphere-reflectance. Red, '
                                'green, and blue bands are pansharpened to 15 meter resolution '
                                'using the panchromatic band.\\n'
                                ' \\n'
                                'Landsat 8 is an American Earth observation satellite launched '
                                'on February 11, 2013. It is the eighth satellite in the '
                                'Landsat program; the seventh to reach orbit successfully. '
                                'Originally called the Landsat Data Continuity Mission (LDCM), '
                                'it is a collaboration between NASA and the United States '
                                'Geological Survey(USGS). NASA Goddard Space Flight Center in '
                                'Greenbelt, Maryland, provided development, mission systems '
                                'engineering, and acquisition of the launch vehicle while the '
                                'USGS provided for development of the ground systems and will '
                                'conduct on-going mission operations.\\n'
                                'The satellite was built by Orbital Sciences Corporation, who '
                                'served as prime contractor for the mission.[3] The '
                                "spacecraft's instruments were constructed by Ball Aerospace "
                                "and NASA's Goddard Space Flight Center,[4] and its launch was "
                                'contracted to United Launch Alliance.[5] During the first 108 '
                                'days in orbit, LDCM underwent checkout and verification by '
                                'NASA and on 30 May 2013 operations were transferred from NASA '
                                'to the USGS when LDCM was officially renamed to Landsat '
                                '8.[6]\\n'
                                'This information uses material from the Wikipedia article <a '
                                'href="https://en.wikipedia.org/wiki/Landsat_8">"Landsat_8"</a>, '
                                'which is released under the <a '
                                'href="https://creativecommons.org/licenses/by-sa/3.0/">Creative '
                                'Commons Attribution-Share-Alike License 3.0</a>.\\n',
                'end_date': 'present',
                'id': 'landsat:LC08:PRE:TOAR',
                'native_bands': ['coastal-aerosol',
                                'red',
                                'green',
                                'blue',
                                'nir',
                                'swir1',
                                'swir2',
                                'tirs1',
                                'cirrus',
                                'qa_water',
                                'qa_snow',
                                'qa_cloud',
                                'qa_cirrus'],
                'notes': 'Red, green, and blue bands have been pansharpened to 15m resolution',
                'orbit': 'sun-synchronous',
                'processing_level': 'TOAR',
                'product': 'landsat:LC08:PRE:TOAR',
                'resolution': 15,
                'revisit': 16,
                'sensor': 'Operational Land Imager (OLI), Thermal Infrared Sensor (TIRS)',
                'spectral bands': 7,
                'start_date': '2/1/2013',
                'swath': '185km',
                'title': 'Landsat 8'}

        """
        r = self.session.get('%s/products/%s' % (self.url, product_id), timeout=self.TIMEOUT)

        return r.json()

    def get_band(self, band_id):
        """Get information about a single product.

        :param str band_id: Band Identifier.

        """
        r = self.session.get('%s/bands/%s' % (self.url, band_id), timeout=self.TIMEOUT)

        return r.json()
