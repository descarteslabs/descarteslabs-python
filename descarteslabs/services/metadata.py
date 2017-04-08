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
from itertools import chain
from .service import Service
from .places import Places


class Metadata(Service):
    TIMEOUT = 360
    """Image Metadata Service https://iam.descarteslabs.com/service/runcible"""

    def __init__(self, url='https://platform-services.descarteslabs.com/runcible', token=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        Service.__init__(self, url, token)

    def sources(self):
        """Get a list of image sources.

        Example::

            >>> metadata.sources()

            [
                {'const_id': 'RE', 'sat_id': 'RE-2', 'value': 10},
                {'const_id': 'RE', 'sat_id': 'RE-5', 'value': 11},
                ...
            ]
        """
        r = self.session.get('%s/sources' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def summary(self, sat_id=None, const_id=None, date='acquired', part='day', place=None, geom=None, start_time=None, end_time=None,
                params=None, bbox=False):
        """Get a summary of the results for the specified spatio-temporal query.

        :param list(str) const_id: Constellation identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str part: Part of the date to aggregate over (e.g. `day`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_time: Desired starting date and time (inclusive).
        :param str end_time: Desired ending date and time (inclusive).
        :param bool bbox: If true, query by the bounding box of the region of interest.
        :param str params: JSON of additional query parameters.

        Example usage::

            >>> metadata.summary(place='north-america_united-states_iowa',
                    const_id=['L8'], part='year')

            [{'bytes': 187707322653,
                'const_id': 'L8',
                'count': 1509,
                'items': [{'bytes': 31765777640,
                    'count': 272,
                    'date': '2013-01-01T00:00:00',
                    'pixels': 65348414912},
                {'bytes': 48481543735,
                    'count': 385,
                    'date': '2014-01-01T00:00:00',
                    'pixels': 94197206464},
                {'bytes': 47936802649,
                    'count': 387,
                    'date': '2015-01-01T00:00:00',
                    'pixels': 94688657728},
                {'bytes': 49918583944,
                    'count': 391,
                    'date': '2016-01-01T00:00:00',
                    'pixels': 95671624320},
                {'bytes': 9604614685,
                    'count': 74,
                    'date': '2017-01-01T00:00:00',
                    'pixels': 18087390976}],
                'pixels': 367993294400}]
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
            
            if isinstance(sat_id, basestring):
                sat_id = [sat_id]
                
            kwargs['sat_id'] = sat_id
            
        if const_id:

            if isinstance(const_id, basestring):
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

        if params:
            kwargs['params'] = json.dumps(params)

        if bbox:
            kwargs['bbox'] = bbox

        r = self.session.post('%s/summary' % self.url, json=kwargs, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def search(self, sat_id=None, const_id=None, date='acquired', place=None, geom=None, start_time=None, end_time=None, params=None,
               limit=100, offset=0, bbox=True):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. Results are paged using limit/offset.

        :param list(str) const_id: Constellation identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_time: Desired starting date and time (inclusive).
        :param str end_time: Desired ending date and time (inclusive).
        :param bool bbox: If true, query by the bounding box of the region of interest.
        :param str params: JSON of additional query parameters.
        :param int limit: Number of items to return.
        :param int offset: Number of items to skip.

        return: GeoJSON ``FeatureCollection``

        Example::

            >>> scenes = metadata.search(place='north-america_united-states_iowa', const_id=['L8'],
                    start_time='2016-07-01', end_time='2016-07-31 23:59:59')
                len(scenes['features'])

            34
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
            
            if isinstance(sat_id, basestring):
                sat_id = [sat_id]
                
            kwargs['sat_id'] = sat_id
            
        if const_id:

            if isinstance(const_id, basestring):
                const_id = [const_id]

            kwargs['const_id'] = const_id

        if geom:
            kwargs['geom'] = geom

        if start_time:
            kwargs['start_time'] = start_time

        if end_time:
            kwargs['end_time'] = end_time

        if params:
            kwargs['params'] = json.dumps(params)

        if bbox:
            kwargs['bbox'] = bbox
            
        r = self.session.post('%s/search' % self.url, json=kwargs, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        features = r.json()

        result = {'type': 'FeatureCollection'}

        result['features'] = features[:limit]

        return result

    def keys(self, sat_id=None, const_id=None, date='acquired', place=None, geom=None, start_time=None, end_time=None, params=None,
             limit=100, offset=0, bbox=False):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. Results are paged using limit/offset.

        :param list(str) const_id: Constellation identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_time: Desired starting date and time (inclusive).
        :param str end_time: Desired ending date and time (inclusive).
        :param bool bbox: If true, query by the bounding box of the region of interest.
        :param str params: JSON of additional query parameters.
        :param int limit: Number of items to return.
        :param int offset: Number of items to skip.

        :return: List of image identifiers.

        Example::

            >>> metadata.keys(place='north-america_united-states_iowa', const_id=['L8'],
                    start_time='2016-07-01', end_time='2016-07-31 23:59:59')

            [
                'meta_LC80260322016213_v1',
                'meta_LC80260312016213_v1',
                'meta_LC80260302016213_v1',
                ...
            ]
        """
        result = self.search(sat_id, const_id, date, place, geom, start_time, end_time, params, limit, offset, bbox)

        return [feature['id'] for feature in result['features'][:limit]]

    def features(self, sat_id=None, const_id=None, date='acquired', place=None, geom=None, start_time=None, end_time=None,
                 params=None, limit=100, bbox=False):
        """Generator that combines summary and search to page through results.

        :param int limit: Number of features to fetch per request.

        :return: Generator of GeoJSON ``Feature`` objects.
        """
        result = self.summary(sat_id=sat_id, const_id=const_id, date=date, place=place, geom=geom, start_time=start_time,
                              end_time=end_time, params=params, bbox=bbox)

        for summary in result:

            offset = 0

            count = summary['count']
            
            while offset < count:

                features = self.search(sat_id=sat_id, const_id=const_id, date=date, place=place, geom=geom, start_time=start_time,
                                       end_time=end_time, params=params, limit=limit, offset=offset, bbox=bbox)

                offset = limit + offset

                for feature in features['features']:
                    yield feature

    def get(self, key):
        """Get metadata of a single image.

        :param str key: Image identifier.

        Example::

            >>> metadata.get('meta_LC82000452016168_v1')

            {
                'acquired': '2016-06-16T10:54:16.400121Z',
                'area': 35518.2,
                'bits_per_pixel': [0.804, 0.803, 0.636],
                ...
            }
        """
        r = self.session.get('%s/get/%s' % (self.url, key), timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()
