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

import operator
import os
from functools import partial
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey
from descarteslabs.client.services.service import Service
from descarteslabs.client.auth import Auth
from six import string_types
from descarteslabs.common.dotdict import DotDict, DotList


class Places(Service):
    TIMEOUT = (9.5, 360)
    """Places and statistics service"""

    def __init__(self, url=None, auth=None, maxsize=10, ttl=600):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        if auth is None:
            auth = Auth()

        if url is None:
            url = os.environ.get("DESCARTESLABS_PLACES_URL", "https://platform.descarteslabs.com/waldo/v2")

        super(Places, self).__init__(url, auth=auth)
        self.cache = TTLCache(maxsize, ttl)

    def placetypes(self):
        """Get a list of place types.

        return: list
            List of placetypes ['continent', 'country', 'dependency', 'macroregion', 'region',
                                'district', 'mesoregion', 'microregion', 'county', 'locality']
        """
        r = self.session.get('/placetypes')
        return r.json()

    def random(self, geom='low', placetype=None):
        """Get a random location

        geom: string
            Resolution for the shape [low (default), medium, high]

        return: geojson
        """
        params = {}

        if geom:
            params['geom'] = geom

        if placetype:
            params['placetype'] = placetype

        r = self.session.get('/random', params=params)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return DotDict(r.json())

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'find'))
    def find(self, path, **kwargs):
        """Find candidate slugs based on full or partial path.

        :param str path: Candidate underscore-separated slug.
        :param placetype: Optional place type for filtering.

        Example::

            >>> from descarteslabs.client.services import Places
            >>> results = Places().find('morocco')
            >>> _ = results[0].pop('bbox')
            >>> results
            [
              {
                'id': 85632693,
                'name': 'Morocco',
                'path': 'continent:africa_country:morocco',
                'placetype': 'country',
                'slug': 'africa_morocco'
              }
            ]
        """
        r = self.session.get('/find/%s' % path, params=kwargs)
        return DotList(r.json())

    def search(self, q, limit=10, country=None, region=None, placetype=None):
        """Search for shapes

        :param str q: A query string.
        :param int limit: Max number of matches to return
        :param str country: Restrict search to a specific country
        :param str region: Restrict search to a specific region
        :param str placetype: Restrict search to a specific placetype

        :return: list of candidates

        Example::
            >>> from descarteslabs.client.services import Places
            >>> results = Places().search('texas')
            >>> results[0]
            {
              'bbox': [-106.645584, 25.837395, -93.508039, 36.50035],
              'id': 85688753,
              'name': 'Texas',
              'placetype': 'region',
              'slug': 'north-america_united-states_texas'
            }
        """
        params = {}

        if q:
            params['q'] = q

        if country:
            params['country'] = country

        if region:
            params['region'] = region

        if placetype:
            params['placetype'] = placetype

        if limit:
            params['n'] = limit

        r = self.session.get('/search', params=params, timeout=self.TIMEOUT)

        return DotList(r.json())

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'shape'))
    def shape(self, slug, output='geojson', geom='low'):
        """Get the geometry for a specific slug

        :param slug: Slug identifier.
        :param str output: Desired geometry format (`GeoJSON`).
        :param str geom: Desired resolution for the geometry (`low`, `medium`, `high`).

        :return: GeoJSON ``Feature``

        Example::
            >>> from descarteslabs.client.services import Places
            >>> kansas = Places().shape('north-america_united-states_kansas')
            >>> kansas['bbox']
            [-102.051744, 36.993016, -94.588658, 40.003078]

            >>> kansas['geometry']['type']
            'Polygon'

            >>> kansas['properties']
            {
              'name': 'Kansas',
              'parent_id': 85633793,
              'path': 'continent:north-america_country:united-states_region:kansas',
              'placetype': 'region',
              'slug': 'north-america_united-states_kansas'
            }

        """
        params = {}

        params['geom'] = geom
        r = self.session.get('/shape/%s.%s' % (slug, output), params=params)
        return DotDict(r.json())

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'prefix'))
    def prefix(self, slug, output='geojson', placetype=None, geom='low'):
        """Get all the places that start with a prefix

        :param str slug: Slug identifier.
        :param str output: Desired geometry format (`GeoJSON`, `TopoJSON`).
        :param str placetype: Restrict results to a particular place type.
        :param str geom: Desired resolution for the geometry (`low`, `medium`, `high`).

        :return: GeoJSON or TopoJSON ``FeatureCollection``

        Example::
            >>> from descarteslabs.client.services import Places
            >>> il_counties = Places().prefix('north-america_united-states_illinois', placetype='county')
            >>> len(il_counties['features'])
            102

        """
        params = {}

        if placetype:
            params['placetype'] = placetype
        params['geom'] = geom
        r = self.session.get('/prefix/%s.%s' % (slug, output), params=params)

        return DotDict(r.json())

    def sources(self):
        """Get a list of sources
        """
        r = self.session.get('/sources', timeout=self.TIMEOUT)

        return DotList(r.json())

    def categories(self):
        """Get a list of categories
        """
        r = self.session.get('/categories', timeout=self.TIMEOUT)

        return DotList(r.json())

    def metrics(self):
        """Get a list of metrics
        """
        r = self.session.get('/metrics', timeout=self.TIMEOUT)

        return DotList(r.json())

    def data(self, slug, source=None, category=None, metric=None, units=None, date=None, placetype='county'):
        """Get all values for a prefix search and point in time

        :param str slug: Slug identifier (or shape id).
        :param str source: Source
        :param str category: Category
        :param str metric: Metric
        :param str units: Units
        :param str date: Date
        :param str placetype: Restrict results to a particular place type.

        """
        params = {}

        if source:
            params['source'] = source

        if category:
            params['category'] = category

        if metric:
            params['metric'] = metric

        if units:
            params['units'] = units

        if date:
            params['date'] = date

        if placetype:
            params['placetype'] = placetype

        r = self.session.get('/data/%s' % (slug),
                             params=params, timeout=self.TIMEOUT)

        return r.json()

    def statistics(self, slug, source=None, category=None, metric=None, units=None):
        """Get a time series for a specific place

        :param str slug: Slug identifier (or shape id).
        :param str slug: Slug identifier (or shape id).
        :param str source: Source
        :param str category: Category
        :param str metric: Metric
        :param str units: Units

        """
        params = {}

        if source:
            params['source'] = source

        if category:
            params['category'] = category

        if metric:
            params['metric'] = metric

        if units:
            params['units'] = units

        r = self.session.get('/statistics/%s' % (slug),
                             params=params, timeout=self.TIMEOUT)

        return r.json()

    def value(self, slug, source=None, category=None, metric=None, date=None):
        """Get point values for a specific place

        :param str slug: Slug identifier (or shape id).
        :param list(str) source: Source(s)
        :param list(str) category: Category(s)
        :param list(str) metric: Metric(s)
        :param str date: Date
        """
        params = {}

        if source:

            if isinstance(source, string_types):
                source = [source]

            params['source'] = source

        if category:

            if isinstance(category, string_types):
                category = [category]

            params['category'] = category

        if metric:

            if isinstance(metric, string_types):
                metric = [metric]

            params['metric'] = metric

        if date:
            params['date'] = date

        r = self.session.get('/value/%s' % (slug),
                             params=params, timeout=self.TIMEOUT)

        return r.json()
