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

import operator
from functools import partial
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey

from .service import Service


class Places(Service):
    TIMEOUT = 120
    """Places and statistics service https://iam.descarteslabs.com/service/waldo"""

    def __init__(self, url='https://platform-services.descarteslabs.com/waldo', token=None, maxsize=10, ttl=600):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        Service.__init__(self, url, token)
        self.cache = TTLCache(maxsize, ttl)

    def placetypes(self):
        """Get a list of known/available placetypes

        return: array
          [
            "string"
          ]

        >>> places.placetypes()
        """
        r = self.session.get('%s/placetypes' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'find'))
    def find(self, path, **kwargs):
        """Find candidate slugs based on full or partial path

        path: string
            Candidate underscore separated slug
        placetype: string
            Restrict results to a particular placetype (optional)

        return: array
          [
            {
              "placetype": "string",
              "path": "string",
              "slug": "string",
              "id":	"integer",
              "name": "string",
              "bbox": [
                0
              ]
            }
          ]

        >>> places.find('iowa_united-states')
        """
        r = self.session.get('%s/find/%s' % (self.url, path), params=kwargs, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'shape'))
    def shape(self, slug, output='geojson', geom='low'):
        """Get the shape for a specified slug

        slug: string
            Unique slug for the shape
        output: string
            Return type: geojson (default)
        geom: string
            Resolution for the shape [low (default), medium, high]

        return: geojson

        >>> places.shape('north-america_united-states_iowa', geom='high')
        """
        r = self.session.get('%s/shape/%s.%s' % (self.url, slug, output), params={'geom': geom}, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'prefix'))
    def prefix(self, slug, output='geojson', placetype=None, geom='low'):
        """Get all the places that start with a prefix

        slug: string
            Unique slug for the shape
        placetype: string
            Restrict results to a particular placetype (optional)
        geom: string
            Include the geometry for each shape using the specified resolution
            (low, medium, high). Default is None, meaning no geometry.

        return: geojson|topojson

        >>> places.prefix('north-america_united-states_iowa', placetype='district', geom='low')

        """
        params = {}
        if placetype:
            params['placetype'] = placetype
        params['geom'] = geom
        r = self.session.get('%s/prefix/%s.%s' % (self.url, slug, output),
                             params=params, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def sources(self):
        """Get a list of models (sources)

        return: array
        [
          {
            "model": "string",
            "pk": 0,
            "fields": {
              "name": "string",
              "label": "string",
              "properties": "string"
            }
          }
        ]

        >>> places.sources()
        """
        r = self.session.get('%s/sources' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def categories(self):
        """Get a list of categories

        return: array
        [
          {
            "model": "string",
            "pk": 0,
            "fields": {
              "created": "string",
              "modified": "string",
              "name": "string"
            }
          }
        ]

        >>> places.categories()
        """
        r = self.session.get('%s/categories' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def metrics(self):
        """Get a list of metrics

        return: array
        [
          {
            "model": "string",
            "pk": 0,
            "fields": {
              "created": "string",
              "modified": "string",
              "name": "string",
              "units": "string"
            }
          }
        ]

        >>> places.metrics()
        """
        r = self.session.get('%s/metrics' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def triples(self):
        """Get a list of triples

        return: array
        [
          {
            "model": "string",
            "pk": 0,
            "fields": {
              "created": "string",
              "modified": "string",
              "name": "string",
              "source": 0,
              "category": 0,
              "metric": 0
            }
          }
        ]

        >>> places.triples()
        """
        r = self.session.get('%s/triples' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'data'))
    def data(self, slug, **kwargs):
        """Get a list of statistics

        source: string
            Source
        category: string
            Category
        metric: string
            Metric
        slug: string
            Shape slug
        year: integer
            Year
        doy: integer
            Day of year

        return: array
        [
          {
            "shape": 0,
            "value": 0
          }
        ]

        >>> places.data('north-america_united-states', placetype='county', source='nass', category='corn',
                       metric='yield', year=2015, doy=1)
        """
        r = self.session.get('%s/data/%s' % (self.url, slug), params=kwargs, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'statistics'))
    def statistics(self, slug, **kwargs):
        """Get a list of statistics for a specific shape

        slug: string
            Shape slug
        source: string
            Source
        category: string
            Category
        metric: string
            Metric
        year: integer
            Year
        doy: integer
            Day of year

        return: object
        {
            "count": 0,
            "items": [
                {
                  "id": 0,
                  "slug": "string",
                  "date": "string",
                  "year": 0,
                  "doy": 0,
                  "source": "string",
                  "category": "string",
                  "metric": "string",
                  "value": 0,
                  "backfill": true,
                }
            ]
          }
        ]

        >>> places.statistics('north-america_united-states_iowa', source='nass', category='corn', metric='yield',
                              year=2015)
        """
        r = self.session.get('%s/statistics/%s' % (self.url, slug), params=kwargs, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()
