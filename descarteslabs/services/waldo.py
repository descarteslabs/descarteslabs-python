"""
"""
import operator
from functools import partial
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey

from .service import Service


class Waldo(Service):

    """Shapes and statistics service https://iam.descarteslabs.com/service/waldo"""

    def __init__(self, url='https://services.descarteslabs.com/waldo/v1', token=None, maxsize=10, ttl=600):
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

        >>> waldo.placetypes()
        """
        r = self.session.get('%s/placetypes' % self.url)

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

        >>> waldo.find('iowa_united-states')
        """
        r = self.session.get('%s/find/%s' % (self.url, path), params=kwargs)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'shape'))
    def shape(self, slug, output='geojson', geom=None):
        """Get the shape for a specified slug

        slug: string
            Unique slug for the shape
        output: string
            Return type: geojson (default)
        geom: string
            Resolution for the shape [low (default), medium, high]

        return: geojson

        >>> waldo.shape('north-america_united-states_iowa', geom='high')
        """
        r = self.session.get('%s/shape/%s.%s' % (self.url, slug, output), params={'geom':geom})

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'prefix'))
    def prefix(self, slug, output='geojson', placetype='county', geom='low'):
        """Get all the shapes that start with a prefix

        slug: string
            Unique slug for the shape
        placetype: string
            Restrict results to a particular placetype (optional)
        geom: string
            Include the geometry for each shape using the specified resolution
            (low, medium, high). Default is None, meaning no geometry.

        return: geojson|topojson

        >>> waldo.prefix('north-america_united-states_iowa', placetype='district', geom='low')

        """
        r = self.session.get('%s/prefix/%s.%s' % (self.url, slug, output), params={'placetype': placetype, 'geom': geom})

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

        >>> waldo.sources()
        """
        r = self.session.get('%s/sources' % self.url)

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

        >>> waldo.categories()
        """
        r = self.session.get('%s/categories' % self.url)

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

        >>> waldo.metrics()
        """
        r = self.session.get('%s/metrics' % self.url)

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

        >>> waldo.triples()
        """
        r = self.session.get('%s/triples' % self.url)

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

        >>> waldo.data('north-america_united-states', placetype='county', source='nass', category='corn', metric='yield', year=2015, doy=1)
        """
        r = self.session.get('%s/data/%s' % (self.url, slug), params=kwargs)

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

        >>> waldo.statistics('north-america_united-states_iowa', source='nass', category='corn', metric='yield', year=2015)
        """
        r = self.session.get('%s/statistics/%s' % (self.url, slug), params=kwargs)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()
