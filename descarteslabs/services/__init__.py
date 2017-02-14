import descarteslabs.cli_auth
import requests
import operator
import random
import json
import os

from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from itertools import chain
from functools import partial
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey

class Service:

    def __init__(self, url):
        self.auth = descarteslabs.cli_auth.Auth()
        self.url = url

    @property
    def token(self):
        return self.auth.token

    @property
    def session(self):

        s = requests.Session()

        retries = Retry(total=5,
                        backoff_factor=random.uniform(1, 10),
                        status_forcelist=[500, 502, 503, 504])

        s.mount('http://', HTTPAdapter(max_retries=retries))

        s.headers.update({"Authorization": self.token})
        s.headers.update({"content-type": "application/json"})

        here = os.path.dirname(__file__)

        try:
            file = os.path.join(here, 'gd_bundle-g2-g1.crt')
            with open(file):
                s.verify = file
        except:
            s.verify = False

        return s

class Runcible(Service):

    """Image Metadata Service https://iam.descarteslabs.com/service/runcible"""

    def __init__(self, url='https://services-dev.descarteslabs.com/runcible/v2'):
        """The parent Service class implements authentication and exponential 
        backoff/retry. Override the url parameter to use a different instance 
        of the backing service.
        """
        Service.__init__(self, url)

    def sources(self):
        """
        Get a list of Image sources

        return: list
        [
            {
              "sat_id": "string",
              "const_id": "string",
              "value": "integer"
            }
        ]

        >>> runcible.sources()
        """
        r = self.session.get('%s/sources' % self.url)

        return r.json()

    def features(self, const_id=[], shape=None, geom=None, start_time=None, end_time=None, params=None, limit=100):
        """
        Generator that combines summary and search to page through results.

        limit: integer
            Specify a page size

        return: GeoJSON Feature(s)

        >>> for feature in runcible.features():
            ...
        """
        result = self.summary(const_id=const_id, shape=shape, geom=geom, start_time=start_time, end_time=end_time, params=params)

        for summary in result:

            offset = 0

            count = summary['count']
            const_id = summary['const_id']

            while offset < count:

                features = self.search(const_id=[const_id], shape=shape, geom=geom, start_time=start_time, end_time=end_time, params=params, limit=limit, offset=offset)

                offset = limit + offset

                for feature in features['features']:
                    yield feature

    def search(self, const_id=[], shape=None, geom=None, start_time=None, end_time=None, params=None, limit=100, offset=0):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. Results are paged using limit/offset.

        shape: string
            An (optional) shape name to use in the spatial filter
        sat_id: list(string)
            Satellite identifier(s)
        const_id: list(string)
            Constellation identifier(s)
        start_time: string
            Start of valid date/time range (inclusive)
        end_time: string
            End of valid date/time range (inclusive)
        geom: string
            Region of interest as GeoJSON or WKT
        params: string
            JSON String of additional key/value pairs for searching properties: tile_id, cloud_fraction, etc.
        limit: integer
            Number of items to return (default 100)
        offset: integer
            Number of items to skip (default 0)
        bbox: boolean
            Whether or not to use a bounding box filter (default: false)

        return: GeoJSON FeatureCollection

        >>> runcible.search(shape='north-america_united-states_iowa', const_id=['L8'])
        """
        if shape:

            waldo = Waldo()

            shape = waldo.shape(shape, geom='low')

            geom = json.dumps(shape['geometry'])

        def f(x):

            kwargs = {}

            kwargs['const_id'] = x
            kwargs['limit'] = limit
            kwargs['offset'] = offset

            if geom:
                kwargs['geom'] = geom

            if start_time:
                kwargs['start_time'] = start_time

            if end_time:
                kwargs['end_time'] = end_time

            if params:
                kwargs['params'] = json.dumps(params)

            r = self.session.post('%s/search' % self.url, json=kwargs)

            return r.json()

        result = {'type':'FeatureCollection'}

        result['features'] = list(chain(*map(f, const_id)))

        return result

    def summary(self, const_id=[], date='acquired', shape=None, geom=None, start_time=None, end_time=None, params=None):
        """Get a summary of results for the specified spatio-temporal query.

        shape: string
            An (optional) shape name to use in the spatial filter
        sat_id: list(string)
            Satellite identifier(s)
        const_id: list(string)
            Constellation identifier(s)
        start_time: string
            Start of valid date/time range (inclusive)
        end_time: string
            End of valid date/time range (inclusive)
        geom: string
            Region of interest as GeoJSON or WKT
        params: string
            JSON String of additional key/value pairs for searching properties
        geom: string
            Region of interest as GeoJSON or WKT

        return: dict
        {
          "count": 0,
          "items: [
            {
              "date": "2016-11-08",
              "n": 0
            }
          ]
        }

        >>> runcible.summary(shape='north-america_united-states_iowa', const_id=['L8'])
        """
        if shape:

            waldo = Waldo()

            shape = waldo.shape(shape, geom='low')

            geom = json.dumps(shape['geometry'])

        def f(x):

            kwargs = {}

            kwargs = {}

            kwargs['const_id'] = x

            if geom:
                kwargs['geom'] = geom

            if start_time:
                kwargs['start_time'] = start_time

            if end_time:
                kwargs['end_time'] = end_time

            if params:
                kwargs['params'] = json.dumps(params)

            r = self.session.post('%s/summary' % self.url, json=kwargs)

            return r.json()

        result = map(f, const_id) 

        return result

    def get(self, key):

        r = self.session.post('%s/get/%s' % (self.url, key))

        return r.json()

    def post(self, key, value):

        r = self.session.post('%s/post/%s' % (self.url, key), data=value)

        return r.json()

class Waldo(Service):

    """Shapes and statistics service https://iam.descarteslabs.com/service/waldo"""

    def __init__(self, url='https://services.descarteslabs.com/waldo/v1', maxsize=10, ttl=600):
        """The parent Service class implements authentication and exponential 
        backoff/retry. Override the url parameter to use a different instance 
        of the backing service.
        """
        Service.__init__(self, url)
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
