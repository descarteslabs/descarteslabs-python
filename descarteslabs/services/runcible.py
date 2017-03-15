import json
from itertools import chain
from .service import Service
from .waldo import Waldo

class Runcible(Service):

    """Image Metadata Service https://iam.descarteslabs.com/service/runcible"""

    def __init__(self, url='https://platform-services.descarteslabs.com/runcible', token=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        Service.__init__(self, url, token)

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

    def search(self, const_id=None, shape=None, geom=None, start_time=None, end_time=None, params=None, limit=100, offset=0):
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

            if x:
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

        if const_id is None:
            const_id = [None]

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

