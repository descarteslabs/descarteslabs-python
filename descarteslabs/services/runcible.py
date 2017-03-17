import json
from itertools import chain
from operator import itemgetter
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

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def summary(self, const_id=None, date='acquired', part='day', shape=None, geom=None, start_time=None, end_time=None, params=None, bbox=False, direct=False):
        """Get a summary of results for the specified spatio-temporal query.

        const_id: list(string)
            Constellation identifier(s)
        date: string
            The date field to search on
        part: string
            The date part to aggregate over
        shape: string
            An (optional) shape name to use in the spatial filter
        geom: string
            Region of interest as GeoJSON or WKT
        start_time: string
            Start of valid date/time range (inclusive)
        end_time: string
            End of valid date/time range (inclusive)
        params: string
            JSON String of additional key/value pairs for searching properties

        return: dict
        [
            {
              "const_id": "L8",
              "count": 0,
              "bytes": 0,
              "pixels": 0,
              "items: [
                {
                  "date": "2016-11-08",
                  "count": 0,
                  "bytes": 0,
                  "pixels": 0
                }
              ]
            }
        ]
        
        >>> runcible.summary(shape='north-america_united-states_iowa', const_id=['L8'])
        """
        if shape:

            waldo = Waldo()

            shape = waldo.shape(shape, geom='low')

            geom = json.dumps(shape['geometry'])

        kwargs = {}

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
            kwargs['bbox'] = 'true'
            
        if direct:
            kwargs['direct'] = 'true'
            
        def f(x):

            kwargs['const_id'] = x

            r = self.session.post('%s/summary' % self.url, json=kwargs)

            if r.status_code != 200:
                raise RuntimeError("%s: %s" % (r.status_code, r.text))

            return r.json()

        if not const_id:

            const_id = list(set([source['const_id'] for source in self.sources()]))

        result = map(f, const_id)

        return result

    def search(self, const_id=None, date='acquired', shape=None, geom=None, start_time=None, end_time=None, params=None, limit=100, offset=0, bbox=False, direct=False):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. Results are paged using limit/offset.

        shape: string
            An (optional) shape name to use in the spatial filter
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
        direct: boolean
            Whether or not to use the main metadata table directly (default: false)

        return: GeoJSON FeatureCollection

        >>> runcible.search(shape='north-america_united-states_iowa', const_id=['L8'])
        """
        if shape:

            waldo = Waldo()

            shape = waldo.shape(shape, geom='low')

            geom = json.dumps(shape['geometry'])

        kwargs = {}

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

        def f(x):

            kwargs['const_id'] = x

            r = self.session.post('%s/search' % self.url, json=kwargs)

            if r.status_code != 200:
                raise RuntimeError("%s: %s" % (r.status_code, r.text))

            return r.json()

        result = {'type':'FeatureCollection'}

        if not const_id:

            const_id = list(set([source['const_id'] for source in self.sources()]))

        result['features'] = list(chain(*map(f, const_id)))

        return result

    def keys(self, const_id=None, date='acquired', shape=None, geom=None, start_time=None, end_time=None, params=None, limit=100, offset=0, bbox=False, direct=False):
        """Search metadata given a spatio-temporal query. All parameters are
        optional. Results are paged using limit/offset.

        shape: string
            An (optional) shape name to use in the spatial filter
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
        direct: boolean
            Whether or not to use the main metadata table directly (default: false)

        return: list
            keys

        >>> runcible.keys(shape='north-america_united-states_iowa', const_id=['L8'])
        """
        result = self.search(const_id, date, shape, geom, start_time, end_time, params, limit, offset, bbox, direct)
        
        return [feature['id'] for feature in result['features']]
        
    def features(self, const_id=None, date='acquired', shape=None, geom=None, start_time=None, end_time=None, params=None, limit=100, bbox=False, direct=False):
        """
        Generator that combines summary and search to page through results.
        
        limit: integer
            Specify a page size
            
        return: GeoJSON Feature(s)

        >>> for feature in runcible.features():
            ...
        """
        result = self.summary(const_id=const_id, date=date, shape=shape, geom=geom, start_time=start_time, end_time=end_time, params=params, bbox=bbox, direct=direct)
        
        for summary in result:
        
            offset = 0     
            
            count = summary['count']
            const_id = summary['const_id']
            
            while offset < count:
    
                features = self.search(const_id=[const_id], date=date, shape=shape, geom=geom, start_time=start_time, end_time=end_time, params=params, limit=limit, offset=offset, bbox=bbox, direct=direct)
    
                offset = limit + offset

                for feature in features['features']:
                    yield feature

    def get(self, key):
        """Get a single metadata entry
        
        key: string
            The primary key identifier to get
            
        return: dict
            properties

        >>> runcible.get('meta_LC82000452016168_v1')
        """
        r = self.session.post('%s/get/%s' % (self.url, key))

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

