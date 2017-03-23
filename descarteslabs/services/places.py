import operator
from functools import partial
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey

from .service import Service


class Places(Service):
    TIMEOUT = 120
    """Places and statistics service https://iam.descarteslabs.com/service/waldo"""

    def __init__(self, url='https://services.descarteslabs.com/waldo/v1', token=None, maxsize=10, ttl=600):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        Service.__init__(self, url, token)
        self.cache = TTLCache(maxsize, ttl)

    def placetypes(self):
        """Get a list of place types.

        Example::

            >>> places.placetypes()

            ['country', 'region', 'district', 'mesoregion', 'microregion',
                'county']
        """
        r = self.session.get('%s/placetypes' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'find'))
    def find(self, path, **kwargs):
        """Find candidate slugs based on full or partial path.

        :param str path: Candidate underscore-separated slug.
        :param placetype: Optional place type for filtering.

        Example::

          >>> places.find('morocco')

          [{'bbox': [-17.013743, 21.419971, -1.031999, 35.926519],
            'id': 85632693,
            'name': 'Morocco',
            'path': 'continent:africa_country:morocco',
            'placetype': 'country',
            'slug': 'africa_morocco'}]
        """
        r = self.session.get('%s/find/%s' % (self.url, path), params=kwargs, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'shape'))
    def shape(self, slug, output='geojson', geom='low'):
        """Get the geometry for a specific slug

        :param slug: Slug identifier.
        :param str output: Desired geometry format (`GeoJSON`).
        :param str geom: Desired resolution for the geometry (`low`, `medium`, `high`).

        :return: GeoJSON ``Feature``
        """
        r = self.session.get('%s/shape/%s.%s' % (self.url, slug, output), params={'geom': geom}, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'prefix'))
    def prefix(self, slug, output='geojson', placetype='county', geom='low'):
        """Get all the places that start with a prefix

        :param str slug: Slug identifier.
        :param str output: Desired geometry format (`GeoJSON`, `TopoJSON`).
        :param str placetype: Restrict results to a particular place type.
        :param str geom: Desired resolution for the geometry (`low`, `medium`, `high`).

        :return: GeoJSON or TopoJSON ``FeatureCollection``

        Example::

            >>> il_counties = places.prefix('north-america_united-states_illinois', placetype='county')
            >>> len(il_counties['features'])

            102

        """
        r = self.session.get('%s/prefix/%s.%s' % (self.url, slug, output),
                             params={'placetype': placetype, 'geom': geom}, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def sources(self):
        """Get a list of models (sources)."""

        r = self.session.get('%s/sources' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def categories(self):
        """Get a list of categories."""
        r = self.session.get('%s/categories' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def metrics(self):
        """Get a list of metrics."""
        r = self.session.get('%s/metrics' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    def triples(self):
        """Get a list of triples."""
        r = self.session.get('%s/triples' % self.url, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'data'))
    def data(self, slug, **kwargs):
        """Get a list of statistics.

        :param str slug: slug identifier
        :param str placetype: place type
        :param str source: source model
        :param str category: category
        :param str metric: metric
        :param int year: year
        :param int doy: day of the year

        Example::

            >>> places.data('north-america_united-states', placetype='county', source='nass',
                    category='corn', metric='yield', year=2015, doy=1)
        """
        r = self.session.get('%s/data/%s' % (self.url, slug), params=kwargs, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'statistics'))
    def statistics(self, slug, **kwargs):
        """Get a list of statistics for a specific shape.

        :param str slug: slug identifier
        :param str source: source model
        :param str category: category
        :param str metric: metric
        :param str year: year
        :param str doy: day of the year

        Example::

            >>> places.statistics('north-america_united-states_iowa', source='nass',
                    category='corn', metric='yield', year=2015)
        """
        r = self.session.get('%s/statistics/%s' % (self.url, slug), params=kwargs, timeout=self.TIMEOUT)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return r.json()
