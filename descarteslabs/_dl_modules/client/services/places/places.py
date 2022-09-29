# Copyright 2018-2020 Descartes Labs.
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
from descarteslabs.auth import Auth
from descarteslabs.config import get_settings

from ....common.dotdict import DotDict, DotList
from ....common.http.service import DefaultClientMixin
from ...deprecation import deprecate_func
from ..service import Service


class Places(Service, DefaultClientMixin):
    TIMEOUT = (9.5, 30)
    """Places and statistics service"""

    @deprecate_func(
        "The Places client has been deprecated and will be removed competely in a future version."
    )
    def __init__(self, url=None, auth=None, maxsize=10, ttl=600, retries=None):
        """
        :param str url: URL for the places service.  Only change
            this if you are being asked to use a non-default Descartes Labs catalog.  If
            not set, then ``descarteslabs.config.get_settings().PLACES_URL`` will be used.
        :param Auth auth: A custom user authentication (defaults to the user
            authenticated locally by token information on disk or by environment
            variables)
        :param int maxsize: Maximum size of the internal cache
        :param int ttl: Maximum lifetime of entries in the internal cache in seconds
        :param urllib3.util.retry.Retry retries: A custom retry configuration
            used for all API requests (defaults to a reasonable amount of retries)
        """
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().places_url

        super(Places, self).__init__(url, auth=auth, retries=retries)
        self.cache = TTLCache(maxsize, ttl)

    def placetypes(self):
        """Get a list of place types.

        return: list
            List of placetypes ['continent', 'country', 'dependency', 'macroregion', 'region',
                                'district', 'mesoregion', 'microregion', 'county', 'locality']
        """
        r = self.session.get("/placetypes")
        return r.json()

    def random(self, geom="low", placetype=None):
        """Get a random location

        geom: string
            Resolution for the shape [low (default), medium, high]

        return: geojson
        """
        params = {}

        if geom:
            params["geom"] = geom

        if placetype:
            params["placetype"] = placetype

        r = self.session.get("/random", params=params)

        if r.status_code != 200:
            raise RuntimeError("%s: %s" % (r.status_code, r.text))

        return DotDict(r.json())

    @cachedmethod(operator.attrgetter("cache"), key=partial(hashkey, "find"))
    def find(self, path, **kwargs):
        """Find candidate slugs based on full or partial path.

        :param str path: Candidate underscore-separated slug.
        :param placetype: Optional place type for filtering.

        Example::

            >>> from descarteslabs.client.services.places import Places
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
        r = self.session.get("/find/%s" % path, params=kwargs)
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
            >>> from descarteslabs.client.services.places import Places
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
            params["q"] = q

        if country:
            params["country"] = country

        if region:
            params["region"] = region

        if placetype:
            params["placetype"] = placetype

        if limit:
            params["n"] = limit

        r = self.session.get("/search", params=params, timeout=self.TIMEOUT)

        return DotList(r.json())

    @cachedmethod(operator.attrgetter("cache"), key=partial(hashkey, "shape"))
    def shape(self, slug, output="geojson", geom="low"):
        """Get the geometry for a specific slug

        :param slug: Slug identifier.
        :param str output: Desired geometry format (`GeoJSON`).
        :param str geom: Desired resolution for the geometry (`low`, `medium`, `high`).

        :return: GeoJSON ``Feature``

        Example::
            >>> from descarteslabs.client.services.places import Places
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

        params["geom"] = geom
        r = self.session.get("/shape/%s.%s" % (slug, output), params=params)
        return DotDict(r.json())

    @cachedmethod(operator.attrgetter("cache"), key=partial(hashkey, "prefix"))
    def prefix(self, slug, output="geojson", placetype=None, geom="low"):
        """Get all the places that start with a prefix

        :param str slug: Slug identifier.
        :param str output: Desired geometry format (`GeoJSON`, `TopoJSON`).
        :param str placetype: Restrict results to a particular place type.
        :param str geom: Desired resolution for the geometry (`low`, `medium`, `high`).

        :return: GeoJSON or TopoJSON ``FeatureCollection``

        Example::
            >>> from descarteslabs.client.services.places import Places
            >>> il_counties = Places().prefix('north-america_united-states_illinois', placetype='county')
            >>> len(il_counties['features'])
            102

        """
        params = {}

        if placetype:
            params["placetype"] = placetype
        params["geom"] = geom
        r = self.session.get("/prefix/%s.%s" % (slug, output), params=params)

        return DotDict(r.json())

    def sources(self):
        """Get a list of sources"""
        r = self.session.get("/sources", timeout=self.TIMEOUT)

        return DotList(r.json())

    def categories(self):
        """Get a list of categories"""
        r = self.session.get("/categories", timeout=self.TIMEOUT)

        return DotList(r.json())

    def metrics(self):
        """Get a list of metrics"""
        r = self.session.get("/metrics", timeout=self.TIMEOUT)

        return DotList(r.json())

    def data(
        self,
        slug,
        source=None,
        category=None,
        metric=None,
        units=None,
        date=None,
        placetype="county",
    ):
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
            params["source"] = source

        if category:
            params["category"] = category

        if metric:
            params["metric"] = metric

        if units:
            params["units"] = units

        if date:
            params["date"] = date

        if placetype:
            params["placetype"] = placetype

        r = self.session.get("/data/%s" % (slug), params=params, timeout=self.TIMEOUT)

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
            params["source"] = source

        if category:
            params["category"] = category

        if metric:
            params["metric"] = metric

        if units:
            params["units"] = units

        r = self.session.get(
            "/statistics/%s" % (slug), params=params, timeout=self.TIMEOUT
        )

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

            if isinstance(source, str):
                source = [source]

            params["source"] = source

        if category:

            if isinstance(category, str):
                category = [category]

            params["category"] = category

        if metric:

            if isinstance(metric, str):
                metric = [metric]

            params["metric"] = metric

        if date:
            params["date"] = date

        r = self.session.get("/value/%s" % (slug), params=params, timeout=self.TIMEOUT)

        return r.json()
