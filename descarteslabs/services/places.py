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
import os
from functools import partial
from cachetools import TTLCache, cachedmethod
from cachetools.keys import hashkey
from .service import Service


class Places(Service):
    TIMEOUT = (9.5, 120)
    """Places and statistics service https://iam.descarteslabs.com/service/waldo"""

    def __init__(self, url=None, token=None, maxsize=10, ttl=600):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        if url is None:
            url = os.environ.get("DESCARTESLABS_PLACES_URL", "https://platform-services.descarteslabs.com/waldo/v1")

        Service.__init__(self, url, token)
        self.cache = TTLCache(maxsize, ttl)

    def placetypes(self):
        """Get a list of place types.

        Example::
            >>> import descarteslabs as dl
            >>> dl.places.placetypes()
            ['continent', 'country', 'dependency', 'macroregion', 'region',
                'district', 'mesoregion', 'microregion', 'county']
        """
        r = self.session.get('%s/placetypes' % self.url, timeout=self.TIMEOUT)

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'find'))
    def find(self, path, **kwargs):
        """Find candidate slugs based on full or partial path.

        :param str path: Candidate underscore-separated slug.
        :param placetype: Optional place type for filtering.

        Example::

            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> results = dl.places.find('morocco')
            >>> pprint(results.pop('bbox'))
            [{'id': 85632693,
              'name': 'Morocco',
              'path': 'continent:africa_country:morocco',
              'placetype': 'country',
              'slug': 'africa_morocco'}]
        """
        r = self.session.get('%s/find/%s' % (self.url, path), params=kwargs, timeout=self.TIMEOUT)

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'shape'))
    def shape(self, slug, output='geojson', geom='low'):
        """Get the geometry for a specific slug

        :param slug: Slug identifier.
        :param str output: Desired geometry format (`GeoJSON`).
        :param str geom: Desired resolution for the geometry (`low`, `medium`, `high`).

        :return: GeoJSON ``Feature``

        Example::
            >>> import descarteslabs as dl
            >>> from pprint import pprint
            >>> kansas = dl.places.shape('north-america_united-states_kansas')
            >>> kansas['bbox']
            [-102.051744, 36.993016, -94.588658, 40.003078]

            >>> kansas['geometry']['type']
            'Polygon'

            >>> pprint(kansas['properties'])
            {'name': 'Kansas',
             'parent_id': 85633793,
             'path': 'continent:north-america_country:united-states_region:kansas',
             'placetype': 'region',
             'slug': 'north-america_united-states_kansas'}

        """
        r = self.session.get('%s/shape/%s.%s' % (self.url, slug, output), params={'geom': geom}, timeout=self.TIMEOUT)

        return r.json()

    @cachedmethod(operator.attrgetter('cache'), key=partial(hashkey, 'prefix'))
    def prefix(self, slug, output='geojson', placetype=None, geom='low'):
        """Get all the places that start with a prefix

        :param str slug: Slug identifier.
        :param str output: Desired geometry format (`GeoJSON`, `TopoJSON`).
        :param str placetype: Restrict results to a particular place type.
        :param str geom: Desired resolution for the geometry (`low`, `medium`, `high`).

        :return: GeoJSON or TopoJSON ``FeatureCollection``

        Example::
            >>> import descarteslabs as dl
            >>> il_counties = dl.places.prefix('north-america_united-states_illinois', placetype='county')
            >>> len(il_counties['features'])
            102

        """
        params = {}
        if placetype:
            params['placetype'] = placetype
        params['geom'] = geom
        r = self.session.get('%s/prefix/%s.%s' % (self.url, slug, output),
                             params=params, timeout=self.TIMEOUT)

        return r.json()
