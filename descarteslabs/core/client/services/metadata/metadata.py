# Copyright 2018-2023 Descartes Labs.
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
import itertools

from ..service import Service
from descarteslabs.auth import Auth
from descarteslabs.config import get_settings
from ...deprecation import deprecate
from ....common import property_filtering
from ....common.dotdict import DotDict, DotList
from ....common.dltile import Tile
from ....common.http.service import DefaultClientMixin
from ....common.shapely_support import shapely_to_geojson


class Metadata(Service, DefaultClientMixin):
    """
    Image Metadata Service

    Any methods that take start and end timestamps accept most common date/time
    formats as a string. If no explicit timezone is given, the timestamp is assumed
    to be in UTC. For example ``'2012-06-01'`` means June 1st 2012 00:00 in UTC,
    ``'2012-06-01 00:00+02:00'`` means June 1st 2012 00:00 in GMT+2.
    """

    TIMEOUT = (9.5, 120)

    properties = property_filtering.Properties()

    def __init__(self, url=None, auth=None, retries=None):
        """
        :param str url: URL for the metadata service.  Only change
            this if you are being asked to use a non-default Descartes Labs catalog.  If
            not set, then ``descarteslabs.config.get_settings().METADATA_URL`` will be used.
        :param Auth auth: A custom user authentication (defaults to the user
            authenticated locally by token information on disk or by environment
            variables)
        :param urllib3.util.retry.Retry retries: A custom retry configuration
            used for all API requests (defaults to a reasonable amount of retries)
        """
        if auth is None:
            auth = Auth.get_default_auth()

        if url is None:
            url = get_settings().metadata_url

        super(Metadata, self).__init__(url, auth=auth, retries=retries)

    def bands(
        self,
        products,
        limit=None,
        offset=None,
        wavelength=None,
        resolution=None,
        tags=None,
        bands=None,
        **kwargs
    ):
        """Search for imagery data bands that you have access to.

        :param list(str) products: A list of product(s) to return bands for. May
          not be empty.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.
        :param float wavelength: A wavelength in nm e.g 700 that the band sensor must measure.
        :param int resolution: The resolution in meters per pixel e.g 30 of the data available in this band.
        :param list(str) tags: A list of tags that the band must have in its own tag list.

        :return: List of dicts containing at most `limit` bands. Empty if there are no
            bands matching query (e.g. product id not available).
        :rtype: DotList(DotDict)
        """
        if not products:
            raise ValueError(
                "One or more products must be specified to search for bands"
            )

        params = ["limit", "offset", "products", "wavelength", "resolution", "tags"]

        args = locals()
        kwargs = dict(
            kwargs,
            **{param: args[param] for param in params if args[param] is not None}
        )

        r = self.session.post("/bands/search", json=kwargs)
        return DotList(r.json())

    def derived_bands(
        self, bands=None, require_bands=None, limit=None, offset=None, **kwargs
    ):
        """Search for predefined derived bands that you have access to.

        :param list(str) bands: Limit the derived bands to ones that can be
                                computed using this list of spectral bands.
                                e.g ["red", "nir", "swir1"]
        :param bool require_bands: Control whether searched bands *must* contain
                                   all the spectral bands passed in the bands param.
                                   Defaults to False.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.

        :return: List of dicts containing at most `limit` bands.
        :rtype: DotList(DotDict)
        """
        params = ["bands", "require_bands", "limit", "offset"]

        args = locals()
        kwargs = dict(
            kwargs,
            **{param: args[param] for param in params if args[param] is not None}
        )

        r = self.session.post("/bands/derived/search", json=kwargs)
        return DotList(r.json())

    def get_bands_by_id(self, id_):
        """
        For a given image source id, return the available bands.

        :param str id_: A :class:`Metadata` image identifier.

        :return: A dictionary of band entries and their metadata.
        :rtype: DotDict

        :raises ~descarteslabs.exceptions.NotFoundError: Raised if image id cannot
            be found.

        Example::

            >>> from descarteslabs.client.services.metadata import Metadata
            >>> bands = Metadata().get_bands_by_id('landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1')
            >>> ndvi_info = bands['derived:ndvi'] # View NDVI band information
            >>> ndvi_info['physical_range']
            [-1.0, 1.0]
        """
        r = self.session.get("/bands/id/{}".format(id_))

        return DotDict(r.json())

    def get_bands_by_product(self, product_id):
        """
        All bands (including derived bands) available in a product.

        :param str product_id: A product identifier.

        :return: A dictionary mapping band ids to dictionaries of their metadata.
            Returns empty dict if product id not found.
        :rtype: DotDict
        """
        r = self.session.get("/bands/all/{}".format(product_id))

        return DotDict(r.json())

    @deprecate(renamed={"band": "bands"})
    def products(
        self, bands=None, limit=None, offset=None, owner=None, text=None, **kwargs
    ):
        """Search products that are available on the platform.

        :param list(str) bands: Band name(s) e.g ["red", "nir"] to filter products by.
                                Note that products must match all bands that are passed.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.
        :param str owner: Filter products by the owner's uuid.
        :param str text: Filter products by string match.

        :return: List of dicts containing at most `limit` products. Empty if no matching
            products are found.
        :rtype: DotList(DotDict)
        """
        keys = ["limit", "offset", "bands", "owner", "text"]
        args = locals()
        params = {key: args[key] for key in keys if args[key]}

        # sets minumum limit
        params["limit"] = 1000 if limit is None else min(limit, 1000)

        # Add parameters that were set to kwargs
        kwargs = dict(kwargs, **params)

        products = []
        continuation_token = None
        while limit is None or len(products) < limit:
            if continuation_token:
                kwargs["continuation_token"] = continuation_token

            r = self.session.post("/products/search", json=kwargs)
            continuation_token = r.headers.get("X-Continuation-Token")
            paged_response = r.json()
            products.extend(paged_response)
            if not continuation_token or not paged_response:
                break

        return DotList(products[:limit])

    def available_products(self):
        """Get the list of product identifiers you have access to.

        :return: List of product ids
        :rtype: DotList

        Example::
            >>> from descarteslabs.client.services.metadata import Metadata
            >>> products = Metadata().available_products()
            >>> products  # doctest: +SKIP
            ['landsat:LC08:PRE:TOAR']
        """
        return DotList(p.id for p in self.products())

    @deprecate(
        renamed={
            "product": "products",
            "const_id": "const_ids",
            "sat_id": "sat_ids",
            "start_time": "start_datetime",
            "end_time": "end_datetime",
            "part": "interval",
        },
    )
    def summary(
        self,
        products,
        sat_ids=None,
        date="acquired",
        interval=None,
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
        storage_state=None,
        q=None,
        pixels=None,
        dltile=None,
        **kwargs
    ):
        """Get a summary of the results for the specified spatio-temporal query.

        :param list(str) products: Product identifier(s). May not be empty.
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str interval: Part of the date to aggregate over (e.g. `day`).
            The list of possibilites is:

            * ``year`` or ``y``
            * ``quarter``
            * ``month`` or ``M``
            * ``week`` or ``q``
            * ``day`` or ``d``
            * ``hour`` or ``h``
            * ``minute`` or ``m``
            * ``product``
        :param str geom: A GeoJSON or WKT region of interest or a Shapely shape object.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum image fill fraction, calculated as valid/total pixels.
        :param str storage_state: Filter results based on `storage_state` value. Allowed values are `"available"`,
            `"remote"`, or `None`, which returns all results regardless of `storage_state` value.
        :param ~descarteslabs.common.property_filtering.filtering.Expression q:
            Expression for filtering the results. See
            :py:attr:`~descarteslabs.client.services.metadata.properties`.
        :param bool pixels: Whether to include pixel counts in summary calculations.
        :param str dltile: A dltile key used to specify the search geometry, an alternative
            to the ``geom`` argument.

        :return: Dictionary containing summary of products that match query. Empty products list
            if no matching products found.
        :rtype: DotDict

        Example::

            >>> from descarteslabs.client.services.metadata import Metadata
            >>> iowa_geom = {
            ...     "coordinates": [[
            ...         [-96.498997, 42.560832],
            ...         [-95.765645, 40.585208],
            ...         [-91.729115, 40.61364],
            ...         [-91.391613, 40.384038],
            ...         [-90.952233, 40.954047],
            ...         [-91.04589, 41.414085],
            ...         [-90.343228, 41.587833],
            ...         [-90.140613, 41.995999],
            ...         [-91.065059, 42.751338],
            ...         [-91.217706, 43.50055],
            ...         [-96.599191, 43.500456],
            ...         [-96.498997, 42.560832]
            ...     ]],
            ...     "type": "Polygon"
            ... }
            >>> Metadata().summary(geom=iowa_geom,
            ...                    products=['landsat:LC08:PRE:TOAR'],
            ...                    start_datetime='2016-07-06',
            ...                    end_datetime='2016-07-07',
            ...                    interval='hour',
            ...                    pixels=True)
            {
                'bytes': 290740659,
                'count': 3,
                'items': [
                  {
                    'bytes': 191795912,
                    'count': 2,
                    'date': '2016-07-06T16:00:00.000Z',
                    'pixels': 500639616,
                    'timestamp': 1467820800
                  },
                  {
                    'bytes': 98944747,
                    'count': 1,
                    'date': '2016-07-06T17:00:00.000Z',
                    'pixels': 251142720,
                    'timestamp': 1467824400
                  }
                ],
                'pixels': 751782336,
                'products': ['landsat:LC08:PRE:TOAR']
            }
        """

        if dltile is not None:
            if isinstance(dltile, str):
                geom = Tile.from_key(dltile).geometry
            if isinstance(dltile, dict):
                geom = dltile["geometry"]

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        if sat_ids:
            if isinstance(sat_ids, str):
                sat_ids = [sat_ids]

            kwargs["sat_ids"] = sat_ids

        if products:
            if isinstance(products, str):
                products = [products]

            kwargs["products"] = products

        if date:
            kwargs["date"] = date

        if interval:
            kwargs["interval"] = interval

        if geom:
            geom = shapely_to_geojson(geom)
            kwargs["geom"] = geom

        if start_datetime:
            kwargs["start_datetime"] = start_datetime

        if end_datetime:
            kwargs["end_datetime"] = end_datetime

        if cloud_fraction is not None:
            kwargs["cloud_fraction"] = cloud_fraction

        if cloud_fraction_0 is not None:
            kwargs["cloud_fraction_0"] = cloud_fraction_0

        if fill_fraction is not None:
            kwargs["fill_fraction"] = fill_fraction

        if q is not None:
            if not isinstance(q, list):
                q = [q]
            kwargs["query_expr"] = property_filtering.filtering.AndExpression(
                q
            ).serialize()

        if pixels:
            kwargs["pixels"] = pixels

        if storage_state:
            kwargs["storage_state"] = storage_state

        r = self.session.post("/summary", json=kwargs)
        return DotDict(r.json())

    @deprecate(
        renamed={
            "product": "products",
            "const_id": "const_ids",
            "sat_id": "sat_ids",
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        },
        deprecated=["offset"],
    )
    def paged_search(
        self,
        products,
        sat_ids=None,
        date="acquired",
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
        storage_state=None,
        q=None,
        limit=100,
        fields=None,
        dltile=None,
        sort_field=None,
        sort_order="asc",
        randomize=None,
        continuation_token=None,
        **kwargs
    ):
        """
        Execute a metadata query in a paged manner, with up to 10,000 items per page.

        Most clients should use :py:func:`features` instead, which batch searches into smaller requests
        and handles the paging for you.

        :param list(str) products: Product Identifier(s). May not be empty.
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (default is `acquired`).
        :param str geom: A GeoJSON or WKT region of interest or a Shapely shape object.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum image fill fraction, calculated as valid/total pixels.
        :param str storage_state: Filter results based on `storage_state` value. Allowed values are
            `"available"`, `"remote"`, or `None`, which returns all results regardless of
            `storage_state` value.
        :param ~descarteslabs.common.property_filtering.filtering.Expression q:
            Expression for filtering the results. See
            :py:attr:`~descarteslabs.client.services.metadata.properties`.
        :param int limit: Maximum number of items per page to return.
        :param list(str) fields: Properties to return.
        :param str dltile: A dltile key used to specify the search geometry, an alternative
            to the ``geom`` argument.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.
        :param bool randomize: Randomize the results. You may also use an `int` or `str` as an explicit seed.
        :param str continuation_token: None for new query, or the `properties.continuation_token` value from
            the returned FeatureCollection from a previous invocation of this method to page through a large
            result set.

        :return: GeoJSON ``FeatureCollection`` containing at most `limit` features.
        :rtype: DotDict
        """

        if dltile is not None:
            if isinstance(dltile, str):
                geom = Tile.from_key(dltile).geometry
            if isinstance(dltile, dict):
                geom = dltile["geometry"]

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        kwargs.update({"date": date, "limit": limit})

        if sat_ids:
            if isinstance(sat_ids, str):
                sat_ids = [sat_ids]

            kwargs["sat_ids"] = sat_ids

        if products:
            if isinstance(products, str):
                products = [products]

            kwargs["products"] = products

        if geom:
            geom = shapely_to_geojson(geom)
            kwargs["geom"] = geom

        if start_datetime:
            kwargs["start_datetime"] = start_datetime

        if end_datetime:
            kwargs["end_datetime"] = end_datetime

        if cloud_fraction is not None:
            kwargs["cloud_fraction"] = cloud_fraction

        if cloud_fraction_0 is not None:
            kwargs["cloud_fraction_0"] = cloud_fraction_0

        if fill_fraction is not None:
            kwargs["fill_fraction"] = fill_fraction

        if storage_state:
            kwargs["storage_state"] = storage_state

        if fields is not None:
            kwargs["fields"] = fields

        if q is not None:
            if not isinstance(q, list):
                q = [q]
            kwargs["query_expr"] = property_filtering.filtering.AndExpression(
                q
            ).serialize()

        if sort_field is not None:
            kwargs["sort_field"] = sort_field

        if sort_order is not None:
            kwargs["sort_order"] = sort_order

        if randomize is not None:
            kwargs["random_seed"] = randomize

        if continuation_token is not None:
            kwargs["continuation_token"] = continuation_token

        r = self.session.post("/search", json=kwargs)

        fc = {"type": "FeatureCollection", "features": r.json()}

        if "x-continuation-token" in r.headers:
            fc["properties"] = {"continuation_token": r.headers["x-continuation-token"]}

        return DotDict(fc)

    @deprecate(
        renamed={
            "product": "products",
            "sat_id": "sat_ids",
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        },
    )
    def search(
        self,
        products,
        sat_ids=None,
        date="acquired",
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
        storage_state=None,
        q=None,
        limit=100,
        fields=None,
        dltile=None,
        sort_field=None,
        sort_order="asc",
        randomize=None,
        **kwargs
    ):
        """Search metadata given a spatio-temporal query. All parameters are
        optional.

        If performing a large query, consider using the iterator :py:func:`features` instead.

        :param list(str) products: Product Identifier(s). May not be empty.
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum image fill fraction, calculated as valid/total pixels.
        :param str storage_state: Filter results based on `storage_state` value. Allowed values are
            `"available"`, `"remote"`, or `None`, which returns all results regardless of
            `storage_state` value.
        :param ~descarteslabs.common.property_filtering.filtering.Expression q:
            Expression for filtering the results. See
            :py:attr:`~descarteslabs.client.services.metadata.properties`.
        :param int limit: Maximum number of items to return.
        :param list(str) fields: Properties to return.
        :param str dltile: A dltile key used to specify the search geometry, an alternative
            to the ``geom`` argument.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.
        :param bool randomize: Randomize the results. You may also use an `int` or `str` as an explicit seed.

        :return: GeoJSON ``FeatureCollection``. Empty features list if no matching images found.
        :rtype: DotDict

        Note that as of release 0.16.0 the ``continuation_token`` token has been removed. Please use the
        :py:func:`paged_search` if you require this feature.

        Example::

            >>> from descarteslabs.client.services.metadata import Metadata
            >>> iowa_geom = {
            ...     "coordinates": [[
            ...         [-96.498997, 42.560832],
            ...         [-95.765645, 40.585208],
            ...         [-91.729115, 40.61364],
            ...         [-91.391613, 40.384038],
            ...         [-90.952233, 40.954047],
            ...         [-91.04589, 41.414085],
            ...         [-90.343228, 41.587833],
            ...         [-90.140613, 41.995999],
            ...         [-91.065059, 42.751338],
            ...         [-91.217706, 43.50055],
            ...         [-96.599191, 43.500456],
            ...         [-96.498997, 42.560832]
            ...     ]],
            ...     "type": "Polygon"
            ... }
            >>> scenes = Metadata().search(
            ...     geom=iowa_geom,
            ...     products=['landsat:LC08:PRE:TOAR'],
            ...     start_datetime='2016-07-01',
            ...     end_datetime='2016-07-31T23:59:59'
            ... )
            >>> len(scenes['features'])  # doctest: +SKIP
            2
        """
        features_iter = self.features(
            products=products,
            sat_ids=sat_ids,
            date=date,
            geom=geom,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            cloud_fraction=cloud_fraction,
            cloud_fraction_0=cloud_fraction_0,
            fill_fraction=fill_fraction,
            storage_state=storage_state,
            q=q,
            fields=fields,
            dltile=dltile,
            sort_field=sort_field,
            sort_order=sort_order,
            randomize=randomize,
            batch_size=1000 if limit is None else min(limit, 1000),
            **kwargs
        )
        limited_features = itertools.islice(features_iter, limit)
        return DotDict(type="FeatureCollection", features=DotList(limited_features))

    @deprecate(
        renamed={
            "product": "products",
            "sat_id": "sat_ids",
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        },
    )
    def ids(
        self,
        products,
        sat_ids=None,
        date="acquired",
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
        storage_state=None,
        q=None,
        limit=100,
        dltile=None,
        sort_field=None,
        sort_order=None,
        randomize=None,
        **kwargs
    ):
        """Search metadata given a spatio-temporal query. All parameters are
        optional.

        :param list(str) products: Products identifier(s). May not be empty.
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum image fill fraction, calculated as valid/total pixels.
        :param str storage_state: Filter results based on `storage_state` value. Allowed values are
            `"available"`, `"remote"`, or `None`, which returns all results regardless of
            `storage_state` value.
        :param ~descarteslabs.common.property_filtering.filtering.Expression q:
            Expression for filtering the results. See
            :py:attr:`~descarteslabs.client.services.metadata.properties`.
        :param int limit: Number of items to return.
        :param str dltile: A dltile key used to specify the search geometry, an alternative
            to the ``geom`` argument.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.
        :param bool randomize: Randomize the results. You may also use an `int` or `str` as an explicit seed.

        :return: List of image identifiers. Empty list if no matching images found.
        :rtype: DotList(str)

        Example::

            >>> from descarteslabs.client.services.metadata import Metadata
            >>> iowa_geom = {
            ...     "coordinates": [[
            ...         [-96.498997, 42.560832],
            ...         [-95.765645, 40.585208],
            ...         [-91.729115, 40.61364],
            ...         [-91.391613, 40.384038],
            ...         [-90.952233, 40.954047],
            ...         [-91.04589, 41.414085],
            ...         [-90.343228, 41.587833],
            ...         [-90.140613, 41.995999],
            ...         [-91.065059, 42.751338],
            ...         [-91.217706, 43.50055],
            ...         [-96.599191, 43.500456],
            ...         [-96.498997, 42.560832]
            ...     ]],
            ...     "type": "Polygon"
            ... }
            >>> ids = Metadata().ids(geom=iowa_geom,
            ...                      products=['landsat:LC08:PRE:TOAR'],
            ...                      start_datetime='2016-07-01',
            ...                      end_datetime='2016-07-31T23:59:59')
            >>> len(ids)  # doctest: +SKIP
            2

            >>> ids  # doctest: +SKIP
            ['landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1', 'landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1']
        """
        result = self.search(
            sat_ids=sat_ids,
            products=products,
            date=date,
            geom=geom,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            cloud_fraction=cloud_fraction,
            cloud_fraction_0=cloud_fraction_0,
            fill_fraction=fill_fraction,
            storage_state=storage_state,
            q=q,
            limit=limit,
            fields=[],
            dltile=dltile,
            sort_field=sort_field,
            sort_order=sort_order,
            randomize=randomize,
            **kwargs
        )

        return DotList(feature["id"] for feature in result["features"])

    @deprecate(
        renamed={
            "product": "products",
            "sat_id": "sat_ids",
            "start_time": "start_datetime",
            "end_time": "end_datetime",
        },
    )
    def features(
        self,
        products,
        sat_ids=None,
        date="acquired",
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
        storage_state=None,
        q=None,
        fields=None,
        batch_size=1000,
        dltile=None,
        sort_field=None,
        sort_order="asc",
        randomize=None,
        **kwargs
    ):
        """Generator that efficiently scrolls through the search results.

        :param int batch_size: Number of features to fetch per request.

        :return: Generator of GeoJSON ``Feature`` objects. Empty if no matching images found.
        :rtype: generator

        Example::

            >>> from descarteslabs.client.services.metadata import Metadata
            >>> features = Metadata().features(
            ...     "landsat:LC08:PRE:TOAR",
            ...     start_datetime='2016-01-01',
            ...     end_datetime="2016-03-01"
            ... )
            >>> total = 0
            >>> for f in features:
            ...     total += 1

            >>> total # doctest: +SKIP
            31898
        """

        continuation_token = None

        while True:
            result = self.paged_search(
                sat_ids=sat_ids,
                products=products,
                date=date,
                geom=geom,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                cloud_fraction=cloud_fraction,
                cloud_fraction_0=cloud_fraction_0,
                fill_fraction=fill_fraction,
                storage_state=storage_state,
                q=q,
                fields=fields,
                limit=batch_size,
                dltile=dltile,
                sort_field=sort_field,
                sort_order=sort_order,
                randomize=randomize,
                continuation_token=continuation_token,
                **kwargs
            )

            if not result["features"]:
                break

            for feature in result["features"]:
                yield feature

            continuation_token = result["properties"].get("continuation_token")
            if not continuation_token:
                break

    def get(self, image_id):
        """Get metadata of a single image.

        :param str image_id: Image identifier.

        :return: A dictionary of metadata for a single image.
        :rtype: DotDict

        :raises ~descarteslabs.exceptions.NotFoundError: Raised if image id cannot
             be found.

        Example::

            >>> from descarteslabs.client.services.metadata import Metadata
            >>> meta = Metadata().get('landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1')
            >>> keys = list(meta.keys())
            >>> keys.sort()
            >>> keys  # doctest: +SKIP
            ['acquired', 'area', 'bits_per_pixel', 'bright_fraction', 'bucket',
             'cloud_fraction', 'cloud_fraction_0', 'confidence_dlsr', 'cs_code',
             'descartes_version', 'file_md5s', 'file_sizes', 'files', 'fill_fraction',
             'geolocation_accuracy', 'geometry', 'geotrans', 'id', 'identifier', 'key',
             'modified', 'processed', 'product', 'proj4', 'projcs', 'published',
             'raster_size', 'reflectance_scale', 'roll_angle', 'sat_id',
             'solar_azimuth_angle', 'solar_elevation_angle', 'storage_state',
             'sw_version', 'terrain_correction', 'tile_id']
        """
        r = self.session.get("/get/{}".format(image_id))
        return DotDict(r.json())

    def get_by_ids(self, ids, fields=None, ignore_not_found=True, **kwargs):
        """Get metadata for multiple images by image id. The response contains list of
        found images in the order of the given ids.

        :param list(str) ids: Image identifiers.
        :param list(str) fields: Properties to return.
        :param bool ignore_not_found: For image id lookups that fail: if :py:obj:`True`, ignore;
                                      if :py:obj:`False`, raise :py:exc:`NotFoundError`. Default is :py:obj:`True`.

        :return: List of image metadata dicts.
        :rtype: DotList(DotDict)

        :raises ~descarteslabs.exceptions.NotFoundError: Raised if an image id cannot
             be found and ignore_not_found set to `False` (default is `True`)
        """
        kwargs["ids"] = ids
        kwargs["ignore_not_found"] = ignore_not_found
        if fields is not None:
            kwargs["fields"] = fields

        r = self.session.post("/batch/images", json=kwargs)
        return DotList(r.json())

    def get_product(self, product_id):
        """Get information about a single product.

        :param str product_id: Product Identifier.

        :return: A dictionary with metadata for a single product.
        :rtype: DotDict

        :raises ~descarteslabs.exceptions.NotFoundError: Raised if an product id
            cannot be found.
        """
        r = self.session.get("/products/{}".format(product_id))
        return DotDict(r.json())

    def get_band(self, band_id):
        """Get information about a single band.

        :param str band_id: Band Identifier.

        :return: A dictionary with metadata for a single band.
        :rtype: DotDict

        :raises ~descarteslabs.exceptions.NotFoundError: Raised if an band id
            cannot be found.
        """
        r = self.session.get("/bands/{}".format(band_id))
        return DotDict(r.json())

    def get_derived_band(self, derived_band_id):
        """Get information about a single derived band.

        :param str derived_band_id: Derived band identifier.

        :return: A dictionary with metadata for a single derived band.
        :rtype: DotDict

        :raises ~descarteslabs.exceptions.NotFoundError: Raised if a band id cannot be found.
        """
        r = self.session.get("/bands/derived/{}".format(derived_band_id))
        return DotDict(r.json())
