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

import json
import os
import itertools
from warnings import warn, simplefilter
from six import string_types
from descarteslabs.client.services.service import Service
from descarteslabs.client.services.places import Places
from descarteslabs.client.auth import Auth
from descarteslabs.client.deprecation import check_deprecated_kwargs
from descarteslabs.client.services.raster import Raster
from descarteslabs.common.property_filtering.filtering import (
    AndExpression,
    GenericProperties,
)
from descarteslabs.common.dotdict import DotDict, DotList


SOURCES_DEPRECATION_MESSAGE = (
    "Metadata.sources() has been deprecated and will be removed in "
    "future versions of the library. Please use "
    "Metadata.available_products() or Metadata.products() instead. "
)


class Metadata(Service):
    """
    Image Metadata Service

    Any methods that take start and end timestamps accept most common date/time
    formats as a string. If no explicit timezone is given, the timestamp is assumed
    to be in UTC. For example ``'2012-06-01'`` means June 1st 2012 00:00 in UTC,
    ``'2012-06-01 00:00+02:00'`` means June 1st 2012 00:00 in GMT+2.
    """

    TIMEOUT = (9.5, 120)

    properties = GenericProperties()

    def __init__(self, url=None, auth=None):
        """The parent Service class implements authentication and exponential
        backoff/retry. Override the url parameter to use a different instance
        of the backing service.
        """
        if auth is None:
            auth = Auth()

        simplefilter("always", DeprecationWarning)
        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_METADATA_URL",
                "https://platform.descarteslabs.com/metadata/v1",
            )

        super(Metadata, self).__init__(url, auth=auth)
        self._raster = Raster(auth=self.auth)

    def sources(self):
        warn(SOURCES_DEPRECATION_MESSAGE, DeprecationWarning)

        r = self.session.get("/sources")
        return DotList(r.json())

    def bands(
        self,
        products=None,
        limit=None,
        offset=None,
        wavelength=None,
        resolution=None,
        tags=None,
        bands=None,
        **kwargs
    ):
        """Search for imagery data bands that you have access to.

        :param list(str) products: A list of product(s) to return bands for.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.
        :param float wavelength: A wavelength in nm e.g 700 that the band sensor must measure.
        :param int resolution: The resolution in meters per pixel e.g 30 of the data available in this band.
        :param list(str) tags: A list of tags that the band must have in its own tag list.


        """
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
        :param bool require_bands: Control whether searched bands must contain
                                   all the spectral bands passed in the bands param.
                                   Defaults to False.
        :param int limit: Number of results to return.
        :param int offset: Index to start at when returning results.
        """
        params = ["bands", "limit", "offset"]

        args = locals()
        kwargs = dict(
            kwargs,
            **{param: args[param] for param in params if args[param] is not None}
        )

        r = self.session.post("/bands/derived/search", json=kwargs)
        return DotList(r.json())

    def get_bands_by_id(self, id_):
        """
        For a given source id, return the available bands.

        :param str id_: A :class:`Metadata` identifier.

        :return: A dictionary of band entries and their metadata.
        """
        r = self.session.get("/bands/id/{}".format(id_))

        return DotDict(r.json())

    def get_bands_by_product(self, product_id):
        """
        All bands (includig derived bands) available in a product.

        :param str product_id: A product identifier.

        :return: A dictionary mapping band IDs to dictionaries of their metadata.
        """
        r = self.session.get("/bands/all/{}".format(product_id))

        return DotDict(r.json())

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

        """
        params = ["limit", "offset", "bands", "owner", "text"]

        args = locals()
        kwargs = dict(
            kwargs,
            **{param: args[param] for param in params if args[param] is not None}
        )
        check_deprecated_kwargs(kwargs, {"band": "bands"})

        r = self.session.post("/products/search", json=kwargs)

        return DotList(r.json())

    def available_products(self):
        """Get the list of product identifiers you have access to.

        Example::
            >>> from descarteslabs.client.services import Metadata
            >>> products = Metadata().available_products()
            >>> products  # doctest: +SKIP
            ['landsat:LC08:PRE:TOAR']

        """
        r = self.session.get("/products")

        return DotList(r.json())

    def summary(
        self,
        products=None,
        sat_ids=None,
        date="acquired",
        interval=None,
        place=None,
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
        q=None,
        pixels=None,
        dltile=None,
        **kwargs
    ):
        """Get a summary of the results for the specified spatio-temporal query.

        :param list(str) products: Product identifier(s).
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
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param expr q: Expression for filtering the results. See
            :py:attr:`descarteslabs.client.services.metadata.properties`.
        :param bool pixels: Whether to include pixel counts in summary calculations.
        :param str dltile: A dltile key used to specify the resolution, bounds, and srs.

        Example usage::

            >>> from descarteslabs.client.services import Metadata
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
                  'bytes': 93298309,
                  'count': 1,
                  'items': [
                    {
                      'bytes': 93298309,
                      'count': 1,
                      'date': '2016-07-06T16:00:00',
                      'pixels': 250508160,
                      'timestamp': 1467820800
                    }
                  ],
                  'pixels': 250508160,
                  'products': ['landsat:LC08:PRE:TOAR']
                }
        """
        check_deprecated_kwargs(
            kwargs,
            {
                "product": "products",
                "const_id": "const_ids",
                "sat_id": "sat_ids",
                "start_time": "start_datetime",
                "end_time": "end_datetime",
                "part": "interval",
            },
        )

        if place:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom="low")
            geom = json.dumps(shape["geometry"])

        if dltile is not None:
            if isinstance(dltile, string_types):
                dltile = self._raster.dltile(dltile)
            if isinstance(dltile, dict):
                geom = dltile["geometry"]

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        if sat_ids:
            if isinstance(sat_ids, string_types):
                sat_ids = [sat_ids]

            kwargs["sat_ids"] = sat_ids

        if products:
            if isinstance(products, string_types):
                products = [products]

            kwargs["products"] = products

        if date:
            kwargs["date"] = date

        if interval:
            kwargs["interval"] = interval

        if geom:
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
            kwargs["query_expr"] = AndExpression(q).serialize()

        if pixels:
            kwargs["pixels"] = pixels

        r = self.session.post("/summary", json=kwargs)
        return DotDict(r.json())

    def _query(
        self,
        products=None,
        sat_ids=None,
        date="acquired",
        place=None,
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
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
        Execute a metadata query for up to 10,000 items.

        Use :py:func:`search` or :py:func:`features` instead, which batch searches into smaller requests
        and handle paging for you.
        """
        check_deprecated_kwargs(
            kwargs,
            {
                "product": "products",
                "const_id": "const_ids",
                "sat_id": "sat_ids",
                "start_time": "start_datetime",
                "end_time": "end_datetime",
                "offset": None,
            },
        )

        if place:
            places = Places()
            places.auth = self.auth
            shape = places.shape(place, geom="low")
            geom = json.dumps(shape["geometry"])

        if dltile is not None:
            if isinstance(dltile, string_types):
                dltile = self._raster.dltile(dltile)
            if isinstance(dltile, dict):
                geom = dltile["geometry"]

        if isinstance(geom, dict):
            geom = json.dumps(geom)

        kwargs.update({"date": date, "limit": limit})

        if sat_ids:
            if isinstance(sat_ids, string_types):
                sat_ids = [sat_ids]

            kwargs["sat_ids"] = sat_ids

        if products:
            if isinstance(products, string_types):
                products = [products]

            kwargs["products"] = products

        if geom:
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

        if fields is not None:
            kwargs["fields"] = fields

        if q is not None:
            if not isinstance(q, list):
                q = [q]
            kwargs["query_expr"] = AndExpression(q).serialize()

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

    def search(
        self,
        products=None,
        sat_ids=None,
        date="acquired",
        place=None,
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
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

        :param list(str) products: Product Identifier(s).
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param expr q: Expression for filtering the results. See
            :py:attr:`descarteslabs.client.services.metadata.properties`.
        :param int limit: Maximuim number of items to return.
        :param list(str) fields: Properties to return.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.
        :param bool randomize: Randomize the results. You may also use an `int` or `str` as an explicit seed.

        return: GeoJSON ``FeatureCollection``

        Example::

            >>> from descarteslabs.client.services import Metadata
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
            32
        """
        features_iter = self.features(
            products=products,
            sat_ids=sat_ids,
            date=date,
            place=place,
            geom=geom,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            cloud_fraction=cloud_fraction,
            cloud_fraction_0=cloud_fraction_0,
            fill_fraction=fill_fraction,
            q=q,
            fields=fields,
            dltile=dltile,
            sort_field=sort_field,
            sort_order=sort_order,
            randomize=randomize,
            **kwargs
        )
        limited_features = itertools.islice(features_iter, limit)
        return DotDict(type="FeatureCollection", features=DotList(limited_features))

    def ids(
        self,
        products=None,
        sat_ids=None,
        date="acquired",
        place=None,
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
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

        :param list(str) products: Products identifier(s).
        :param list(str) sat_ids: Satellite identifier(s).
        :param str date: The date field to use for search (e.g. `acquired`).
        :param str place: A slug identifier to be used as a region of interest.
        :param str geom: A GeoJSON or WKT region of interest.
        :param str start_datetime: Desired starting timestamp, in any common format.
        :param str end_datetime: Desired ending timestamp, in any common format.
        :param float cloud_fraction: Maximum cloud fraction, calculated by data provider.
        :param float cloud_fraction_0: Maximum cloud fraction, calculated by cloud mask pixels.
        :param float fill_fraction: Minimum scene fill fraction, calculated as valid/total pixels.
        :param expr q: Expression for filtering the results. See
            :py:attr:`descarteslabs.client.services.metadata.properties`.
        :param int limit: Number of items to return.
        :param str dltile: a dltile key used to specify the resolution, bounds, and srs.
        :param str sort_field: Property to sort on.
        :param str sort_order: Order of sort.
        :param bool randomize: Randomize the results. You may also use an `int` or `str` as an explicit seed.

        :return: List of image identifiers.

        Example::

            >>> from descarteslabs.client.services import Metadata
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
            32

            >>> ids  # doctest: +SKIP
            ['landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1', 'landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1']

        """
        result = self.search(
            sat_ids=sat_ids,
            products=products,
            date=date,
            place=place,
            geom=geom,
            start_datetime=start_datetime,
            end_datetime=end_datetime,
            cloud_fraction=cloud_fraction,
            cloud_fraction_0=cloud_fraction_0,
            fill_fraction=fill_fraction,
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

    def features(
        self,
        products=None,
        sat_ids=None,
        date="acquired",
        place=None,
        geom=None,
        start_datetime=None,
        end_datetime=None,
        cloud_fraction=None,
        cloud_fraction_0=None,
        fill_fraction=None,
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

        :return: Generator of GeoJSON ``Feature`` objects.

        Example::

            >>> from descarteslabs.client.services import Metadata
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
            result = self._query(
                sat_ids=sat_ids,
                products=products,
                date=date,
                place=place,
                geom=geom,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                cloud_fraction=cloud_fraction,
                cloud_fraction_0=cloud_fraction_0,
                fill_fraction=fill_fraction,
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

        Example::

            >>> from descarteslabs.client.services import Metadata
            >>> meta = Metadata().get('landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1')
            >>> keys = list(meta.keys())
            >>> keys.sort()
            >>> keys
            ['acquired', 'area', 'bits_per_pixel', 'bright_fraction', 'bucket', 'cloud_fraction',
             'cloud_fraction_0', 'confidence_dlsr', 'cs_code', 'descartes_version', 'file_md5s', 'file_sizes',
             'files', 'fill_fraction', 'geolocation_accuracy', 'geometry', 'geotrans', 'id', 'identifier',
             'key', 'processed', 'product', 'projcs', 'published', 'raster_size', 'reflectance_scale',
             'roll_angle', 'sat_id', 'solar_azimuth_angle', 'solar_elevation_angle', 'sw_version',
             'terrain_correction', 'tile_id']
        """
        r = self.session.get("/get/{}".format(image_id))
        return DotDict(r.json())

    def get_by_ids(self, ids, fields=None, ignore_not_found=True, **kwargs):
        """Get metadata for multiple images by id. The response contains found images in the
        order of the given ids.

        :param list(str) ids: Image identifiers.
        :param list(str) fields: Properties to return.
        :param bool ignore_not_found: For image id lookups that fail: if :py:obj:`True`, ignore;
                                      if :py:obj:`False`, raise :py:exc:`NotFoundError`. Default is :py:obj:`True`.

        :return: List of image metadata.
        :rtype: list(dict)
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

        """
        r = self.session.get("/products/{}".format(product_id))
        return DotDict(r.json())

    def get_band(self, band_id):
        """Get information about a single band.

        :param str band_id: Band Identifier.

        """
        r = self.session.get("/bands/{}".format(band_id))
        return DotDict(r.json())

    def get_derived_band(self, derived_band_id):
        """Get information about a single product.

        :param str derived_band_id: Derived band identifier.

        """
        r = self.session.get("/bands/derived/{}".format(derived_band_id))
        return DotDict(r.json())
