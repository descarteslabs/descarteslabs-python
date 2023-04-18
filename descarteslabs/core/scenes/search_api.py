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

from descarteslabs.exceptions import NotFoundError
from ..common.dotdict import DotDict, DotList
from ..client.deprecation import deprecate
from ..catalog import (
    Product,
    Band,
    SpectralBand,
    DerivedBand,
    Image,
    properties,
)

from ..common.geo import GeoContext, AOI

from .scenecollection import SceneCollection
from .helpers import REQUEST_PARAMS


# map from v1 field names to v2 field names for sort
IMAGE_FIELD_MAP = {
    "cloud_fraction_0": "alt_cloud_fraction",
    "descartes_version": "processing_pipeline_id",
    "identifier": "provider_id",
    "key": "name",
    "processed": "created",
    "proj4": "projection",
    "sat_id": "satellite_id",
}


@deprecate(removed=["randomized", "raster_client", "metadata_client"])
def search(
    aoi,
    products,
    start_datetime=None,
    end_datetime=None,
    cloud_fraction=None,
    storage_state=None,
    limit=100,
    sort_field="acquired",
    sort_order="asc",
    date_field="acquired",
    query=None,
):
    """
    Search for Scenes in the Descartes Labs catalog.

    Returns a SceneCollection of Scenes that overlap with an area of interest,
    and meet the given search criteria.

    Parameters
    ----------
    aoi : GeoJSON-like dict, :class:`~descarteslabs.common.geo.geocontext.GeoContext`, or object with __geo_interface__
        Search for scenes that intersect this area by any amount.
        If a :class:`~descarteslabs.common.geo.geocontext.GeoContext`, a copy is returned as ``ctx``,
        with missing values filled in. Otherwise, the returned ``ctx`` will be
        an `AOI`, with this as its geometry.
    products : str or List[str]
        Descartes Labs product identifiers. May not be empty.
    start_datetime : str, datetime-like, optional
        Restrict to scenes acquired after this datetime
    end_datetime : str, datetime-like, optional
        Restrict to scenes acquired before this datetime
    cloud_fraction : float, optional
        Restrict to scenes that are covered in clouds by less than this fraction
        (between 0 and 1)
    storage_state : str, optional
        Filter results based on ``storage_state`` value
        (``"available"``, ``"remote"``, or ``None``)
    limit : int or None, optional, default 100
        Maximum number of Scenes to return, or None for all results.
    sort_field : str, optional, default 'acquired'
        Field name in :py:attr:`Scene.properties` by which to order the results
    sort_order : str, optional, default 'asc'
        ``"asc"`` or ``"desc"``
    date_field : str, optional, default 'acquired'
        The field used when filtering by date
        (``"acquired"``, ``"processed"``, ``"published"``)
    query : ~descarteslabs.common.property_filtering.filtering.Expression, optional
        Expression used to filter Scenes by their properties, built from
        :class:`dl.properties <descarteslabs.common.property_filtering.filtering.Properties>`.
        You can construct filter expression using the ``==``, ``!=``, ``<``, ``>``,
        ``<=`` and ``>=`` operators as well as the
        :meth:`~descarteslabs.common.property_filtering.filtering.Property.like`
        and :meth:`~descarteslabs.common.property_filtering.filtering.Property.in_`
        methods. You cannot use the boolean keywords ``and`` and ``or`` because of
        Python language limitations; instead you can combine filter expressions
        with ``&`` (boolean "and") and ``|`` (boolean "or").
        Example:
        ``150 < dl.properties.azimuth_angle < 160 & dl.properties.cloud_fraction < 0.5``
    randomize : bool, default False, optional
        Randomize the order of the results.
        You may also use an int or str as an explicit seed.

    Returns
    -------
    scenes : `SceneCollection`
        Scenes matching your criteria.
    ctx: :class:`~descarteslabs.common.geo.geocontext.GeoContext`
        The given ``aoi`` as a :class:`~descarteslabs.common.geo.geocontext.GeoContext` (if it isn't one already),
        with reasonable default parameters for loading all matching Scenes.

        If ``aoi`` was a :class:`~descarteslabs.common.geo.geocontext.GeoContext`, ``ctx`` will be a copy of ``aoi``,
        with any properties that were ``None`` assigned the defaults below.

        If ``aoi`` was not a :class:`~descarteslabs.common.geo.geocontext.GeoContext`, an `AOI` instance will be created
        with ``aoi`` as its geometry, and defaults assigned as described below:

        **Default Spatial Parameters:**

        * resolution: the finest resolution of any band of all matching scenes
        * crs: the most common CRS used of all matching scenes
    """

    if not products:
        raise ValueError("Products is a required parameter.")
    elif isinstance(products, str):
        products = [products]

    if isinstance(aoi, GeoContext):
        ctx = aoi
        if ctx.bounds is None and ctx.geometry is None:
            raise ValueError(
                "Unspecified where to search, "
                "since the GeoContext given for ``aoi`` has neither geometry nor bounds set"
            )
    else:
        ctx = AOI(geometry=aoi)

    search = (
        Image.search(request_params=REQUEST_PARAMS)
        .filter(properties.product_id.in_(products))
        .intersects(ctx)
    )

    if start_datetime or end_datetime:
        date = getattr(properties, date_field)
        if start_datetime and end_datetime:
            search = search.filter(start_datetime <= date < end_datetime)
        elif start_datetime:
            search = search.filter(start_datetime <= date)
        else:
            search = search.filter(date < end_datetime)

    if cloud_fraction:
        search = search.filter(properties.cloud_fraction < cloud_fraction)
    if storage_state:
        search = search.filter(properties.storage_state == storage_state)

    if query is not None:
        if not isinstance(query, list):
            query = [query]
        for q in query:
            search = search.filter(q)

    if sort_field != "acquired" or sort_order != "asc":
        search = search.sort(
            IMAGE_FIELD_MAP.get(sort_field, sort_field), sort_order == "asc"
        )

    if limit:
        search = search.limit(limit)

    ic = search.collect()

    return (SceneCollection(ic), ic.geocontext)


@deprecate(removed=["metadata_client"])
def get_product(product_id):
    """Retrieve information about a single product.

    Parameters
    ----------
    product_id : str
        Product Identifier.

    Returns
    -------
    DotDict
        A dictionary with metadata for a single product.

    Raises
    ------
    NotFoundError
        Raised if a product id cannot be found.
    """
    product = Product.get(product_id, request_params=REQUEST_PARAMS)
    if product is None:
        raise NotFoundError(f"Product {product_id} not found")
    return DotDict(product.v1_properties)


@deprecate(removed=["bands", "offset", "metadata_client"])
def search_products(
    limit=None,
    owner=None,
    text=None,
):
    """Search products that are available on the platform.
    An empty search with no parameters will pass back all available products.

    Parameters
    ----------
    limit : int, optional
        Number of results to return.
    owner : str, optional
        Filter products by the owner's uuid.
    text : str, optional
        Filter products by string match.

    Returns
    -------
    DotList(DotDict)
        List of dicts containing at most `limit` products.
        Empty if no matching products are found.
    """

    search = Product.search(request_params=REQUEST_PARAMS)
    if owner:
        search = search.filter(properties.owners.in_([owner]))
    if text:
        search = search.find_text(text)
    if limit:
        search = search.limit(limit)

    return DotList(DotDict(p.v1_properties) for p in search)


@deprecate(removed=["metadata_client"])
def get_band(band_id):
    """Get information about a single band.

    Parameters
    ----------
    band_id : str
        A band identifier.

    Returns
    -------
    DotDict
        A dictionary of metadata for the band

    Raises
    ------
    NotFoundError
        Raised if a band id cannot be found.
    """
    band = Band.get(band_id, request_params=REQUEST_PARAMS)
    if band is None:
        raise NotFoundError(f"Band {band_id} not found")
    return DotDict(band.v1_properties)


@deprecate(removed=["bands", "offset", "metadata_client"])
def search_bands(
    products,
    limit=None,
    wavelength=None,
    resolution=None,
    tags=None,
):
    """Search for imagery data bands that you have access to.

    Parameters
    ----------
    products : list(str)
        A list of product(s) to return bands for. May not be empty.
    limit : int, optional
        Number of results to return.
    wavelength : float, optional
        A wavelength in nm e.g 700 that the band sensor must measure.
    resolution : int, optional
        The resolution in meters per pixel e.g 30 of the data available in this band.
    tags : list(str), optional
        A list of tags to match. Any band which has any of these tags will be included.

    Returns
    -------
    DotList(DotDict)
        List of dicts containing at most `limit` bands.
        Empty if there are no bands matching query (e.g. product id not available).
    """
    if wavelength:
        search = SpectralBand.search(request_params=REQUEST_PARAMS)
    else:
        search = Band.search(request_params=REQUEST_PARAMS)
    if not products:
        raise ValueError("Products is a required parameter.")
    elif isinstance(products, str):
        products = [products]
    search = search.filter(properties.product_id.in_(products))
    if wavelength:
        search = search.filter(properties.wavelength_nm_min <= wavelength).filter(
            properties.wavelength_nm_max >= wavelength
        )
    if resolution:
        search = search.filter(properties.resolution == resolution)
    if tags:
        if isinstance(tags, str):
            tags = [tags]
        search = search.filter(properties.tags.in_(tags))
    if limit:
        search = search.limit(limit)

    return DotList(DotDict(b.v1_properties) for b in search)


@deprecate(removed=["metadata_client"])
def get_derived_band(derived_band_id):
    """Get information about a single derived band.

    Parameters
    ----------
    derived_band_id : str
        Derived band identifier.

    Returns
    -------
    DotDict
        A dictionary with metadata for a single derived band.

    Raises
    ------
    NotFoundError
        Raised if a band id cannot be found.
    """
    band = DerivedBand.get(derived_band_id, request_params=REQUEST_PARAMS)
    if band is None:
        raise NotFoundError(f"DerivedBand {derived_band_id} not found")
    return DotDict(band.v1_properties)


@deprecate(removed=["offset", "metadata_client"])
def search_derived_bands(
    bands,
    require_bands=None,
    limit=None,
):
    """Search for predefined derived bands that you have access to.

    Parameters
    ----------
    bands : list(str)
        Limit the derived bands to ones that can be computed using this list of spectral bands.
        e.g ["red", "nir", "swir1"]
    require_bands : bool
        Control whether searched bands *must* contain all the spectral bands passed in the bands param.
        Defaults to False.
    limit : int
        Number of results to return.

    Returns
    -------
    DotList(DotDict)
        List of dicts containing at most `limit` bands.
    """
    search = DerivedBand.search(request_params=REQUEST_PARAMS)
    if bands:
        if isinstance(bands, str):
            bands = [bands]
        if require_bands:
            for band in bands:
                search = search.filter(properties.bands == band)
        else:
            search = search.filter(properties.bands.in_(bands))
    if limit:
        search = search.limit(limit)

    return DotList(DotDict(db.v1_properties) for db in search)
