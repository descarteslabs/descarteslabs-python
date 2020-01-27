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

import six
import collections
import datetime

from descarteslabs.client.services.raster import Raster
from descarteslabs.client.services.metadata import Metadata

from .scene import Scene
from .scenecollection import SceneCollection
from . import geocontext


def search(
    aoi,
    products=None,
    start_datetime=None,
    end_datetime=None,
    cloud_fraction=None,
    storage_state=None,
    limit=100,
    sort_field=None,
    sort_order="asc",
    date_field="acquired",
    query=None,
    randomize=False,
    raster_client=None,
    metadata_client=None,
):
    """
    Search for Scenes in the Descartes Labs catalog.

    Returns a SceneCollection of Scenes that overlap with an area of interest,
    and meet the given search criteria.

    Parameters
    ----------
    aoi : GeoJSON-like dict, :class:`~descarteslabs.scenes.geocontext.GeoContext`, or object with __geo_interface__
        Search for scenes that intersect this area by any amount.
        If a :class:`~descarteslabs.scenes.geocontext.GeoContext`, a copy is returned as ``ctx``, with missing values
        filled in. Otherwise, the returned ``ctx`` will be an `AOI`, with this as its geometry.
    products : str or List[str], optional
        Descartes Labs product identifiers
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
    sort_field : str, optional
        Field name in :py:attr:`Scene.properties` by which to order the results
    sort_order : str, optional, default 'asc'
        ``"asc"`` or ``"desc"``
    date_field : str, optional, default 'acquired'
        The field used when filtering by date
        (``"acquired"``, ``"processed"``, ``"published"``)
    query : ~descarteslabs.common.property_filtering.filtering.Expression, optional
        Expression used to filter Scenes by their properties, built from
        :class:`dl.properties <descarteslabs.common.property_filtering.filtering.GenericProperties>`.
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
    raster_client : Raster, optional
        Unneeded in general use; lets you use a specific client instance
        with non-default auth and parameters.
    metadata_client : Metadata, optional
        Unneeded in general use; lets you use a specific client instance
        with non-default auth and parameters.

    Returns
    -------
    scenes : `SceneCollection`
        Scenes matching your criteria.
    ctx: :class:`~descarteslabs.scenes.geocontext.GeoContext`
        The given ``aoi`` as a :class:`~descarteslabs.scenes.geocontext.GeoContext` (if it isn't one already),
        with reasonable default parameters for loading all matching Scenes.

        If ``aoi`` was a :class:`~descarteslabs.scenes.geocontext.GeoContext`, ``ctx`` will be a copy of ``aoi``,
        with any properties that were ``None`` assigned the defaults below.

        If ``aoi`` was not a :class:`~descarteslabs.scenes.geocontext.GeoContext`, an `AOI` instance will be created
        with ``aoi`` as its geometry, and defaults assigned as described below:

        **Default Spatial Parameters:**

        * resolution: the finest resolution of any band of all matching scenes
        * crs: the most common CRS used of all matching scenes
    """

    if isinstance(aoi, geocontext.GeoContext):
        ctx = aoi
        if ctx.bounds is None and ctx.geometry is None:
            raise ValueError(
                "Unspecified where to search, "
                "since the GeoContext given for ``aoi`` has neither geometry nor bounds set"
            )
    else:
        ctx = geocontext.AOI(geometry=aoi)

    if raster_client is None:
        raster_client = Raster()
    if metadata_client is None:
        metadata_client = Metadata()

    if isinstance(products, six.string_types):
        products = [products]

    if isinstance(start_datetime, datetime.datetime):
        start_datetime = start_datetime.isoformat()

    if isinstance(end_datetime, datetime.datetime):
        end_datetime = end_datetime.isoformat()

    metadata_params = dict(
        products=products,
        geom=ctx.__geo_interface__,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        cloud_fraction=cloud_fraction,
        storage_state=storage_state,
        limit=limit,
        sort_field=sort_field,
        sort_order=sort_order,
        date=date_field,
        q=query,
        randomize=randomize,
    )

    metadata = metadata_client.search(**metadata_params)
    if products is None:
        products = {meta["properties"]["product"] for meta in metadata["features"]}

    product_bands = {
        product: Scene._scenes_bands_dict(metadata_client.get_bands_by_product(product))
        for product in products
    }

    scenes = SceneCollection(
        (
            Scene(meta, product_bands[meta["properties"]["product"]])
            for meta in metadata["features"]
        ),
        raster_client=raster_client,
    )

    if len(scenes) > 0 and isinstance(ctx, geocontext.AOI):
        assign_ctx = {}
        if ctx.resolution is None and ctx.shape is None:
            resolutions = filter(
                None,
                (
                    b.get("resolution")
                    for band in six.itervalues(product_bands)
                    for b in six.itervalues(band)
                ),
            )
            try:
                assign_ctx["resolution"] = min(resolutions)
            except ValueError:
                assign_ctx[
                    "resolution"
                ] = None  # from min of an empty sequence; no band defines resolution

        if ctx.crs is None:
            assign_ctx["crs"] = collections.Counter(
                scene.properties["crs"] for scene in scenes
            ).most_common(1)[0][0]

        if len(assign_ctx) > 0:
            ctx = ctx.assign(**assign_ctx)

    return scenes, ctx
