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

import six
import collections
import datetime

from descarteslabs.client.services.raster import Raster
from descarteslabs.client.services.metadata import Metadata

from .scene import Scene
from .scenecollection import SceneCollection
from . import geocontext

MAX_RESULT_WINDOW = 10000


def search(aoi,
           products=None,
           start_datetime=None,
           end_datetime=None,
           cloud_fraction=None,
           limit=100,
           sort_field=None,
           sort_order='asc',
           date_field='acquired',
           query=None,
           randomize=False,
           raster_client=None,
           metadata_client=None
           ):
    """
    Search for Scenes in the Descartes Labs catalog.

    Returns a SceneCollection of Scenes that overlap with an area of interest,
    and meet the given search criteria.

    Parameters
    ----------
    aoi : GeoJSON-like dict, GeoContext, or object with __geo_interface__
        Search for scenes that intersect this area by any amount.
        If a GeoContext, a copy is returned as ``ctx``, with missing values filled in.
        Otherwise, the returned ``ctx`` will be an `AOI`, with this as its geometry.
    products : str or List[str], optional
        Descartes Labs product identifiers
    start_datetime : str, datetime-like, optional
        Restrict to scenes acquired after this datetime
    end_datetime : str, datetime-like, optional
        Restrict to scenes acquired before this datetime
    cloud_fraction : float, optional
        Restrict to scenes that are covered in clouds by less than this fraction
        (between 0 and 1)
    limit : int, optional
        Maximum number of Scenes to return, up to 10000.
    sort_field : str, optional
        Field name in ``Scene.properties`` by which to order the results
    sort_order : str, optional, default 'asc'
        ``"asc"`` or ``"desc"``
    date_field : str, optional, default 'acquired'
        The field used when filtering by date
        (``"acquired"``, ``"processed"``, ``"published"``)
    query : descarteslabs.common.property_filtering.Expression, optional
        Expression used to filter Scenes by their properties, built from ``dl.properties``.

        >>> query = 150 < dl.properties.azimuth_angle < 160 & dl.properties.cloud_fraction < 0.5
        >>> query = dl.properties.sat_id == "Terra"
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
    scenes : SceneCollection
        Scenes matching your criteria.
    ctx: GeoContext
        The given ``aoi`` as a GeoContext (if it isn't one already),
        with reasonable default parameters for loading all matching Scenes.

        If ``aoi`` was a `GeoContext`, ``ctx`` will be a copy of ``aoi``,
        with any properties that were ``None`` assigned the defaults below.

        If ``aoi`` was not a `GeoContext`, an `AOI` instance will be created
        with ``aoi`` as its geometry, and defaults assigned as described below:

        **Default Spatial Parameters:**

        * resolution: the finest resolution of any band of all matching scenes
        * crs: the most common CRS used of all matching scenes
    """

    if isinstance(aoi, geocontext.GeoContext):
        ctx = aoi
        if ctx.bounds is None and ctx.geometry is None:
            raise ValueError("Unspecified where to search, "
                             "since the GeoContext given for ``aoi`` has neither geometry nor bounds set")
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

    if limit > MAX_RESULT_WINDOW:
        raise ValueError("Limit must be <= {}".format(MAX_RESULT_WINDOW))

    metadata_params = dict(
        products=products,
        geom=ctx.__geo_interface__,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        cloud_fraction=cloud_fraction,
        limit=limit,
        sort_field=sort_field,
        sort_order=sort_order,
        date=date_field,
        q=query,
        randomize=randomize
    )

    metadata = metadata_client.search(**metadata_params)
    if products is None:
        products = {meta["properties"]["product"] for meta in metadata["features"]}

    product_bands = {
        product: Scene._scenes_bands_dict(metadata_client.get_bands_by_product(product))
        for product in products
    }

    scenes = SceneCollection(
        (Scene(meta, product_bands[meta["properties"]["product"]])
            for meta in metadata["features"]),
        raster_client=raster_client
    )

    if len(scenes) > 0:
        assign_ctx = {}
        if ctx.resolution is None and ctx.shape is None:
            resolutions = filter(
                None,
                (b.get("resolution") for band in six.itervalues(product_bands) for b in six.itervalues(band))
            )
            try:
                assign_ctx["resolution"] = min(resolutions)
            except ValueError:
                assign_ctx["resolution"] = None  # from min of an empty sequence; no band defines resolution

        if ctx.crs is None:
            assign_ctx["crs"] = collections.Counter(scene.properties["crs"] for scene in scenes).most_common(1)[0][0]

        if len(assign_ctx) > 0:
            ctx = ctx.assign(**assign_ctx)

    return scenes, ctx
