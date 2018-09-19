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

"""
Datasets in the Descartes Labs catalog have many different resolutions and
projections. In two different images, even covering the same place on Earth,
the pixels ``(i, j)`` usually correspond to two different points on the ground.

GeoContexts are a way to ensure multiple images from different sources
are **spatially compatible**---that is, they all have the same shape
(same width and height, in pixels), and the same pixel in each image
corresponds to the same area on Earth.

They do this by simply capturing all the spatial parameters that affect how
imagery is rasterized---namely output resolution, coordinate reference system,
and bounding box---in one object that can be passed into different method calls.
In typical use, these contexts are created for you with reasonable defaults,
so you only need to understand the different parameters when you need more control.

The different subclasses of `GeoContext` implement different
functionality.

* `AOI` clips to arbitrary geometry, and lets you specify any output resolution
  and projection.
* `DLTile` helps you split large regions up into a grid of any spacing and
  resolution, and represents a single tile in that grid, in UTM projection.

Tutorial
========

Often, you don't have to create GeoContexts yourself---an `AOI`
with default parameters is created for you by `scenes.search <scenes._search.search>`
and `Scene.from_id`.

.. ipython:: python

    import descarteslabs as dl

    scene, default_ctx = dl.scenes.Scene.from_id('landsat:LC08:PRE:TOAR:meta_LC80260322013188_v1')
    default_ctx

GeoContexts are immutable; instead, create copies with new values using `AOI.assign`.
(Assigning new values to DLTiles is not yet supported.)

.. ipython:: python

    # let's use a lower resolution to load images faster
    lowres = default_ctx.assign(resolution=75)
    lowres_arr = scene.ndarray("red green blue", lowres)
    @savefig geocontext1.png
    dl.scenes.display(lowres_arr, size=4, title="Default GeoContext, 75-meter resolution")

You can also create GeoContexts explicitly:

.. ipython:: python

    import shapely.affinity

    # make a new polygon half the size of the scene's full extent
    new_cutline = shapely.affinity.scale(scene.geometry, xfact=0.5, yfact=0.5)

    webmerc_cutline_aoi = dl.scenes.AOI(
        geometry=new_cutline,
        resolution=75,
        crs="EPSG:3857"  # "EPSG:3857" is the code for the Web Mercator
    )                    # coordinate reference system, see http://epsg.io/3857

    webmerc_cutline_arr = scene.ndarray("red green blue", webmerc_cutline_aoi)
    @savefig geocontext2.png
    dl.scenes.display(webmerc_cutline_arr, size=4, title="Same scene, with cutline and Web Mercator")

Let's assign our new cutline to the default GeoContext
to see the difference between the coordinate reference systems:

.. ipython:: python

    with_cutline = lowres.assign(geometry=new_cutline)

    with_cutline_arr = scene.ndarray("red green blue", with_cutline)
    @savefig geocontext3.png
    dl.scenes.display(with_cutline_arr, size=4, title="Original GeoContext with new cutline")

Why is there all that empty space around the sides?
We assigned a new geometry, but we didn't change the *bounds*.
Bounds determine the x-y extent that's rasterized; geometry just clips within that.
You can pass ``bounds="update"`` to compute new bounds when assinging a new geometry.

.. ipython:: python

    cutline_bounds = lowres.assign(geometry=new_cutline, bounds="update")
    cutline_bounds_arr = scene.ndarray("red green blue", cutline_bounds)
    @savefig geocontext4.png
    dl.scenes.display(cutline_bounds_arr, size=4, title="Original GeoContext, new cutline and bounds")

You can also use DLTiles to split up regions along a grid:

.. ipython:: python

    tiles = dl.scenes.DLTile.from_shape(
        new_cutline, resolution=75, tilesize=256, pad=16
    )
    len(tiles)

    tile0_arr = scene.ndarray("red green blue", tiles[0])
    tile1_arr = scene.ndarray("red green blue", tiles[1])

    @savefig geocontext5.png
    dl.scenes.display(tile0_arr, tile1_arr, title=[tiles[0].key, tiles[1].key], size=3)
"""

import copy
import shapely.geometry
import six
import threading

from geojson import Feature, FeatureCollection, GeometryCollection, GeoJSON
from six.moves import reprlib

from descarteslabs.client.services.raster import Raster


class GeoContext(object):
    """
    Specifies spatial parameters to use when loading a raster
    from the Descartes Labs catalog.

    Two Scenes loaded with the same GeoContext will result in images
    with the same shape (in pixels), covering the same spatial extent,
    regardless of the dimensions or projection of the original data.

    Specifically, a fully-defined GeoContext specifies:

    * geometry to use as a cutline (WGS84), and/or bounds (WGS84)
    * resolution (m)
    * EPSG code of the output coordinate reference system
    * whether to align pixels to the output CRS
      (see docstring for `AOI.align_pixels` for more information)

    GeoContexts are immutable.
    """
    __slots__ = "_geometry_lock_"
    # slots *suffixed* with an underscore will be ignored by `__eq__` and `__repr__`.
    # a double-underscore prefix would be more conventional, but that actually breaks as a slot name.

    def __init__(self):
        # Shapely objects are not thread-safe, due to the way the underlying GEOS library is used.
        # Specifically, accessing `__geo_interface__` on the same geometry across threads
        # can cause bizzare exceptions. This makes `raster_params` and `__geo_interface__` thread-unsafe,
        # which becomes an issue in `SceneCollection.stack` or `download`.
        # Subclasses of GeoContext can use this lock to ensure `self._geometry.__geo_interface__`
        # is accessed from at most 1 thread at a time.
        self._geometry_lock_ = threading.Lock()

    def __getstate__(self):
        # Lock objects shouldn't be pickled or deepcopied
        return {attr: getattr(self, attr) for attr in self.__slots__ if not attr.endswith("_")}

    def __setstate__(self, state):
        for attr, val in six.iteritems(state):
            setattr(self, attr, val)
        self._geometry_lock_ = threading.Lock()

    @property
    def raster_params(self):
        """
        dict: The properties of this GeoContext,
        as keyword arguments to use for `Raster.ndarray` or `Raster.raster`.
        """
        raise NotImplementedError

    def __eq__(self, other):
        """
        Two GeoContexts are equal only if they are the same type,
        and every property is equal.
        """
        if not isinstance(other, self.__class__):
            return False
        for attr in self.__slots__:
            if attr.endswith("_"):
                continue
            if getattr(self, attr) != getattr(other, attr):
                return False
        return True

    def __repr__(self):
        classname = self.__class__.__name__
        delim = ",\n" + " " * (len(classname) + 1)
        props = delim.join(
            "{}={}".format(attr.lstrip("_"), reprlib.repr(getattr(self, attr)))
            for attr in self.__slots__ if not attr.endswith("_")
        )
        return "{}({})".format(classname, props)


class AOI(GeoContext):
    """
    A GeoContext that clips scenes to a geometry, and/or to square bounds,
    with any output resolution and CRS.

    Examples
    --------

    .. code-block:: python

        cutline_aoi = dl.scenes.AOI(my_geometry, resolution=40)
        aoi_with_cutline_disabled = cutline_aoi.assign(geometry=None)
        no_cutline_aoi = dl.scenes.AOI(geometry=None, resolution=15, bounds=(-40, 35, -39, 36))
        aoi_without_auto_bounds = dl.scenes.AOI(geometry=my_geometry, resolution=15, bounds=(-40, 35, -39, 36))
        aoi_with_specific_pixel_dimensions = dl.scenes.AOI(geometry=my_geometry, shape=(200, 400))
    """
    __slots__ = (
        "_geometry",
        "_resolution",
        "_crs",
        "_align_pixels",
        "_bounds",
        "_shape",
    )

    def __init__(self,
                 geometry=None,
                 resolution=None,
                 crs=None,
                 align_pixels=True,
                 bounds=None,
                 shape=None,
                 ):
        """
        Parameters
        ----------
        geometry: GeoJSON-like dict, object with ``__geo_interface__``; optional
            Clip scenes to this geometry.
            Coordinates must be WGS84 (lat-lon).
            If None, scenes will just be clipped to ``bounds``.
        resolution: float, optional
            Distance, in units of the CRS, that the edge of each pixel
            represents on the ground.
            Can only specify one of ``resolution`` and ``shape``.
        crs: str, optional
            Coordinate Reference System into which scenes will be projected,
            expressed as an EPSG code (like ``"EPSG:4326"``), a PROJ.4 definition,
            or an OGC CRS Well-Known Text string
        align_pixels: bool, optional, default True
            If True, this ensures that, in different Scenes rasterized
            with this same AOI GeoContext, pixels ``(i, j)`` correspond
            to the same area in space. This is accomplished by snapping the
            coordinates of the origin (top-left corner of top-left pixel)
            to a non-fractional interval of ``resolution``.

            If ``align_pixels`` is False, when using scenes with different
            native resolutions and/or projections, pixels at the same indicies
            can be misaligned by a fraction of ``resolution``
            (i.e. correspond to *slighly* different coordinates in space).

            However, this requires warping of the original image, which can be
            undesireable when you want to work with the original data in its
            native resolution and projection.
        bounds: 4-tuple, optional
            Clip scenes to these ``(min_x, min_y, max_x, max_y)`` bounds,
            expressed in WGS84 (lat-lon) coordinates.
            ``bounds`` are automatically computed from ``geometry`` if not specified.
            Otherwise, ``bounds`` are required.
        shape: 2-tuple, optional
            ``(rows, columns)``, in pixels, the output raster should fit within;
            the longer side of the raster will be min(shape).
            Can only specify one of ``resolution`` and ``shape``.
        """
        super(AOI, self).__init__()

        self._validate_and_assign(
            geometry,
            resolution,
            crs,
            align_pixels,
            bounds,
            shape,
        )

        # If no bounds were given, use the bounds of the geometry
        if self._bounds is None and self._geometry is not None:
            self._bounds = self._geometry.bounds

    @property
    def geometry(self):
        """
        shapely geometry: Clip scenes to this geometry
        Coordinates must be WGS84 (lat-lon)
        If None, scenes will just be clipped to ``bounds``
        """
        return self._geometry

    @property
    def resolution(self):
        """
        float: Distance, in units of the CRS, that the edge of each pixel
        represents on the ground.
        """
        return self._resolution

    @property
    def crs(self):
        """
        str: Coordinate Reference System into which scenes will be projected,
        expressed as an EPSG code (like ``"EPSG:4326"``), a PROJ.4 definition,
        or an OGC CRS Well-Known Text string
        """
        return self._crs

    @property
    def align_pixels(self):
        """
        bool: If True, this ensures that, in different Scenes rasterized with
        this same AOI GeoContext, pixels ``(i, j)`` correspond to the
        same area in space. This is accomplished by snapping the coordinates of
        the origin (top-left corner of top-left pixel) to a non-fractional
        interval of ``resolution``.

        If ``align_pixels`` is False, when using scenes with different native
        resolutions and/or projections, pixels at the same indicies can be
        misaligned by a fraction of ``resolution`` (i.e. correspond to *slighly*
        different coordinates in space).

        However, this requires warping of the original image, which can be
        undesireable when you want to work with the original data in its native
        resolution and projection.
        """
        return self._align_pixels

    @property
    def bounds(self):
        """
        tuple: Clip scenes to these ``(min_x, min_y, max_x, max_y)`` bounds,
        expressed in WGS84 (lat-lon) coordinates.
        """
        return self._bounds

    @property
    def shape(self):
        """
        tuple: ``(rows, columns)``, in pixels, the output raster should fit within;
        the longer side of the raster will be min(shape).
        """
        return self._shape

    @property
    def raster_params(self):
        """
        dict: The properties of this AOI,
        as keyword arguments to use for ``Raster.ndarray`` or ``Raster.raster``.

        Raises ValueError if ``self.bounds``, ``self.crs``, ``self.resolution``,
        or ``self.align_pixels`` is None, or values are invalid.
        """
        # Ensure that there can be no ambiguity: every parameter must be specified,
        # so every raster call using this context will return spatially equivalent data
        if self._bounds is None:
            raise ValueError("AOI must have bounds specified")
        if self._crs is None:
            raise ValueError("AOI must have CRS specified")
        if self._resolution is None and self._shape is None:
            raise ValueError("AOI must have one of resolution or shape specified")
        if self._align_pixels is None:
            raise ValueError("AOI must have align_pixels specified")

        with self._geometry_lock_:
            # see comment in `GeoContext.__init__` for why we need to prevent
            # parallel access to `self._geometry.__geo_interface__`
            cutline = self._geometry.__geo_interface__ if self._geometry is not None else None

        dimensions = (self._shape[1], self._shape[0]) if self._shape is not None else None

        if self._resolution is not None and self._crs == "EPSG:4326":
            # helpful warning about a common mistake
            crs_width = self._bounds[2] - self._bounds[0]
            crs_height = self._bounds[3] - self._bounds[1]
            msg = (
                "Output raster's {dim} ({dim_len:.4f} decimal degrees) is smaller than its resolution "
                "({res:.4f} decimal degrees), meaning it would be less than one pixel {dim_adj}.\n"
                "Remember that resolution is specified in units of the output CRS. "
                "Since your CRS is EPSG:4326 (WGS84 lat-lon), resolution must be given in decimal degrees, not meters."
            )
            if crs_width < self._resolution:
                raise ValueError(msg.format(dim="width", dim_len=crs_width, res=self._resolution, dim_adj="wide"))
            if crs_height < self._resolution:
                raise ValueError(msg.format(dim="height", dim_len=crs_height, res=self._resolution, dim_adj="tall"))

        return {
            "cutline": cutline,
            "resolution": self._resolution,
            "srs": self._crs,
            "bounds_srs": "EPSG:4326",
            "align_pixels": self._align_pixels,
            "bounds": self._bounds,
            "dimensions": dimensions
        }

    @property
    def __geo_interface__(self):
        """
        dict: ``self.geometry`` as a GeoJSON Geometry dict,
        otherwise ``self.bounds`` as a GeoJSON Polygon dict if ``self.geometry`` is None.
        """
        if self._geometry is not None:
            with self._geometry_lock_:
                # see comment in `GeoContext.__init__` for why we need to prevent
                # parallel access to `self._geometry.__geo_interface__`
                return self._geometry.__geo_interface__
        elif self._bounds is not None:
            return self._polygon_from_bounds(self._bounds)

    def assign(self,
               geometry="unchanged",
               resolution="unchanged",
               crs="unchanged",
               align_pixels="unchanged",
               bounds="unchanged",
               shape="unchanged",
               ):
        """
        Return a copy of the AOI with the given values assigned.

        Note
        ----
            If you are assigning a new geometry and want bounds to updated as
            well, use ``bounds="update"``.

            If you assign ``geometry`` without changing ``bounds``,
            the new AOI GeoContext will produce rasters with the same
            shape and covering the same spatial area as the old one, just with
            pixels masked out that fall outside your new geometry.

        Returns
        -------
        new : `AOI`
        """
        new = copy.deepcopy(self)
        new._validate_and_assign(
            geometry,
            resolution,
            crs,
            align_pixels,
            bounds,
            shape,
        )
        return new

    def _validate_and_assign(self,
                             geometry,
                             resolution,
                             crs,
                             align_pixels,
                             bounds,
                             shape,
                             ):
        if geometry is not None and geometry != "unchanged":
            geometry = self._interpret_geometry_like(geometry)

        # test that bounds are sane, and in WGS84
        if bounds is not None and bounds != "unchanged":
            if bounds == "update":
                if geometry is not None:
                    bounds = geometry.bounds
                else:
                    raise ValueError("A geometry must be given with which to update the bounds")
            else:
                self._test_valid_bounds(bounds)
                bounds = tuple(bounds)

        if shape != "unchanged" and shape is not None:
            if not isinstance(shape, (list, tuple)) or len(shape) != 2:
                raise TypeError("Shape must be a tuple of (rows, columns) in pixels")

        if resolution != "unchanged" and resolution is not None:
            if not isinstance(resolution, (int, float)):
                raise TypeError("Resolution must be an int or float, got type '{}'".format(type(resolution).__name__))
            if resolution <= 0:
                raise ValueError("Resolution must be greater than zero")

        if geometry != "unchanged":
            self._geometry = geometry
        if resolution != "unchanged":
            self._resolution = resolution
        if crs != "unchanged":
            self._crs = crs
        if align_pixels != "unchanged":
            self._align_pixels = align_pixels
        if bounds != "unchanged":
            self._bounds = bounds
        if shape != "unchanged":
            self._shape = shape

        if self._resolution is not None and self._shape is not None:
            raise ValueError("Cannot set both resolution and shape")

        # test that bounds and geometry actually intersect
        if self._geometry is not None and self._bounds is not None:
            bounds_shp = shapely.geometry.box(*self._bounds)
            if not bounds_shp.intersects(self._geometry):
                raise ValueError(
                    "Geometry and bounds do not intersect. This would result in all data being masked. "
                    "If you're assigning new geometry, assign new bounds as well "
                    "(use `bounds='update'` to use the bounds of the new geometry)."
                )

    @classmethod
    def _polygon_from_bounds(cls, bounds):
        return {
            "type": "Polygon",
            "coordinates": ((
                (bounds[2], bounds[1]),
                (bounds[2], bounds[3]),
                (bounds[0], bounds[3]),
                (bounds[0], bounds[1]),
                (bounds[2], bounds[1]),
            ),)
        }

    @classmethod
    def _test_valid_bounds(cls, bounds):
        """
        Test given bounds are in correct order and are valid WGS84 lat-lon values

        Raises TypeError or ValueError if bounds are invalid
        """
        try:
            if not isinstance(bounds, (list, tuple)):
                raise TypeError()

            if len(bounds) != 4:
                raise ValueError(
                    "Bounds must a sequence of (minx, miny, maxx, maxy), "
                    "got sequence of length {}".format(len(bounds))
                )
        except TypeError:
            six.raise_from(
                TypeError("Bounds must a sequence of (minx, miny, maxx, maxy), got {}".format(type(bounds))),
                None
            )
        if not (-180 < bounds[0] < 180 and
                -90 < bounds[1] < 90 and
                -180 < bounds[2] < 180 and
                -90 < bounds[3] < 90):
            raise ValueError("Bounds must be in EPSG:4326 (WGS84 lat-lon) coordinates")

        if bounds[0] >= bounds[2]:
            raise ValueError("minx >= maxx in given bounds, should be (minx, miny, maxx, maxy)")
        if bounds[1] >= bounds[3]:
            raise ValueError("miny >= maxy in given bounds, should be (minx, miny, maxx, maxy)")

    @classmethod
    def _interpret_geometry_like(cls, geometry):
        geo_interface = getattr(geometry, '__geo_interface__', None)
        if geo_interface is None and not isinstance(geometry, dict):
            raise ValueError(
                "geometry not recognized as GeoJSON or geometry-like with a `__geo_interface__`: {}".format(geometry))

        # convert geometry to shapely
        if not isinstance(geometry, shapely.geometry.base.BaseGeometry):
            geometry_like = geometry if geo_interface else cls._as_geojson_instance(geometry)
            try:
                geometry = shapely.geometry.shape(geometry_like)
            except Exception:
                raise ValueError(
                    "Could not interpret the given geometry {} as a Shapely shape".format(geometry)
                )

        # test that geometry is in WGS84
        geom_bounds = geometry.bounds
        try:
            cls._test_valid_bounds(geom_bounds)
        except ValueError:
            six.raise_from(ValueError("Geometry must be in EPSG:4326 (WGS84 lat-lon) coordinates"), None)

        return geometry

    @classmethod
    def _as_geojson_instance(cls, geojson_dict):
        try:
            geojson = GeoJSON.to_instance(geojson_dict, strict=True)
        except (TypeError, KeyError, UnicodeEncodeError) as ex:
            raise ValueError("geometry not recognized as valid GeoJSON ({}): {}".format(str(ex), geojson_dict))
        # Shapely cannot handle GeoJSON Features or FeatureCollections
        if isinstance(geojson, Feature):
            geojson = geojson.geometry
        elif isinstance(geojson, FeatureCollection):
            features = []
            for feature in geojson.features:
                try:
                    features.append(GeoJSON.to_instance(feature, strict=True).geometry)
                except (TypeError, KeyError, UnicodeEncodeError) as ex:
                    raise ValueError(
                        "feature in FeatureCollection not recognized as valid ({}): {}".format(str(ex), feature))
            geojson = GeometryCollection(features)
        return geojson


class DLTile(GeoContext):
    """
    A GeoContext that clips and projects Scenes to a single DLTile.

    DLTiles allow you to define a grid of arbitrary spacing, resolution,
    and overlap that can cover the globe.
    DLTiles are always in a UTM projection.
    """
    __slots__ = (
        "_key",
        "_resolution",
        "_tilesize",
        "_pad",
        "_crs",
        "_bounds",
        "_geometry",
        "_zone",
        "_ti",
        "_tj",
    )

    def __init__(self, dltile_dict):
        """
        ``__init__`` instantiates a DLTile from a dict returned by `Raster.dltile`.

        It's preferred to use the `DLTile.from_latlon`, `DLTile.from_shape`,
        or `DLTile.from_key` class methods to construct a DLTile GeoContext.
        """
        super(DLTile, self).__init__()
        self._geometry = shapely.geometry.shape(dltile_dict['geometry'])
        properties = dltile_dict['properties']
        self._key = properties["key"]
        self._resolution = properties["resolution"]
        self._tilesize = properties["tilesize"]
        self._pad = properties["pad"]
        self._crs = properties["cs_code"]
        self._bounds = tuple(properties["outputBounds"])
        self._zone = properties["zone"]
        self._ti = properties["ti"]
        self._tj = properties["tj"]

    @classmethod
    def from_latlon(cls, lat, lon, resolution, tilesize, pad, raster_client=None):
        """
        Return a DLTile GeoContext that covers a latitude/longitude

        Where the point falls within the tile will vary, depending on the point
        and tiling parameters.

        Parameters
        ----------
        lat : float
            Latitude (WGS84)
        lon : float
            Longitude (WGS84)
        resolution : float
            Distance, in meters, that the edge of each pixel represents on the ground
        tilesize : int
            Length of each side of the tile, in pixels
        pad : int
            Number of extra pixels by which each side of the tile is buffered.
            This determines the number of pixels by which two tiles overlap.
        raster_client : descarteslabs.client.services.Raster, optional, default None
            Unneeded in general use; lets you use a specific client instance
            with non-default auth and parameters.

        Returns
        -------
        tile : DLTile
        """
        if raster_client is None:
            raster_client = Raster()
        tile = raster_client.dltile_from_latlon(lat, lon, resolution, tilesize, pad)
        return cls(tile)

    @classmethod
    def from_shape(cls, shape, resolution, tilesize, pad, raster_client=None):
        # TODO : non-overlapping tiles across UTM zones
        """
        Return a list of DLTiles that intersect the given geometry

        Parameters
        ----------
        shape : GeoJSON-like
            A GeoJSON dict, or object with a __geo_interface__. Must be in
            EPSG:4326 (WGS84 lat-lon) projection.
        resolution : float
            Distance, in meters, that the edge of each pixel represents on the ground
        tilesize : int
            Length of each side of the tile, in pixels
        pad : int
            Number of extra pixels by which each side of the tile is buffered.
            This determines the number of pixels by which two tiles overlap.
        raster_client : descarteslabs.client.services.Raster, optional, default None
            Unneeded in general use; lets you use a specific client instance
            with non-default auth and parameters.

        Returns
        -------
        tiles : List[DLTile]
        """
        if raster_client is None:
            raster_client = Raster()

        if hasattr(shape, "__geo_interface__"):
            shape = shape.__geo_interface__

        tiles_fc = raster_client.dltiles_from_shape(resolution=resolution, tilesize=tilesize, pad=pad, shape=shape)
        return [cls(tile) for tile in tiles_fc["features"]]

    @classmethod
    def from_key(cls, dltile_key, raster_client=None):
        """
        Return a DLTile GeoContext from a DLTile key.

        Parameters
        ----------
        dltile_key : str
            DLTile key, e.g. '128:16:960.0:15:-1:37'
        raster_client : descarteslabs.client.services.Raster, optional, default None
            Unneeded in general use; lets you use a specific client instance
            with non-default auth and parameters.

        Returns
        -------
        tile: DLTile
        """
        if raster_client is None:
            raster_client = Raster()
        tile = raster_client.dltile(dltile_key)
        return cls(tile)

    @property
    def key(self):
        """
        str: The DLTile's key, which encodes the tiling parameters,
        and which number in the grid this tile is.
        """
        return self._key

    @property
    def resolution(self):
        "float: Distance, in meters, that the edge of each pixel represents on the ground"
        return self._resolution

    @property
    def tilesize(self):
        """
        int: Length of each side of the tile, in pixels.
        Note that the total number of pixels along each side of an image is
        ``tile_size + 2*padding``
        """
        return self._tilesize

    @property
    def pad(self):
        """
        int: Number of extra pixels by which each side of the tile is buffered.
        This determines the number of pixels by which two tiles overlap.
        """
        return self._pad

    @property
    def crs(self):
        """
        str: Coordinate Reference System into which scenes will be projected.
        For DLTiles, this is always a UTM projection, given as an EPSG code.
        """
        return self._crs

    @property
    def bounds(self):
        """
        tuple: The ``(min_x, min_y, max_x, max_y)`` of the area covered by
        this DLTile, in UTM coordinates
        """
        # QUESTION: should this be in WGS84 to be consistent with the rest of GeoContext
        return self._bounds

    @property
    def geometry(self):
        """
        shapely.geometry.Polygon: The polygon covered by this DLTile
        in WGS84 (lat-lon) coordinates
        """
        return self._geometry

    @property
    def zone(self):
        "int: The UTM zone of this tile"
        return self._zone

    @property
    def ti(self):
        "int: The y-index of this tile in its grid"
        return self._ti

    @property
    def tj(self):
        "int: The x-index of this tile in its grid"
        return self._tj

    @property
    def raster_params(self):
        """
        dict: The properties of this DLTile,
        as keyword arguments to use for `Raster.ndarray` or `Raster.raster`.
        """
        return {
            "dltile": self._key,
            "align_pixels": False
            # QUESTION: shouldn't align_pixels be True?
            # based on the GDAL documentation for `-tap`, seems like that should be true
            # to ensure that pixels of images with different resolutions/projections
            # are aligned with the same dltile. otherwise, pixel (0,0) in 1 image could be at
            # different coordinates than the other
        }

    @property
    def __geo_interface__(self):
        "dict: ``self.geometry`` as a GeoJSON Polygon"
        with self._geometry_lock_:
            # see comment in `GeoContext.__init__` for why we need to prevent
            # parallel access to `self._geometry.__geo_interface__`
            return self._geometry.__geo_interface__

# TODO: XYZTile?
