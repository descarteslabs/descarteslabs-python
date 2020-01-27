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
"""


import copy
import shapely.geometry
import six
import threading
import warnings
import math

from six.moves import reprlib

from descarteslabs.client.addons import mercantile
from descarteslabs.client.services.raster import Raster
from descarteslabs.common import shapely_support

from . import _helpers


class GeoContext(object):
    """
    Specifies spatial parameters to use when loading a raster
    from the Descartes Labs catalog.

    Two Scenes loaded with the same GeoContext will result in images
    with the same shape (in pixels), covering the same spatial extent,
    regardless of the dimensions or projection of the original data.

    Specifically, a fully-defined GeoContext specifies:

    * geometry to use as a cutline (WGS84), and/or bounds
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
        return {
            attr: getattr(self, attr)
            for attr in self.__slots__
            if not attr.endswith("_")
        }

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
            for attr in self.__slots__
            if not attr.endswith("_")
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
        "_bounds_crs",
        "_shape",
    )

    def __init__(
        self,
        geometry=None,
        resolution=None,
        crs=None,
        align_pixels=True,
        bounds=None,
        bounds_crs="EPSG:4326",
        shape=None,
    ):
        """
        Parameters
        ----------
        geometry: GeoJSON-like dict, object with ``__geo_interface__``; optional
            Clip scenes to this geometry.
            Coordinates must be WGS84 (lat-lon).
            If :const:`None`, scenes will just be clipped to `bounds`.
        resolution: float, optional
            Distance, in units of the CRS, that the edge of each pixel
            represents on the ground.
            Can only specify one of `resolution` and `shape`.
        crs: str, optional
            Coordinate Reference System into which scenes will be projected,
            expressed as an EPSG code (like :const:`EPSG:4326`), a PROJ.4 definition,
            or an OGC CRS Well-Known Text string
        align_pixels: bool, optional, default True
            If True, this ensures that, in different Scenes rasterized
            with this same AOI GeoContext, pixels ``(i, j)`` correspond
            to the same area in space. This is accomplished by snapping the
            coordinates of the origin (top-left corner of top-left pixel)
            to a non-fractional interval of `resolution`.

            If `align_pixels` is False, when using scenes with different
            native resolutions and/or projections, pixels at the same indicies
            can be misaligned by a fraction of `resolution`
            (i.e. correspond to *slighly* different coordinates in space).

            However, this requires warping of the original image, which can be
            undesireable when you want to work with the original data in its
            native resolution and projection.
        bounds: 4-tuple, optional
            Clip scenes to these ``(min_x, min_y, max_x, max_y)`` bounds,
            expressed in `bounds_crs` (which defaults to WGS84 lat-lon).
            `bounds` are automatically computed from `geometry` if not specified.
            Otherwise, `bounds` are required.
        bounds_crs: str, optional, default "EPSG:4326"
            The Coordinate Reference System of the `bounds`,
            given as an EPSG code (like :const:`EPSG:4326`), a PROJ.4 definition,
            or an OGC CRS Well-Known Text string.
        shape: 2-tuple, optional
            ``(rows, columns)``, in pixels, the output raster should fit within;
            the longer side of the raster will be min(shape).
            Can only specify one of `resolution` and `shape`.
        """

        super(AOI, self).__init__()

        if bounds is None and geometry is not None:
            bounds = "update"

        # If no bounds were given, use the bounds of the geometry
        self._assign(geometry, resolution, crs, align_pixels, bounds, bounds_crs, shape)
        self._validate()

    @property
    def geometry(self):
        """
        shapely geometry: Clip scenes to this geometry
        Coordinates must be WGS84 (lat-lon).
        If :const:`None`, scenes will just be clipped to `bounds`.
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
        str: Coordinate reference system into which scenes will be projected,
        expressed as an EPSG code (like :const:`EPSG:4326`), a PROJ.4 definition,
        or an OGC CRS Well-Known Text string.
        """

        return self._crs

    @property
    def align_pixels(self):
        """
        bool: If True, this ensures that, in different
        `Scenes <descarteslabs.scenes.scene.Scene>` rasterized with
        this same AOI GeoContext, pixels ``(i, j)`` correspond to the
        same area in space. This is accomplished by snapping the coordinates of
        the origin (top-left corner of top-left pixel) to a non-fractional
        interval of `resolution`.

        If `align_pixels` is False, when using scenes with different native
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
        expressed in the coordinate reference system in `bounds_crs`.
        """

        return self._bounds

    @property
    def bounds_crs(self):
        """
        str: The coordinate reference system of the `bounds`,
        given as an EPSG code (like :const:`EPSG:4326`), a PROJ.4 definition,
        or an OGC CRS Well-Known Text string.
        """

        return self._bounds_crs

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
        dict: The properties of this `AOI`,
        as keyword arguments to use for
        :class:`~descarteslabs.client.services.raster.raster.Raster.ndarray` or
        :class:`~descarteslabs.client.services.raster.raster.Raster.raster`.

        Raises ValueError if `bounds`, `crs`, `bounds_crs`,
        `resolution`, or `align_pixels` is :const:`None`.
        """

        # Ensure that there can be no ambiguity: every parameter must be specified,
        # so every raster call using this context will return spatially equivalent data
        if self._bounds is None:
            raise ValueError("AOI must have bounds specified")
        if self._bounds_crs is None:
            raise ValueError("AOI must have bounds_crs specified")
        if self._crs is None:
            raise ValueError("AOI must have CRS specified")
        if self._resolution is None and self._shape is None:
            raise ValueError("AOI must have one of resolution or shape specified")
        if self._align_pixels is None:
            raise ValueError("AOI must have align_pixels specified")

        with self._geometry_lock_:
            # see comment in `GeoContext.__init__` for why we need to prevent
            # parallel access to `self._geometry.__geo_interface__`
            cutline = (
                self._geometry.__geo_interface__ if self._geometry is not None else None
            )

        dimensions = (
            (self._shape[1], self._shape[0]) if self._shape is not None else None
        )

        return {
            "cutline": cutline,
            "resolution": self._resolution,
            "srs": self._crs,
            "bounds_srs": self._bounds_crs,
            "align_pixels": self._align_pixels,
            "bounds": self._bounds,
            "dimensions": dimensions,
        }

    @property
    def __geo_interface__(self):
        """
        dict: `self.geometry` as a GeoJSON Geometry dict,
        otherwise `bounds` as a GeoJSON Polygon dict if `geometry` is
        :const:`None` and `bounds_crs` is :const:`EPSG:4326`, otherwise
        raises :exc:`RuntimeError`.
        """

        if self._geometry is not None:
            with self._geometry_lock_:
                # see comment in `GeoContext.__init__` for why we need to prevent
                # parallel access to `self._geometry.__geo_interface__`
                return self._geometry.__geo_interface__
        elif self._bounds is not None and _helpers.is_wgs84_crs(self._bounds_crs):
            return _helpers.polygon_from_bounds(self._bounds)
        else:
            raise RuntimeError(
                "AOI GeoContext must have a geometry set, or bounds set and a WGS84 `bounds_crs`, "
                "to have a __geo_interface__"
            )

    def assign(
        self,
        geometry="unchanged",
        resolution="unchanged",
        crs="unchanged",
        align_pixels="unchanged",
        bounds="unchanged",
        bounds_crs="unchanged",
        shape="unchanged",
    ):
        """
        Return a copy of the AOI with the given values assigned.

        Note
        ----
            If you are assigning a new geometry and want bounds to updated as
            well, use ``bounds="update"``. This will also change `bounds_crs`
            to :const:`EPSG:4326`, since the geometry's coordinates are in WGS84
            decimal degrees, so the new bounds determined from those coordinates
            must be in that CRS as well.

            If you assign `geometry` without changing `bounds`,
            the new AOI GeoContext will produce rasters with the same
            shape and covering the same spatial area as the old one, just with
            pixels masked out that fall outside your new geometry.

        Returns
        -------
        new : `AOI`
        """

        new = copy.deepcopy(self)
        new._assign(geometry, resolution, crs, align_pixels, bounds, bounds_crs, shape)
        new._validate()
        return new

    def _validate(self):
        # test that bounds are sane
        if self._bounds is not None:
            shapely_support.check_valid_bounds(self._bounds)

        # rough check that bounds values actually make sense for bounds_crs
        if self._bounds_crs is not None and self._bounds is not None:
            valid_latlon_bounds = _helpers.valid_latlon_bounds(self._bounds)
            if _helpers.is_geographic_crs(self._bounds_crs):
                if not valid_latlon_bounds:
                    raise ValueError(
                        "Bounds must be in lat-lon coordinates, "
                        "but the given bounds are outside [-90, 90] for y or [-180, 180] for x."
                    )
            else:
                if valid_latlon_bounds:
                    # Warn that bounds are probably in the wrong CRS.
                    # But we can't be sure without a proper tool for working with CRSs,
                    # since bounds that look like valid lat-lon coords
                    # *could* be valid in a different CRS, though unlikely.
                    warnings.warn(
                        "You might have the wrong `bounds_crs` set.\n"
                        "Bounds appear to be in lat-lon decimal degrees, but the `bounds_crs` "
                        "does not seem to be a geographic coordinate reference system "
                        "(i.e. its units are not degrees, but meters, feet, etc.).\n\n"
                        "If this is unexpected, set `bounds_crs='EPSG:4326'`."
                    )

        # validate shape
        if self._shape is not None:
            if not isinstance(self._shape, (list, tuple)) or len(self._shape) != 2:
                raise TypeError("Shape must be a tuple of (rows, columns) in pixels")

        # validate resolution
        if self._resolution is not None:
            if not isinstance(self._resolution, (int, float)):
                raise TypeError(
                    "Resolution must be an int or float, got type '{}'".format(
                        type(self._resolution).__name__
                    )
                )
            if self._resolution <= 0:
                raise ValueError("Resolution must be greater than zero")

        # can't set both resolution and shape
        if self._resolution is not None and self._shape is not None:
            raise ValueError("Cannot set both resolution and shape")

        # check that bounds and geometry actually intersect (if bounds in wgs84)
        if (
            self._geometry is not None
            and self._bounds is not None
            and _helpers.is_wgs84_crs(self._bounds_crs)
        ):
            bounds_shp = shapely.geometry.box(*self._bounds)
            if not bounds_shp.intersects(self._geometry):
                raise ValueError(
                    "Geometry and bounds do not intersect. This would result in all data being masked. "
                    "If you're assigning new geometry, assign new bounds as well "
                    "(use `bounds='update'` to use the bounds of the new geometry)."
                )

        # Helpful warning about a common mistake: resolution < width
        # The CRS of bounds and CRS of resolution must be the same to compare between those values

        # This most often happens when switching from a projected to a geodetic CRS (i.e. UTM to WGS84)
        # and not updating the (units of the) resolution accordingly, so you now have, say,
        # 30 decimal degrees as your resolution. Probably not what you meant.

        # TODO: better way of checking equivalence between CRSs than string equality
        if (
            self._crs is not None
            and self._resolution is not None
            and self._bounds is not None
            and self._bounds_crs == self._crs
        ):
            crs_width = self._bounds[2] - self._bounds[0]
            crs_height = self._bounds[3] - self._bounds[1]
            msg = (
                "Output raster's {dim} ({dim_len:.4f}) is smaller than its resolution "
                "({res:.4f}), meaning it would be less than one pixel {dim_adj}.\n"
                "Remember that resolution is specified in units of the output CRS, "
                "which are not necessarily meters."
            )
            if _helpers.is_geographic_crs(self._crs):
                msg += "\nSince your CRS is in lat-lon coordinates, resolution must be given in decimal degrees."

            if crs_width < self._resolution:
                raise ValueError(
                    msg.format(
                        dim="width",
                        dim_len=crs_width,
                        res=self._resolution,
                        dim_adj="wide",
                    )
                )
            if crs_height < self._resolution:
                raise ValueError(
                    msg.format(
                        dim="height",
                        dim_len=crs_height,
                        res=self._resolution,
                        dim_adj="tall",
                    )
                )

    def _assign(
        self, geometry, resolution, crs, align_pixels, bounds, bounds_crs, shape
    ):
        # we use "unchanged" as a sentinel value, because None is a valid thing to set attributes to.
        if geometry is not None and geometry != "unchanged":
            geometry = shapely_support.geometry_like_to_shapely(geometry)

        if bounds is not None and bounds != "unchanged":
            if bounds == "update":
                if bounds_crs not in (None, "unchanged", "EPSG:4326"):
                    raise ValueError(
                        "Can't compute bounds from a geometry while also explicitly setting a `bounds_crs`.\n\n"
                        "To resolve: don't set `bounds_crs`. It will be set to 'EPSG:4326' for you. "
                        "(Though you can do so explicitly if you'd like.)\n\n"
                        "Explanation: the coordinates in a geometry are latitudes and longitudes "
                        "in decimal degrees, defined in the WGS84 coordinate reference system "
                        "(referred to by the code EPSG:4326). When we infer `bounds` from a `geometry`, "
                        "those bounds will be in the same coordinate reference system as the geometry---i.e., WGS84. "
                        "Therefore, setting `bounds_crs` to anything besides 'EPSG:4326' doesn't make sense."
                    )
                bounds_crs = "EPSG:4326"
                if geometry is not None and geometry != "unchanged":
                    bounds = geometry.bounds
                else:
                    raise ValueError(
                        "A geometry must be given with which to update the bounds"
                    )
            else:
                bounds = tuple(bounds)

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
        if bounds_crs != "unchanged":
            self._bounds_crs = bounds_crs
        if shape != "unchanged":
            self._shape = shape


class DLTile(GeoContext):
    """
    A GeoContext that clips and projects
    :class:`Scenes <descarteslabs.scenes.scene.Scene>` to a single DLTile.

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
        "_bounds_crs",
        "_geometry",
        "_zone",
        "_ti",
        "_tj",
        "_geotrans",
        "_proj4",
        "_wkt",
    )

    def __init__(self, dltile_dict):
        """
        ``__init__`` instantiates a DLTile from a dict returned by `Raster.dltile`.

        It's preferred to use the `DLTile.from_latlon`, `DLTile.from_shape`,
        or `DLTile.from_key` class methods to construct a DLTile GeoContext.
        """

        super(DLTile, self).__init__()
        self._geometry = shapely.geometry.shape(dltile_dict["geometry"])
        properties = dltile_dict["properties"]
        self._key = properties["key"]
        self._resolution = properties["resolution"]
        self._tilesize = properties["tilesize"]
        self._pad = properties["pad"]
        self._crs = properties["cs_code"]
        self._bounds = tuple(properties["outputBounds"])
        self._bounds_crs = properties["cs_code"]
        self._zone = properties["zone"]
        self._ti = properties["ti"]
        self._tj = properties["tj"]

        # these properties may not be present
        self._geotrans = properties.get("geotrans", None)
        self._proj4 = properties.get("proj4", None)
        self._wkt = properties.get("wkt", None)

    @classmethod
    def from_latlon(cls, lat, lon, resolution, tilesize, pad, raster_client=None):
        """
        Return a DLTile GeoContext that covers a latitude/longitude.

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
        Return a list of DLTiles that intersect the given geometry.

        Parameters
        ----------
        shape : GeoJSON-like
            A GeoJSON dict, or object with a ``__geo_interface__``. Must be in
            :const:`EPSG:4326` (WGS84 lat-lon) projection.
        resolution : float
            Distance, in meters, that the edge of each pixel represents on the ground.
        tilesize : int
            Length of each side of the tile, in pixels.
        pad : int
            Number of extra pixels by which each side of the tile is buffered.
            This determines the number of pixels by which two tiles overlap.
        raster_client : `Raster`, optional, default :const:`None`.
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

        tiles_fc = raster_client.dltiles_from_shape(
            resolution=resolution, tilesize=tilesize, pad=pad, shape=shape
        )
        return [cls(tile) for tile in tiles_fc["features"]]

    @classmethod
    def from_key(cls, dltile_key, raster_client=None):
        """
        Return a DLTile GeoContext from a DLTile key.

        Parameters
        ----------
        dltile_key : str
            DLTile key, e.g. '128:16:960.0:15:-1:37'
        raster_client : `Raster`, optional, default :const:`None`.
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

    def assign(self, pad, raster_client=None):
        """
        Return a copy of the DLTile with the pad value modified.

        Returns
        -------
        new : `DLTile`
        """

        key = self._key.split(":")
        key[1] = str(int(pad))
        return self.from_key(":".join(key), raster_client=raster_client)

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
        str: Coordinate reference system into which scenes will be projected.
        For DLTiles, this is always a UTM projection, given as an EPSG code.
        """

        return self._crs

    @property
    def bounds(self):
        """
        tuple: The ``(min_x, min_y, max_x, max_y)`` of the area covered by
        this DLTile, in the UTM coordinate reference system given in `bounds_crs`.
        """

        return self._bounds

    @property
    def bounds_crs(self):
        """
        str: The coordinate reference system of the `bounds`,
        given as an EPSG code (like :const:`EPSG:32615`).
        A DLTile's CRS is always UTM.
        """

        return self._bounds_crs

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
    def geotrans(self):
        """
        tuple: The 6-tuple GDAL geotrans for this DLTile in the shape
        ``(a, b, c, d, e, f)`` where

        | a is the top left pixel's x-coordinate
        | b is the west-east pixel resolution
        | c is the row rotation, always 0 for DLTiles
        | d is the top left pixel's y-coordinate
        | e is the column rotation, always 0 for DLTiles
        | f is the north-south pixel resolution, always a negative value
        """

        if self._geotrans is None:
            return None

        return tuple(self._geotrans)

    @property
    def proj4(self):
        "str: PROJ.4 definition for this DLTile's coordinate reference system"

        return self._proj4

    @property
    def wkt(self):
        "str: OGC Well-Known Text definition for this DLTile's coordinate reference system"

        return self._wkt

    @property
    def __geo_interface__(self):
        "dict: `geometry` as a GeoJSON Polygon"

        with self._geometry_lock_:
            # see comment in `GeoContext.__init__` for why we need to prevent
            # parallel access to `self._geometry.__geo_interface__`
            return self._geometry.__geo_interface__


EARTH_CIRCUMFERENCE_WGS84 = 2 * math.pi * 6378137
"Circumference of the earth, in meters, on the WGS84 ellipsoid"


class XYZTile(GeoContext):
    """
    A GeoContext for XYZ tiles, such as those used in web maps.

    The tiles are always 256x256 pixels, in the spherical Mercator
    or "Web Mercator" coordinate reference system (:const:`EPSG:3857`).

    Requires the optional ``mercantile`` package.
    """

    __slots__ = ("_x", "_y", "_z")

    def __init__(self, x, y, z):
        """
        Parameters
        ----------
        x: int
            X-index of the tile (increases going east)
        y: int
            Y-index of the tile (increases going south)
        z: int
            Zoom level of the tile
        """

        self._x = x
        self._y = y
        self._z = z
        super(XYZTile, self).__init__()

    @property
    def x(self):
        "int: X-index of the tile (increases going east)"

        return self._x

    @property
    def y(self):
        "int: Y-index of the tile (increases going south)"

        return self._y

    @property
    def z(self):
        "int: Zoom level of the tile"

        return self._z

    def parent(self):
        "The parent XYZTile enclosing this one"

        return self.__class__(*mercantile.parent(self._x, self._y, self._z))

    def children(self):
        "List of child XYZTiles contained within this one"

        return [
            self.__class__(*t) for t in mercantile.children(self._x, self._y, self._z)
        ]

    @property
    def geometry(self):
        """
        shapely.geometry.Polygon: The polygon covered by this XYZTile
        in :const:`WGS84` (lat-lon) coordinates
        """

        return shapely.geometry.box(*mercantile.bounds(self._x, self._y, self._z))

    @property
    def bounds(self):
        """
        tuple: The ``(min_x, min_y, max_x, max_y)`` of the area covered by
        this XYZTile, in spherical Mercator coordinates (EPSG:3857).
        """

        return tuple(mercantile.xy_bounds(self._x, self._y, self._z))

    @property
    def crs(self):
        """
        str: Coordinate reference system into which scenes will be projected.
        Always :const:`EPSG:3857` (spherical Mercator, aka "Web Mercator")
        """

        return "EPSG:3857"

    @property
    def bounds_crs(self):
        """
        str: The coordinate reference system of the `bounds`.
        Always :const:`EPSG:3857` (spherical Mercator, aka "Web Mercator")
        """

        return "EPSG:3857"

    @property
    def tilesize(self):
        """
        int: Length of each side of the tile, in pixels. Always 256.
        """

        return 256

    @property
    def resolution(self):
        """
        float: Distance, in meters, that the edge of each pixel represents in the
        spherical Mercator ("Web Mercator", EPSG:3857) projection.
        """
        num_tiles = 1 << self.z
        return EARTH_CIRCUMFERENCE_WGS84 / num_tiles / self.tilesize

    @property
    def __geo_interface__(self):
        "dict: `geometry` as a GeoJSON Polygon"

        return self.geometry.__geo_interface__

    @property
    def raster_params(self):
        """
        dict: The properties of this XYZTile,
        as keyword arguments to use for `Raster.ndarray` or `Raster.raster`.
        """

        return {
            "bounds": self.bounds,
            "srs": self.crs,
            "bounds_srs": self.bounds_crs,
            "align_pixels": False,
            "resolution": self.resolution,
        }
