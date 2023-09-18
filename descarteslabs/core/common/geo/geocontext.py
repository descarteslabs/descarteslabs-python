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


import copy
import threading
import warnings
import math

import reprlib
import mercantile
import shapely.geometry

from .. import shapely_support
from ..dltile import Tile, Grid

from .utils import (
    is_geographic_crs,
    is_wgs84_crs,
    polygon_from_bounds,
    valid_latlon_bounds,
)

EARTH_CIRCUMFERENCE_WGS84 = 2 * math.pi * 6378137


class GeoContext(object):
    """
    Specifies spatial parameters to use when loading a raster
    from the Descartes Labs catalog.

    Two Images loaded with the same GeoContext will result in images
    with the same shape (in pixels), covering the same spatial extent,
    regardless of the dimensions or projection of the original data.

    Specifically, a fully-defined GeoContext specifies:

    * geometry to use as a cutline (WGS84), and/or bounds
    * resolution (m) or a shape defining the extent in pixels
    * EPSG code of the output coordinate reference system
    * whether to align pixels to the output CRS
      (see docstring for `AOI.align_pixels` for more information)

    GeoContexts are immutable.
    """

    __slots__ = (
        "_geometry_lock_",
        "_all_touched",
    )
    # slots *suffixed* with an underscore will be ignored by `__eq__` and `__repr__`.
    # a double-underscore prefix would be more conventional, but that actually breaks as a slot name.

    def __init__(self, all_touched=False):
        """
        Parameters
        ----------
        all_touched: bool, default False
            If True, this ensures that any source pixel which intersects the
            AOI GeoContext contributes to the raster result. Normally this mode is
            not enabled, and its use is strongly discouraged. However, it can be
            useful when the AOI is smaller than a source pixel, which under many
            situations will return no result at all (i.e. entirely masked).
        """

        # Shapely objects are not thread-safe, due to the way the underlying GEOS library is used.
        # Specifically, accessing `__geo_interface__` on the same geometry across threads
        # can cause bizzare exceptions. This makes `raster_params` and `__geo_interface__` thread-unsafe.
        # Subclasses of GeoContext can use this lock to ensure `self._geometry.__geo_interface__`
        # is accessed from at most 1 thread at a time.
        self._geometry_lock_ = threading.Lock()
        self._all_touched = bool(all_touched)

    def __getstate__(self):
        # Lock objects shouldn't be pickled or deepcopied, but recursively get all the other slots
        return {
            attr: getattr(self, attr)
            for s in self.__class__.__mro__
            for attr in getattr(s, "__slots__", [])
            if not attr.endswith("_")
        }

    def __setstate__(self, state):
        for attr, val in state.items():
            setattr(self, attr, val)
        self._geometry_lock_ = threading.Lock()

    @property
    def all_touched(self):
        """
        bool: If True, this ensures that any source pixel which intersects the
        GeoContext contributes to the raster result.

        Normally this mode is not enabled, and its use is strongly discouraged.
        However, it can be useful when the AOI is smaller than a source pixel,
        which under many situations will return no result at all (i.e. entirely
        masked).
        """
        return self._all_touched

    @property
    def raster_params(self):
        """
        dict: The properties of this GeoContext,
        as keyword arguments to use for `Raster.ndarray` or `Raster.raster`.
        """

        raster_params = {}
        if self.all_touched:
            raster_params["cutline_all_touched"] = True

        return raster_params

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
            for s in self.__class__.__mro__
            for attr in getattr(s, "__slots__", [])
            if not attr.endswith("_")
        )
        return "{}({})".format(classname, props)


class AOI(GeoContext):
    """
    A GeoContext that clips imagery to a geometry, and/or to square bounds,
    with any output resolution and CRS.

    Examples
    --------

    .. code-block:: python

        cutline_aoi = dl.geo.AOI(my_geometry, resolution=40)
        aoi_with_cutline_disabled = cutline_aoi.assign(geometry=None)
        no_cutline_aoi = dl.geo.AOI(geometry=None, resolution=15, bounds=(-40, 35, -39, 36))
        aoi_without_auto_bounds = dl.geo.AOI(geometry=my_geometry, resolution=15, bounds=(-40, 35, -39, 36))
        aoi_with_specific_pixel_dimensions = dl.geo.AOI(geometry=my_geometry, shape=(200, 400))
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
        align_pixels=None,
        bounds=None,
        bounds_crs="EPSG:4326",
        shape=None,
        all_touched=False,
    ):
        """
        Parameters
        ----------
        geometry: GeoJSON-like dict, object with ``__geo_interface__``; optional
            When searching, filter for elements which intersect this geometry.
            When rastering, clip imagery to this geometry.
            Coordinates must be WGS84 (lat-lon).
            If :const:`None`, imagery will just be clipped to
            :py:attr:`~descarteslabs.common.gecontext.AOI.bounds`.
        resolution: float, optional
            Distance, in native units of the CRS, that the edge of each pixel
            represents on the ground. Do not assume this to always be either
            degrees or meters.
            Can only specify one of `resolution` and `shape`.
        crs: str, optional
            Coordinate Reference System into which imagery will be projected,
            expressed as an EPSG code (like :const:`EPSG:4326`), a PROJ.4 definition,
            or an OGC CRS Well-Known Text string.
        align_pixels: bool, optional, default True if resolution is not None
            If :const:`True`, this ensures that, in different images rasterized
            with this same AOI GeoContext, pixels ``(i, j)`` correspond
            to the same area in space. This is accomplished by snapping the
            coordinates of the origin (top-left corner of top-left pixel)
            to a non-fractional interval of `resolution`. Note that in cases
            where `shape` has been specified, this may lead to the resulting
            image being one pixel larger in each dimension, so the the entire
            bounds is included.

            If `align_pixels` is :const:`False`, when using imagery with different
            native resolutions and/or projections, pixels at the same indices
            can be misaligned by a fraction of `resolution`
            (i.e. correspond to *slighly* different coordinates in space).

            However, this requires warping of the original image, which can be
            undesireable when you want to work with the original data in its
            native resolution and projection.
        bounds: 4-tuple, optional
            Clip imagery to these ``(min_x, min_y, max_x, max_y)`` bounds,
            expressed in :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds_crs`
            (which defaults to WGS84 lat-lon).
            :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds`
            are automatically computed from `geometry` if not specified.
            Otherwise,
            :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds` are required.
        bounds_crs: str, optional, default "EPSG:4326"
            The Coordinate Reference System of the
            :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds`,
            given as an EPSG code (like :const:`EPSG:4326`), a PROJ.4 definition,
            or an OGC CRS Well-Known Text string.
        shape: 2-tuple, optional
            ``(rows, columns)``, in pixels, the output raster should fit within;
            the longer side of the raster will be min(shape).
            Can only specify one of `resolution` and `shape`. Note that when
            `align_pixels` is :const:`True`, the actual resulting raster may
            be one pixel larger in each direction.
        all_touched: bool, default False
            If True, this ensures that any source pixel which intersects the
            AOI GeoContext contributes to the raster result. Normally this mode is
            not enabled, and its use is strongly discouraged. However, it can be
            useful when the AOI is smaller than a source pixel, which under many
            situations will return no result at all (i.e. entirely masked).
        """

        super(AOI, self).__init__(all_touched=all_touched)

        if bounds is None and geometry is not None:
            bounds = "update"

        # If no bounds were given, use the bounds of the geometry
        self._assign(
            geometry,
            resolution,
            crs,
            align_pixels,
            bounds,
            bounds_crs,
            shape,
            "unchanged",
        )
        self._validate()

    @property
    def geometry(self):
        """
        shapely geometry: Clip imagery to this geometry
        Coordinates must be WGS84 (lat-lon).
        If :const:`None`, imagery will just be clipped to
        :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds`.
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
        str: Coordinate reference system into which imagery will be projected,
        expressed as an EPSG code (like :const:`EPSG:4326`), a PROJ.4 definition,
        or an OGC CRS Well-Known Text string.
        """

        return self._crs

    @property
    def align_pixels(self):
        """
        bool: If True, this ensures that, in different images rasterized with
        this same AOI GeoContext, pixels ``(i, j)`` correspond to the
        same area in space. This is accomplished by snapping the coordinates of
        the origin (top-left corner of top-left pixel) to a non-fractional
        interval of `resolution`. Note that in cases where `shape` has been
        specified, this may lead to the resulting image being one pixel larger
        in each dimension, so the the entire bounds is included.

        If `align_pixels` is False, when using imagery with different native
        resolutions and/or projections, pixels at the same indicies can be
        misaligned by a fraction of ``resolution`` (i.e. correspond to *slighly*
        different coordinates in space).

        However, this requires warping of the original image, which can be
        undesireable when you want to work with the original data in its native
        resolution and projection.
        """

        if self._align_pixels is None:
            return self._resolution is not None
        else:
            return self._align_pixels

    @property
    def bounds(self):
        """
        tuple: Clip imagery to these ``(min_x, min_y, max_x, max_y)`` bounds,
        expressed in the coordinate reference system in
        :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds_crs`.
        """

        return self._bounds

    @property
    def bounds_crs(self):
        """
        str: The coordinate reference system of the
        :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds`,
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

        Raises ValueError if
        :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds`, `crs`,
        :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds_crs`,
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
        # align_pixels will always be True or False based on resolution
        # all_touched doesn't affect the spatial equivalence

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
            **super().raster_params,
            "cutline": cutline,
            "resolution": self._resolution,
            "srs": self._crs,
            "bounds_srs": self._bounds_crs,
            "align_pixels": self.align_pixels,
            "bounds": self._bounds,
            "dimensions": dimensions,
        }

    @property
    def __geo_interface__(self):
        """
        dict: :py:attr:`~descarteslabs.common.geo.geocontext.AOI.geometry` as a GeoJSON Geometry dict,
        otherwise
        :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds`
        as a GeoJSON Polygon dict if
        :py:attr:`~descarteslabs.common.geo.geocontext.AOI.geometry` is
        :const:`None` and
        :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds_crs`
        is :const:`EPSG:4326`, otherwise
        raises :exc:`RuntimeError`.
        """

        if self._geometry is not None:
            with self._geometry_lock_:
                # see comment in `GeoContext.__init__` for why we need to prevent
                # parallel access to `self._geometry.__geo_interface__`
                return self._geometry.__geo_interface__
        elif self._bounds is not None and is_wgs84_crs(self._bounds_crs):
            return polygon_from_bounds(self._bounds)
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
        all_touched="unchanged",
    ):
        """
        Return a copy of the AOI with the given values assigned.

        Note
        ----
            If you are assigning a new geometry and want bounds to updated as
            well, use ``bounds="update"``. This will also change
            :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds_crs`
            to :const:`EPSG:4326`, since the geometry's coordinates are in WGS84
            decimal degrees, so the new bounds determined from those coordinates
            must be in that CRS as well.

            If you assign
            :py:attr:`~descarteslabs.common.geo.geocontext.AOI.geometry`
            without changing
            :py:attr:`~descarteslabs.common.geo.geocontext.AOI.bounds`,
            the new AOI GeoContext will produce rasters with the same
            shape and covering the same spatial area as the old one, just with
            pixels masked out that fall outside your new geometry.

        Returns
        -------
        new : `AOI`
        """

        new = copy.deepcopy(self)
        new._assign(
            geometry,
            resolution,
            crs,
            align_pixels,
            bounds,
            bounds_crs,
            shape,
            all_touched,
        )
        new._validate()
        return new

    def _validate(self):
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

        # test that bounds are sane
        if self._bounds is not None:
            shapely_support.check_valid_bounds(self._bounds)

        # rough check that bounds values actually make sense for bounds_crs
        if self._bounds_crs is not None and self._bounds is not None:
            is_geographic, lon_wrap = is_geographic_crs(
                self._bounds_crs, with_lon_wrap=True
            )
            if is_geographic:
                # some whole-globe products are funky around the dateline. Try
                # to allow up to a 1/2 pixel slop there. This will generally only
                # occur with AOIs created automatically from Image properties.
                if self._resolution and self._crs and is_geographic_crs(self._crs):
                    tol = self._resolution / 2
                elif self._shape is not None:
                    tol = (
                        max(
                            (self._bounds[2] - self._bounds[0]) / self._shape[1],
                            (self._bounds[3] - self._bounds[1]) / self._shape[0],
                        )
                        / 2
                    )
                else:
                    tol = 0.001
                if not valid_latlon_bounds(self._bounds, tol, lon_wrap=lon_wrap):
                    raise ValueError(
                        "Bounds must be in lat-lon coordinates, "
                        "but the given bounds are outside [-90, 90] for y or [-180, 180] for x."
                    )
            else:
                if valid_latlon_bounds(self._bounds):
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

        # check that bounds and geometry actually intersect (if bounds in wgs84)
        if (
            self._geometry is not None
            and self._bounds is not None
            and is_wgs84_crs(self._bounds_crs)
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
            not self._all_touched
            and self._crs is not None
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
            if is_geographic_crs(self._crs):
                msg += "\nSince your CRS is in lat-lon coordinates, resolution must be given in decimal degrees."
            msg += (
                "\nIf you are intending to raster an area smaller than the source imagery resolution, then you"
                "should set an appropriate value of resolution, shape, or all_touched=True on the supplied AOI"
                " to signal your intentions."
            )

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
        self,
        geometry,
        resolution,
        crs,
        align_pixels,
        bounds,
        bounds_crs,
        shape,
        all_touched,
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
            # To avoid breaking existing code, avoid a conflict with shape.
            # getattr() to handle pre-init cases.
            if (
                getattr(self, "_resolution", None) is None
                and getattr(self, "_shape", None) is not None
            ):
                self._shape = None
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
        if all_touched != "unchanged":
            self._all_touched = bool(all_touched)


class DLTile(GeoContext):
    """
    A GeoContext that clips and projects imagery to a single DLTile.

    DLTiles allow you to define a grid of arbitrary spacing, resolution,
    and overlap that can cover the globe.

    DLTiles are always in a UTM projection.

    Example
    -------
    >>> import descarteslabs as dl
    >>> from descarteslabs.geo import DLTile
    >>> tile = DLTile.from_latlon(
    ...    lat=35.691,
    ...    lon=-105.944,
    ...    tilesize=512,
    ...    resolution=10,
    ...    pad=0
    ... )
    >>> product = dl.catalog.Product.get("usgs:landsat:oli-tirs:c2:l2:v0")  # doctest: +SKIP
    >>> images = product.images().intersects(tile).collect()  # doctest: +SKIP
    >>> images  # doctest: +SKIP
            ImageCollection of 558 images
            * Dates: Mar 18, 2013 to Sep 14, 2023
            * Products: usgs:landsat:oli-tirs:c2:l2:v0: 558
    >>> images.geocontext     # doctest: +SKIP
    DLTile(key='512:0:10.0:13:-17:771',
       resolution=10.0,
       tilesize=512,
       pad=0,
       crs='EPSG:32613',
       bounds=(412960.0, 3947520.0, 418080.0, 3952640.0),
       bounds_crs='EPSG:32613',
       geometry=<POLYGON ((-1....962 35.71...>,
       zone=13,
       ti=-17,
       tj=771,
       geotrans=(412960.0, 10.0, 0.0, 3952640.0, 0.0, -10.0),
       proj4='+proj=utm +z...s=m +no_defs ',
       wkt='PROJCS["WGS ...SG","32613"]]',
       all_touched=False)
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

    def __init__(self, dltile_dict, all_touched=False):
        """
        Constructs a DLTile from a parameter dictionary.
        It is preferred to use the
        :meth:`DLTile.from_latlon, :meth:`DLTile.from_shape`, or :meth:`DLTile.from_key`
        class methods to construct a DLTile GeoContext.

        Parameters
        ----------
        dltile_dict: Dict[Str, Any]
            Dictionary for the tile.
        all_touched: bool, default False
            If True, this ensures that any source pixel which intersects the
            AOI GeoContext contributes to the raster result. Normally this mode is
            not enabled, and its use is strongly discouraged. However, it can be
            useful when the AOI is smaller than a source pixel, which under many
            situations will return no result at all (i.e. entirely masked).
        """

        super(DLTile, self).__init__(all_touched=all_touched)

        if isinstance(dltile_dict["geometry"], shapely.geometry.polygon.Polygon):
            self._geometry = dltile_dict["geometry"]
        else:
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
    def from_latlon(cls, lat, lon, resolution, tilesize, pad, all_touched=False):
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
        all_touched: bool, default False
            If True, this ensures that any source pixel which intersects the
            AOI GeoContext contributes to the raster result. Normally this mode is
            not enabled, and its use is strongly discouraged. However, it can be
            useful when the AOI is smaller than a source pixel, which under many
            situations will return no result at all (i.e. entirely masked).

        Returns
        -------
        tile : DLTile

        Example
        -------
        >>> from descarteslabs.geo import DLTile
        >>> # make a tile with total size 100, centered on lat, lon
        >>> # total tilesize == tilesize + 2 * pad
        >>> params = {
        ...    "lat": 30.0131,
        ...    "lon": 31.2089,
        ...    "resolution": 10,
        ...    "tilesize": 2,
        ...    "pad": 49,
        ... }
        >>> tile = DLTile.from_latlon(**params)
        >>> tile.key
        '2:49:10.0:36:-8637:166079'
        >>> tile.geometry.centroid.xy  # doctest: +SKIP
        (array('d', [31.20899205942612]), array('d', [30.013121672688087]))
        """

        grid = Grid(resolution=resolution, tilesize=tilesize, pad=pad)
        tile = grid.tile_from_lonlat(lat=lat, lon=lon)
        return cls(tile.geocontext, all_touched=all_touched)

    @classmethod
    def from_shape(
        cls, shape, resolution, tilesize, pad, keys_only=False, all_touched=False
    ):
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
        keys_only : bool, default False
            Whether to return DLTile objects or only DLTile keys. Set to True when
            returning a large number of tiles and you do not need the full objects.
        all_touched: bool, default False
            If True, this ensures that any source pixel which intersects the
            AOI GeoContext contributes to the raster result. Normally this mode is
            not enabled, and its use is strongly discouraged. However, it can be
            useful when the AOI is smaller than a source pixel, which under many
            situations will return no result at all (i.e. entirely masked).

        Returns
        -------
        tiles : List[DLTile] or List[Str]

        Example
        -------
        >>> from descarteslabs.geo import DLTile
        >>> shape = {
        ... "type":"Feature",
        ... "geometry":{
        ...     "type":"Polygon",
        ...     "coordinates":[[
        ...            [-122.51140471760839,37.77130087547876],
        ...            [-122.45475646845254,37.77475476721895],
        ...            [-122.45303985468301,37.76657207194229],
        ...            [-122.51057242081689,37.763446782666094],
        ...            [-122.51140471760839,37.77130087547876]]]
        ...    },"properties": None
        ... }
        >>> tiles = DLTile.from_shape(
        ...    shape=shape,
        ...    resolution=1,
        ...    tilesize=500,
        ...    pad=0,
        ... )
        >>> len(tiles)
        31
        """

        grid = Grid(resolution=resolution, tilesize=tilesize, pad=pad)

        if grid._estimate_ntiles_from_shape(shape) > 50000:
            warnings.warn(
                "DLTile.from_shape will return a large number of tiles. "
                "Consider using DLTile.iter_from_shape instead."
            )

        tiles = grid.tiles_from_shape(shape=shape, keys_only=keys_only)
        if keys_only:
            result = [tile for tile in tiles]
        else:
            result = [cls(tile.geocontext, all_touched=all_touched) for tile in tiles]
        return result

    @classmethod
    def iter_from_shape(
        cls, shape, resolution, tilesize, pad, keys_only=False, all_touched=False
    ):
        """
        Return a iterator for DLTiles that intersect the given geometry.

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
        keys_only : bool, default False
            Whether to return DLTile objects or only DLTile keys. Set to True when
            returning a large number of tiles and you do not need the full objects.
        all_touched: bool, default False
            If True, this ensures that any source pixel which intersects the
            AOI GeoContext contributes to the raster result. Normally this mode is
            not enabled, and its use is strongly discouraged. However, it can be
            useful when the AOI is smaller than a source pixel, which under many
            situations will return no result at all (i.e. entirely masked).

        Returns
        -------
        Iterator of DLTiles or str

        Example
        -------
        >>> from descarteslabs.geo import DLTile
        >>> shape = {
        ... "type":"Feature",
        ... "geometry":{
        ...     "type":"Polygon",
        ...     "coordinates":[[
        ...            [-122.51140471760839,37.77130087547876],
        ...            [-122.45475646845254,37.77475476721895],
        ...            [-122.45303985468301,37.76657207194229],
        ...            [-122.51057242081689,37.763446782666094],
        ...            [-122.51140471760839,37.77130087547876]]]
        ...    },"properties": None
        ... }
        >>> gen = DLTile.from_shape(
        ...    shape=shape,
        ...    resolution=1,
        ...    tilesize=500,
        ...    pad=0,
        ...    keys_only=True
        ... )
        >>> tiles = [tile for tile in gen]  # doctest: +SKIP
        >>> tiles[0]                        # doctest: +SKIP
        '500:0:1.0:10:94:8359'
        """

        grid = Grid(resolution=resolution, tilesize=tilesize, pad=pad)
        tiles = grid.tiles_from_shape(shape=shape, keys_only=keys_only)
        for tile in tiles:
            if keys_only:
                yield tile
            else:
                yield cls(tile.geocontext, all_touched=all_touched)

    @classmethod
    def from_key(cls, dltile_key, all_touched=False):
        """
        Return a DLTile GeoContext from a DLTile key.

        Parameters
        ----------
        dltile_key : str
            DLTile key, e.g. '128:16:960.0:15:-1:37'
        all_touched: bool, default False
            If True, this ensures that any source pixel which intersects the
            AOI GeoContext contributes to the raster result. Normally this mode is
            not enabled, and its use is strongly discouraged. However, it can be
            useful when the AOI is smaller than a source pixel, which under many
            situations will return no result at all (i.e. entirely masked).

        Returns
        -------
        tile: DLTile

        Example
        -------
        >>> from descarteslabs.geo import DLTile
        >>> tile = DLTile.from_key("2048:16:30.0:15:3:80")
        >>> tile            # doctest: +SKIP
        DLTile(key='2048:16:30.0:15:3:80',
               resolution=30.0,
               tilesize=2048,
               pad=16,
               crs='EPSG:32615',
               bounds=(683840.0, 4914720.0, 746240.0, 4977120.0),
               bounds_crs='EPSG:32615',
               geometry=<shapely.geom...>,
               zone=15,
               ti=3,
               tj=80,
               geotrans=[
        ...
        """

        tile = Tile.from_key(dltile_key)
        return cls(tile.geocontext, all_touched=all_touched)

    def subtile(self, subdivide, resolution=None, pad=None, keys_only=False):
        """
        Return an iterator for new DLTiles that subdivide this tile.

        The DLtile will be sub-divided into subdivide^2 total sub-tiles each with a side length
        of tile_size / subdivide. The resulting sub-tile size must be an integer.
        Each sub-tile will by default inherit the same resolution and pad as the orginal tile.

        Parameters
        ----------
        subdivide : int
            The value to subdivide the tile. The total number of sub-tiles will be the
            square of this value. This value must evenly divide the original tilesize.
        resolution : None, float
            A new resolution for the sub-tiles. None defaults to the original DLTile resolution.
            The new resolution must evenly divide the the original tilesize divided by
            the subdivide ratio.
        pad : None, int
            A new pad value for the sub-tiles. None defaults to the original DLTile pad value.
        keys_only : bool, default False
            Whether to return DLTile objects or only DLTile keys. Set to True when returning a large number of tiles
            and you do not need the full objects.

        Returns
        -------
        Iterator over DLTiles or str

        Example:
        -------
        >>> from descarteslabs.geo import DLTile
        >>> tile = DLTile.from_key("2048:0:30.0:15:3:80")
        >>> tiles = [tile for tile in tile.subtile(8)]
        >>> len(tiles)
        64
        >>> tiles[0].tilesize
        256
        """

        subtiles = Tile.from_key(self.key).subtile(
            subdivide=subdivide,
            new_resolution=resolution,
            new_pad=pad,
        )

        for tile in subtiles:
            if keys_only:
                yield tile.key
            else:
                yield DLTile(tile.geocontext, all_touched=self.all_touched)

    def rowcol_to_latlon(self, row, col):
        """
        Convert pixel coordinates to lat, lon coordinates

        Parameters
        ----------
        row : int or List[int]
            Pixel row coordinate or coordinates
        col : int or List[int]
            Pixel column coordinate or coordinates

        Returns
        -------
        coords : List[Tuple[float], Tuple[float]]
            List with the first element the latitude values and the second element longitude values

        Example
        -------
        >>> from descarteslabs.geo import DLTile
        >>> tile = DLTile.from_key("2048:0:30.0:15:3:80")
        >>> tile.rowcol_to_latlon(row=56, col=1111)
        [(44.894653081367544,), (-90.24334206726267,)]
        """

        lonlat = Tile.from_key(self.key).rowcol_to_lonlat(row=row, col=col)
        lonlat = lonlat.tolist()
        if isinstance(lonlat[0], (int, float)):
            result = [(lonlat[1],), (lonlat[0],)]
        else:
            result = list(zip(*lonlat))
            result[0], result[1] = result[1], result[0]
        return result

    def latlon_to_rowcol(self, lat, lon):
        """
        Convert lat, lon coordinates to pixel coordinates

        Parameters
        ----------
        lat: float or List[float]
            Latitude coordinate or coordinates
        lon: float or List[float]
            Longitude coordinate or coordinates

        Returns
        -------
        coords: List[Tuple[int] Tuple[int]]
            Tuple with the first element the row values and the second element column values

        Example
        -------
        >>> from descarteslabs.geo import DLTile
        >>> tile = DLTile.from_key("2048:0:30.0:15:3:80")
        >>> tile.latlon_to_rowcol(lat=44.8, lon=-90.2)
        [(403,), (1237,)]
        """

        rowcol = Tile.from_key(self.key).lonlat_to_rowcol(lat=lat, lon=lon)
        rowcol = rowcol.tolist()
        if isinstance(rowcol[0], (int, float)):
            result = [(rowcol[0],), (rowcol[1],)]
        else:
            result = list(zip(*rowcol))
        return result

    def assign(self, pad="unchanged", all_touched="unchanged"):
        """
        Return a copy of the DLTile with the pad and/or all_touched value modified.

        Parameters
        ----------
        pad : int, default "unchanged"
            New pad value
        all_touched : bool, default "unchanged"
            New all_touched value

        Returns
        -------
        tile : DLTile

        Example:
        --------
        >>> from descarteslabs.geo import DLTile
        >>> tile = DLTile.from_key("2048:16:30.0:15:3:80")
        >>> tile.pad
        16
        >>> tile = tile.assign(123)
        >>> tile.pad
        123
        """

        tile = Tile.from_key(self.key)
        if pad != "unchanged":
            tile = tile.assign(pad=pad)
        if all_touched == "unchanged":
            all_touched = self.all_touched
        return DLTile(tile.geocontext, all_touched=all_touched)

    @property
    def key(self):
        """
        str: The DLTile's key, which encodes the tiling parameters,
        and which number in the grid this tile is.
        """

        return self._key

    @property
    def resolution(self):
        """float: Distance, in meters, that the edge of each pixel represents on the ground"""

        return self._resolution

    @property
    def tilesize(self):
        """
        int: Length of each side of the tile, in pixels.
        Note that the total number of pixels along each side of an image is
        ``tile_size + 2 * padding``
        """

        return self._tilesize

    @property
    def tile_extent(self):
        """
        int: total extent of geocontext length in pixels, including pad.
        Size is ``tile_size + 2 * pad``.
        """

        return self._tilesize + 2 * self._pad

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
        str: Coordinate reference system into which imagery will be projected.
        For DLTiles, this is always a UTM projection, given as an EPSG code.
        """

        return self._crs

    @property
    def bounds(self):
        """
        tuple: The ``(min_x, min_y, max_x, max_y)`` of the area covered by
        this DLTile, in the UTM coordinate reference system given in
        :py:attr:`~descarteslabs.common.geo.geocontext.DLTile.bounds_crs`.
        """

        return self._bounds

    @property
    def bounds_crs(self):
        """
        str: The coordinate reference system of the
        :py:attr:`~descarteslabs.common.geo.geocontext.DLTile.bounds`,
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
        """int: The UTM zone of this tile"""

        return self._zone

    @property
    def ti(self):
        """int: The y-index of this tile in its grid"""

        return self._ti

    @property
    def tj(self):
        """int: The x-index of this tile in its grid"""

        return self._tj

    @property
    def raster_params(self):
        """
        dict: The properties of this DLTile,
        as keyword arguments to use for `Raster.ndarray` or `Raster.raster`.
        """

        return {
            **super().raster_params,
            "dltile": self._key,
            # QUESTION: shouldn't align_pixels be True?
            # based on the GDAL documentation for `-tap`, seems like that should be true
            # to ensure that pixels of images with different resolutions/projections
            # are aligned with the same dltile. otherwise, pixel (0,0) in 1 image could be at
            # different coordinates than the other
            "align_pixels": False,
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
        """str: PROJ.4 definition for this DLTile's coordinate reference system"""

        return self._proj4

    @property
    def wkt(self):
        """str: OGC Well-Known Text definition for this DLTile's coordinate reference system"""

        return self._wkt

    @property
    def __geo_interface__(self):
        """dict: :py:attr:`~descarteslabs.common.geo.geocontext.DLTile.geometry` as a GeoJSON Polygon"""

        with self._geometry_lock_:
            # see comment in `GeoContext.__init__` for why we need to prevent
            # parallel access to `self._geometry.__geo_interface__`
            return self._geometry.__geo_interface__


class XYZTile(GeoContext):
    """
    A GeoContext for XYZ tiles, such as those used in web maps.

    The tiles are always 256x256 pixels, in the spherical Mercator
    or "Web Mercator" coordinate reference system (:const:`EPSG:3857`).
    """

    __slots__ = ("_x", "_y", "_z")

    def __init__(self, x, y, z, all_touched=False):
        """
        Parameters
        ----------
        x: int
            X-index of the tile (increases going east)
        y: int
            Y-index of the tile (increases going south)
        z: int
            Zoom level of the tile
        all_touched: bool, default False
            If True, this ensures that any source pixel which intersects the
            AOI GeoContext contributes to the raster result. Normally this mode is
            not enabled, and its use is strongly discouraged. However, it can be
            useful when the AOI is smaller than a source pixel, which under many
            situations will return no result at all (i.e. entirely masked).
        """

        self._x = x
        self._y = y
        self._z = z
        super(XYZTile, self).__init__(all_touched=all_touched)

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
        str: Coordinate reference system into which common.geo will be projected.
        Always :const:`EPSG:3857` (spherical Mercator, aka "Web Mercator")
        """

        return "EPSG:3857"

    @property
    def bounds_crs(self):
        """
        str: The coordinate reference system of the
        :py:attr:`~descarteslabs.common.geo.geocontext.XYZTile.bounds`.
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
        "dict: :py:attr:`~descarteslabs.common.geo.geocontext.XYZTile.geometry` as a GeoJSON Polygon"

        return self.geometry.__geo_interface__

    @property
    def raster_params(self):
        """
        dict: The properties of this XYZTile,
        as keyword arguments to use for `Raster.ndarray` or `Raster.raster`.
        """

        return {
            **super().raster_params,
            "bounds": self.bounds,
            "srs": self.crs,
            "bounds_srs": self.bounds_crs,
            "align_pixels": False,
            "resolution": self.resolution,
        }
