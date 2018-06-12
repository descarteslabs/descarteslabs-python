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
The Scene class holds metadata about a single scene in the Descartes Labs catalog.

Example
-------
>>> import descarteslabs as dl
>>> scene, ctx = dl.scenes.Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
>>> ctx  # a default GeoContext to use when loading raster data from this Scene
AOI(geometry=None,
    resolution=60,
    crs='EPSG:32615',
    align_pixels=True,
    bounds=(-95.8364984, 40.703737, -93.1167728, 42.7999878),
    dimensions=None)
>>> scene.properties.id
'landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1'
>>> scene.properties.date
datetime.datetime(2016, 7, 6, 16, 59, 42, 753476)
>>> scene.properties.bands.red.resolution
15
>>> arr = scene.ndarray(ctx, "red green blue")
>>> type(arr)
numpy.ma.core.MaskedArray
>>> arr.shape
(3, 3815, 3836)
"""

from __future__ import division
import six
import json
import datetime

from descarteslabs.client.addons import ThirdParty, shapely, numpy as np

from descarteslabs.client.services.raster import Raster
from descarteslabs.client.services.metadata import Metadata
from descarteslabs.client.exceptions import NotFoundError, BadRequestError

from . import geocontext


class Scene(object):
    """
    Object holding metadata about a single scene in the Descartes Labs catalog.

    A Scene is structured like a GeoJSON Feature, with geometry and properties.

    Attributes
    ----------
    geometry : shapely.geometry.Polygon or dict
        The region the scene's data covers, in WGS84 (lat-lon) coordinates.
        If the Shapely package is installed, it will be a shapely Polygon,
        otherwise a dict of a GeoJSON Polygon
    properties : DotDict
        Metadata about the scene. Some fields will vary between products,
        but these will be present:

        * id : str
            Descartes Labs ID of this scene
        * crs : str
            Native coordinate reference system of the scene,
            as an EPSG code or PROJ.4 definition
        * date : datetime.datetime
            ``'acquired'`` date parsed as a Python datetime if set,
            otherwise None
        * bands : DotDict[str, DotDict]
            Metadata about the bands available in the scene,
            as the mapping {band name: band metadata}
            Each band metadata dict should contain these fields:

            * id : str
                Descartes Labs ID of the band;
                unique to every band of every product
            * name : str
                Human-readable name of the band
            * dtype : str
                Native type in which the band's data is stored
            * data_range : list
                List of [min, max] values the band's data can have

            These fields are useful and available in most products,
            but may not always be available:

            * resolution : float
                Native resolution of the band, in ``resolution_unit``,
                that the edge of each pixel represents on the ground
            * resolution_unit : str
                Units of ``resolution`` field, such as ``"m"``
            * physical_range : list
                [min, max] range of values the band's data *represents*.
                Values of data have physical meaning
                (such as a reflectance fraction from 0-1), but often
                those values are remapped to a different numerical range
                for more efficient storage (since fixed-point integers require
                less space than floats). To return data to numbers
                with physical meaning, they should be mapped
                from ``data_range`` to ``physical_range``.
            * wavelength_min
                Minimum wavelength captured by the sensor in this band
            * wavelength_center
                Central wavelength captured by the sensor in this band
            * wavelength_max
                Maximum wavelength captured by the sensor in this band
            * wavelength_unit
                Units of the wavelength fields, such as ``"nm"``
    """
    def __init__(self, scene_dict, bands_dict):
        """
        ``__init__`` instantiates a Scene from a dict returned by `Metadata.search`
        and `Metadata.get_bands_by_id`.

        It's preferred to use `Scene.from_id` or `scenes.search` instead.
        """
        self.geometry = (shapely.geometry.shape(scene_dict["geometry"])
                         if not isinstance(shapely, ThirdParty)
                         else scene_dict["geometry"])
        properties = scene_dict["properties"]
        properties["id"] = scene_dict["id"]
        properties["bands"] = {b["name"]: b for b in six.itervalues(bands_dict)}
        properties["crs"] = (properties.pop("cs_code")
                             if "cs_code" in properties
                             else properties.get("proj4"))
        try:
            properties["date"] = datetime.datetime.strptime(properties["acquired"], '%Y-%m-%dT%H:%M:%S.%fZ')
        except KeyError:
            properties["date"] = None
        except ValueError:
            try:
                properties["date"] = datetime.datetime.strptime(properties["acquired"], '%Y-%m-%dT%H:%M:%S.%f+00:00')
            except ValueError:
                properties["date"] = None
        self.properties = properties

    @classmethod
    def from_id(cls, scene_id, metadata_client=None):
        """
        Return the metadata for a Descartes Labs scene ID as a Scene object.

        Also returns a GeoContext with reasonable defaults to use
        when loading the Scene's ndarray.

        Parameters
        ----------
        scene_id: str
            Descartes Labs scene ID,
            e.g. "landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1"
        metadata_client : Metadata, optional
            Unneeded in general use; lets you use a specific client instance
            with non-default auth and parameters.

        Returns
        -------
        scene: Scene
            Scene instance with metadata loaded from the Descartes Labs catalog
        ctx: AOI
            A default GeoContext useful for loading this Scene.
            These defaults are used:

            * bounds: bounds of the Scene's geometry
            * resolution: the finest resolution of any band in the scene
            * crs: native CRS of the Scene (generally, a UTM CRS)
            * align_pixels: True

        Example
        -------
        >>> import descarteslabs as dl
        >>> scene, ctx = dl.scenes.Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80260322016197_v1")
        >>> ctx
        AOI(geometry=None,
            resolution=60,
            crs='EPSG:32615',
            align_pixels=True,
            bounds=(-94.724166, 39.2784859, -92.0686956, 41.3717716),
            dimensions=None)
        >>> scene.properties.date
        datetime.datetime(2016, 7, 15, 16, 53, 59, 495435)

        Raises
        ------
        NotFoundError
            If the ``scene_id`` cannot be found in the Descartes Labs catalog
        """
        if metadata_client is None:
            metadata_client = Metadata()

        metadata = metadata_client.get(scene_id)
        metadata = {
            "type": "Feature",
            "geometry": metadata.pop("geometry"),
            "id": metadata.pop("id"),
            "key": metadata.pop("key"),
            "properties": metadata
        }

        bands = metadata_client.get_bands_by_id(scene_id)
        scene = cls(metadata, bands)

        # TODO: not sure what default res should be
        try:
            default_resolution = min(filter(None, [b.get("resolution") for b in six.itervalues(bands)]))
        except ValueError:
            default_resolution = 100

        # QUESTION: default bounds will now be in WGS84, not UTM
        # indeed, there's no way to give bounds in UTM besides with a DLTile,
        # which means you could get off-by-one issues with loading an entire scene
        # at native resolution, where the WGS84 bounds result in a slightly differently
        # sized raster than native UTM bounds would with reprojection errors
        try:
            bounds = scene.geometry.bounds
        except AttributeError:
            xs, ys = zip(*scene.geometry["coordinates"][0])
            bounds = (min(xs), min(ys), max(xs), max(ys))

        default_ctx = geocontext.AOI(bounds=bounds,
                                     resolution=default_resolution,
                                     crs=scene.properties["crs"]
                                     )

        return scene, default_ctx

    def ndarray(self,
                bands,
                ctx,
                mask_nodata=True,
                mask_alpha=True,
                bands_axis=0,
                raster_info=False,
                raster_client=None
                ):
        """
        Load bands from this scene as an ndarray, optionally masking invalid data.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
            If the alpha band is requested, it must be last in the list
            to reduce rasterization errors.
        ctx : GeoContext
            A GeoContext to use when loading this Scene
        mask_nodata : bool, default True
            Whether to mask out values in each band that equal
            that band's ``nodata`` sentinel value.
        mask_alpha : bool, default True
            Whether to mask pixels in all bands where the alpha band is 0.
        bands_axis : int, default 0
            Axis along which bands should be located in the returned array.
            If 0, the array will have shape ``(band, y, x)``, if -1,
            it will have shape ``(y, x, band)``.

            It's usually easier to work with bands as the outermost axis,
            but when working with large arrays, or with many arrays concatenated
            together, NumPy operations aggregating each xy point across bands
            can be slightly faster with bands as the innermost axis.
        raster_info : bool, default False
            Whether to also return a dict of information about the rasterization
            of the scene, including the coordinate system WKT and geotransform matrix.
            Generally only useful if you plan to upload data derived
            from this scene back to the Descartes catalog, or use it with GDAL.
        raster_client : Raster, optional
            Unneeded in general use; lets you use a specific client instance
            with non-default auth and parameters.

        Returns
        -------
        arr : ndarray
            Returned array's shape will be ``(band, y, x)`` if bands_axis is 0,
            ``(y, x, band)`` if bands_axis is -1
            If ``mask_nodata`` or ``mask_alpha`` is True, arr will be a masked array.
        raster_info : dict
            If ``raster_info=True``, a raster information dict is also returned.

        Example
        -------
        >>> import descarteslabs as dl
        >>> scene, ctx = dl.scenes.Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
        >>> arr = scene.ndarray("red green blue", ctx)
        >>> type(arr)
        numpy.ma.core.MaskedArray
        >>> arr.shape
        (3, 3815, 3836)
        >>> red_band = arr[0]

        Raises
        ------
        ValueError
            If requested bands are unavailable.
            If band names are not given or are invalid.
            If the requested bands have different dtypes.
        NotFoundError
            If a Scene's id cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs platform is given invalid parameters
        """
        if raster_client is None:
            raster_client = Raster()

        if not (-3 < bands_axis < 3):
            raise ValueError("Invalid bands_axis; axis {} would not exist in a 3D array".format(bands_axis))

        bands = self._bands_to_list(bands)
        common_data_type = self._common_data_type_of_bands(bands)

        self_bands = self.properties["bands"]
        if mask_alpha:
            if "alpha" not in self_bands:
                raise ValueError(
                    "Cannot mask alpha: no alpha band for the product '{}'".format(self.properties["product"])
                )
            try:
                alpha_i = bands.index("alpha")
            except ValueError:
                bands.append("alpha")
                drop_alpha = True
            else:
                if alpha_i != len(bands) - 1:
                    raise ValueError("Alpha must be the last band in order to reduce rasterization errors")
                drop_alpha = False

        raster_params = ctx.raster_params
        full_raster_args = dict(
            inputs=self.properties["id"],
            order="gdal",
            bands=bands,
            scales=None,
            data_type=common_data_type,
            **raster_params
        )

        try:
            arr, info = raster_client.ndarray(**full_raster_args)
        except NotFoundError as e:
            raise NotFoundError("'{}' does not exist in the Descartes catalog".format(self.properties["id"]))
        except BadRequestError as e:
            msg = ("Error with request:\n"
                   "{err}\n"
                   "For reference, dl.Raster.ndarray was called with these arguments:\n"
                   "{args}")
            msg = msg.format(err=e, args=json.dumps(full_raster_args, indent=2))
            six.raise_from(BadRequestError(msg), None)

        if len(arr.shape) == 2:
            # if only 1 band requested, still return a 3d array
            arr = arr[np.newaxis]

        if mask_nodata or mask_alpha:
            if mask_alpha:
                alpha = arr[-1]
                if drop_alpha:
                    arr = arr[:-1]
                    bands.pop(-1)

            mask = np.zeros_like(arr, dtype=bool)

            if mask_nodata:
                for i, bandname in enumerate(bands):
                    nodata = self_bands[bandname].get('nodata')
                    if nodata is not None:
                        mask[i] = arr[i] == nodata

            if mask_alpha:
                mask |= alpha == 0

            arr = np.ma.MaskedArray(arr, mask, copy=False)

        if bands_axis != 0:
            arr = np.moveaxis(arr, 0, bands_axis)
        if raster_info:
            return arr, info
        else:
            return arr

    @property
    def __geo_interface__(self):
        # QUESTION: this returns a Geometry, should it be a Feature and include properties?
        try:
            return self.geometry.__geo_interface__
        except AttributeError:
            return self.geometry

    def _dict(self):
        return dict(
            geometry=self.__geo_interface__,
            properties=self.properties,
        )

    def _repr_json_(self):
        return self._dict()

    def __repr__(self):
        return repr(self._dict())

    def _common_data_type_of_bands(self, bands):
        "Ensure all requested bands are available, and that they all have the same dtypes"
        self_bands = self.properties["bands"]
        common_data_type = None
        for b in bands:
            try:
                band = self_bands[b]
            except KeyError:
                six.raise_from(
                    ValueError("Band '{}' is not available in the product '{}'".format(b, self.properties["product"])),
                    None
                )
            try:
                data_type = band["dtype"]
            except KeyError:
                six.raise_from(
                    ValueError(
                        "Band '{}' of product '{}' has no 'dtype' field. "
                        "If you created this product, you can fix the metadata "
                        "at https://catalog.descarteslabs.com.".format(b, self.properties["product"])
                    ),
                    None
                )
            if common_data_type is None:
                common_data_type = data_type
            else:
                if data_type != common_data_type:
                    raise ValueError(
                        "Bands must all have the same dtype. The band '{}' has dtype '{}', "
                        "but all bands before it had the dtype '{}'.".format(b, data_type, common_data_type)
                    )
        return common_data_type

    @staticmethod
    def _bands_to_list(bands):
        if isinstance(bands, six.string_types):
            return bands.split(" ")
        if not isinstance(bands, (list, tuple)):
            raise TypeError("Expected list or tuple of band names, instead got {}".format(type(bands)))
        if len(bands) == 0:
            raise ValueError("No bands specified to load")
        return list(bands)
