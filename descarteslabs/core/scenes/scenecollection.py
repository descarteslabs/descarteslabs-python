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

import collections

from ..common.collection import Collection
from ..catalog import ImageCollection
from .scene import Scene


class SceneCollection(Collection):
    """
    Holds Scenes, with methods for loading their data.

    As a subclass of the `Collection` class, the `filter`, `map`, and `groupby`
    methods and `each` property simplify inspection and subselection of
    contained Scenes.

    `stack` and `mosaic` rasterize all contained Scenes into an ndarray
    using the a :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
    """

    _item_type = Scene

    def __init__(self, iterable=None):
        # unlike an ImageCollection, SceneCollection has no default geocontext
        if isinstance(iterable, ImageCollection):
            super(SceneCollection, self).__init__(Scene(image) for image in iterable)
        else:
            super(SceneCollection, self).__init__(iterable)

    def filter_coverage(self, geom, minimum_coverage=1):
        """
        Include only Scenes overlapping with ``geom`` by some fraction.

        See `Image.coverage <descarteslabs.catalog.image.Image.coverage>`
        for getting coverage information for an image.

        Parameters
        ----------
        geom : GeoJSON-like dict, :class:`~descarteslabs.common.geo.geocontext.GeoContext`, or object with __geo_interface__  # noqa: E501
            Geometry to which to compare each image's geometry.
        minimum_coverage : float
            Only include scenes that cover ``geom`` by at least this fraction.

        Returns
        -------
        scenes : SceneCollection

        Example
        -------
        >>> import descarteslabs as dl
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-95, 42],[-93, 42],[-93, 40],[-95, 41],[-95, 42]]]}
        >>> scenes, ctx = dl.scenes.search(aoi_geometry, products="landsat:LC08:PRE:TOAR", limit=20)  # doctest: +SKIP
        >>> filtered_scenes = scenes.filter_coverage(ctx, 0.01)  # doctest: +SKIP
        >>> assert len(filtered_scenes) < len(scenes)  # doctest: +SKIP
        """
        return self.filter(lambda s: s._image.coverage(geom) >= minimum_coverage)

    def stack(
        self,
        bands,
        ctx,
        flatten=None,
        mask_nodata=True,
        mask_alpha=None,
        bands_axis=1,
        raster_info=False,
        resampler="near",
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
        max_workers=None,
    ):
        """
        Load bands from all scenes and stack them into a 4D ndarray,
        optionally masking invalid data.

        If the selected bands and scenes have different data types the resulting
        ndarray has the most general of those data types. See
        `Scene.ndarray() <descarteslabs.scenes.scene.Scene.ndarray>` for details
        on data type conversions.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
            If the alpha band is requested, it must be last in the list
            to reduce rasterization errors.
        ctx : :class:`~descarteslabs.common.geo.geocontext.GeoContext`
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading each Scene
        flatten : str, Sequence[str], callable, or Sequence[callable], default None
            "Flatten" groups of Scenes in the stack into a single layer by mosaicking
            each group (such as Scenes from the same day), then stacking the mosaics.

            ``flatten`` takes the same predicates as `Collection.groupby`, such as
            ``"properties.date"`` to mosaic Scenes acquired at the exact same timestamp,
            or ``["properties.date.year", "properties.date.month", "properties.date.day"]``
            to combine Scenes captured on the same day (but not necessarily the same time).

            This is especially useful when ``ctx`` straddles a scene boundary
            and contains one image captured right after another. Instead of having
            each as a separate layer in the stack, you might want them combined.

            Note that indicies in the returned ndarray will no longer correspond to
            indicies in this SceneCollection, since multiple Scenes may be combined into
            one layer in the stack. You can call ``groupby`` on this SceneCollection
            with the same parameters to iterate through groups of Scenes in equivalent
            order to the returned ndarray.

            Additionally, the order of scenes in the ndarray will change:
            they'll be sorted by the parameters to ``flatten``.
        mask_nodata : bool, default True
            Whether to mask out values in each band of each scene that equal
            that band's ``nodata`` sentinel value.
        mask_alpha : bool or str or None, default None
            Whether to mask pixels in all bands where the alpha band of all scenes is 0.
            Provide a string to use an alternate band name for masking.
            If the alpha band is available for all scenes in the collection and
            ``mask_alpha`` is None, ``mask_alpha`` is set to True. If not,
            mask_alpha is set to False.
        bands_axis : int, default 1
            Axis along which bands should be located.
            If 1, the array will have shape ``(scene, band, y, x)``, if -1,
            it will have shape ``(scene, y, x, band)``, etc.
            A bands_axis of 0 is currently unsupported.
        raster_info : bool, default False
            Whether to also return a list of dicts about the rasterization of
            each scene, including the coordinate system WKT
            and geotransform matrix.
            Generally only useful if you plan to upload data derived from this
            scene back to the Descartes catalog, or use it with GDAL.
        resampler : str, default "near"
            Algorithm used to interpolate pixel values when scaling and transforming
            each image to its new resolution or SRS. Possible values are
            ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicsplice``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : str, optional
            How the processing level of the underlying data should be adjusted. Possible
            values depend on the product and bands in use. Legacy products support
            ``toa`` (top of atmosphere) and in some cases ``surface``. Consult the
            available ``processing_levels`` in the product bands to understand what
            is available.
        scaling : None, str, list, dict
            Band scaling specification. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        data_type : None, str
            Output data type. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        progress : None, bool
            Controls display of a progress bar.
        max_workers : int, default None
            Maximum number of threads to use to parallelize individual ndarray
            calls to each Scene.
            If None, it defaults to the number of processors on the machine,
            multiplied by 5.
            Note that unnecessary threads *won't* be created if ``max_workers``
            is greater than the number of Scenes in the SceneCollection.

        Returns
        -------
        arr : ndarray
            Returned array's shape is ``(scene, band, y, x)`` if bands_axis is 1,
            or ``(scene, y, x, band)`` if bands_axis is -1.
            If ``mask_nodata`` or ``mask_alpha`` is True, arr will be a masked array.
            The data type ("dtype") of the array is the most general of the data
            types among the scenes being rastered.
        raster_info : List[dict]
            If ``raster_info=True``, a list of raster information dicts for each scene
            is also returned

        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the
            :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
            If the SceneCollection is empty.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given unrecognized parameters
        """
        # ImageCollection can handle everything, except we have to generate a flattening map
        # now as it depends upon Scene properties, not Image properties
        if flatten is not None:
            if isinstance(flatten, str) or not hasattr(flatten, "__len__"):
                flatten = [flatten]
            image_map = {
                s._image.id: group for group, sc in self.groupby(*flatten) for s in sc
            }

            def map_group(image):
                return image_map.get(image.id)

            flatten = [map_group]

        return ImageCollection(s._image for s in self).stack(
            bands=bands,
            geocontext=ctx,
            flatten=flatten,
            mask_nodata=mask_nodata,
            mask_alpha=mask_alpha,
            bands_axis=bands_axis,
            raster_info=raster_info,
            resampler=resampler,
            processing_level=processing_level,
            scaling=scaling,
            data_type=data_type,
            progress=progress,
            max_workers=max_workers,
        )

    def mosaic(
        self,
        bands,
        ctx,
        mask_nodata=True,
        mask_alpha=None,
        bands_axis=0,
        resampler="near",
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
        raster_info=False,
    ):
        """
        Load bands from all scenes, combining them into a single 3D ndarray
        and optionally masking invalid data.

        Where multiple scenes overlap, only data from the scene that comes last
        in the SceneCollection is used.

        If the selected bands and scenes have different data types the resulting
        ndarray has the most general of those data types. See
        `Scene.ndarray() <descarteslabs.scenes.scene.Scene.ndarray>` for details
        on data type conversions.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
            If the alpha band is requested, it must be last in the list
            to reduce rasterization errors.
        ctx : :class:`~descarteslabs.common.geo.geocontext.GeoContext`
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading each Scene
        mask_nodata : bool, default True
            Whether to mask out values in each band that equal
            that band's ``nodata`` sentinel value.
        mask_alpha : bool or str or None, default None
            Whether to mask pixels in all bands where the alpha band of all scenes is 0.
            Provide a string to use an alternate band name for masking.
            If the alpha band is available for all scenes in the collection and
            ``mask_alpha`` is None, ``mask_alpha`` is set to True. If not,
            mask_alpha is set to False.
        bands_axis : int, default 0
            Axis along which bands should be located in the returned array.
            If 0, the array will have shape ``(band, y, x)``,
            if -1, it will have shape ``(y, x, band)``.

            It's usually easier to work with bands as the outermost axis,
            but when working with large arrays, or with many arrays concatenated
            together, NumPy operations aggregating each xy point across bands
            can be slightly faster with bands as the innermost axis.
        raster_info : bool, default False
            Whether to also return a dict of information about the rasterization
            of the scenes, including the coordinate system WKT and geotransform matrix.
            Generally only useful if you plan to upload data derived
            from this scene back to the Descartes catalog, or use it with GDAL.
        resampler : str, default "near"
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or SRS. Possible values are
            ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicsplice``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : str, optional
            How the processing level of the underlying data should be adjusted. Possible
            values depend on the product and bands in use. Legacy products support
            ``toa`` (top of atmosphere) and in some cases ``surface``. Consult the
            available ``processing_levels`` in the product bands to understand what
            is available.
        scaling : None, str, list, dict
            Band scaling specification. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        data_type : None, str
            Output data type. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        progress : None, bool
            Controls display of a progress bar.


        Returns
        -------
        arr : ndarray
            Returned array's shape will be ``(band, y, x)`` if ``bands_axis``
            is 0, and ``(y, x, band)`` if ``bands_axis`` is -1.
            If ``mask_nodata`` or ``mask_alpha`` is True, arr will be a masked array.
            The data type ("dtype") of the array is the most general of the data
            types among the scenes being rastered.
        raster_info : dict
            If ``raster_info=True``, a raster information dict is also returned.

        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the
            :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
            If the SceneCollection is empty.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given unrecognized parameters
        """
        return ImageCollection(s._image for s in self).mosaic(
            bands=bands,
            geocontext=ctx,
            mask_nodata=mask_nodata,
            mask_alpha=mask_alpha,
            bands_axis=bands_axis,
            resampler=resampler,
            processing_level=processing_level,
            scaling=scaling,
            data_type=data_type,
            progress=progress,
            raster_info=raster_info,
        )

    def download(
        self,
        bands,
        ctx,
        dest,
        format="tif",
        resampler="near",
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
        max_workers=None,
    ):
        """
        Download scenes as image files in parallel.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
        ctx : :class:`~descarteslabs.common.geo.geocontext.GeoContext`
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading each Scene
        dest : str, path-like, or sequence of str or path-like
            Directory or sequence of paths to which to write the image files.

            If a directory, files within it will be named by
            their scene IDs and the bands requested, like
            ``"sentinel-2:L1C:2018-08-10_10TGK_68_S2A_v1-red-green-blue.tif"``.

            If a sequence of paths of the same length as the SceneCollection is given,
            each Scene will be written to the corresponding path. This lets you use your
            own naming scheme, or even write images to multiple directories.

            Any intermediate paths are created if they do not exist,
            for both a single directory and a sequence of paths.
        format : str, default "tif"
            Only if a single directory is given as ``dest``:
            what image format to use. One of "tif", "png", or "jpg".

            If ``dest`` is a sequence of paths, ``format`` is ignored
            and determined by the extension on each path.
        resampler : str, default "near"
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or SRS. Possible values are
            ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicsplice``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : str, optional
            How the processing level of the underlying data should be adjusted. Possible
            values depend on the product and bands in use. Legacy products support
            ``toa`` (top of atmosphere) and in some cases ``surface``. Consult the
            available ``processing_levels`` in the product bands to understand what
            is available.
        scaling : None, str, list, dict
            Band scaling specification. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        data_type : None, str
            Output data type. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        progress : None, bool
            Controls display of a progress bar.
        max_workers : int, default None
            Maximum number of threads to use to parallelize individual ``download``
            calls to each Scene.
            If None, it defaults to the number of processors on the machine,
            multiplied by 5.
            Note that unnecessary threads *won't* be created if ``max_workers``
            is greater than the number of Scenes in the SceneCollection.

        Returns
        -------
        paths : Sequence[str]
            A list of all the paths where files were written.

        Example
        -------
        >>> import descarteslabs as dl
        >>> tile = dl.scenes.DLTile.from_key("256:0:75.0:15:-5:230")  # doctest: +SKIP
        >>> scenes, _ = dl.scenes.search(tile, products=["landsat:LC08:PRE:TOAR"], limit=5)  # doctest: +SKIP
        >>> scenes.download("red green blue", tile, "rasters")  # doctest: +SKIP
        ["rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013108_v1-red-green-blue.tif",
         "rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013124_v1-red-green-blue.tif",
         "rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013140_v1-red-green-blue.tif",
         "rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013156_v1-red-green-blue.tif",
         "rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013172_v1-red-green-blue.tif"]
        >>> # use explicit paths for a custom naming scheme:
        >>> paths = [
        ...     "{tile.key}/l8-{scene.properties.date:%Y-%m-%d-%H:%m}.jpg".format(tile=tile, scene=scene)
        ...     for scene in scenes
        ... ]  # doctest: +SKIP
        >>> scenes.download("nir red", tile, paths)  # doctest: +SKIP
        ["256:0:75.0:15:-5:230/l8-2013-04-18-16:04.jpg",
         "256:0:75.0:15:-5:230/l8-2013-05-04-16:05.jpg",
         "256:0:75.0:15:-5:230/l8-2013-05-20-16:05.jpg",
         "256:0:75.0:15:-5:230/l8-2013-06-05-16:06.jpg",
         "256:0:75.0:15:-5:230/l8-2013-06-21-16:06.jpg"]

        Raises
        ------
        RuntimeError
            If the paths given are not all unique.
            If there is an error generating default filenames.
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the
            :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
            If the SceneCollection is empty.
            If ``dest`` is a sequence not equal in length to the SceneCollection.
            If ``format`` is invalid, or a path has an invalid extension.
        TypeError
            If ``dest`` is not a string or a sequence type.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given unrecognized parameters
        """
        return ImageCollection(s._image for s in self).download(
            bands=bands,
            geocontext=ctx,
            dest=dest,
            format=format,
            resampler=resampler,
            processing_level=processing_level,
            scaling=scaling,
            data_type=data_type,
            progress=progress,
            max_workers=max_workers,
        )

    def download_mosaic(
        self,
        bands,
        ctx,
        dest=None,
        format="tif",
        resampler="near",
        processing_level=None,
        scaling=None,
        data_type=None,
        mask_alpha=None,
        nodata=None,
        progress=None,
    ):
        """
        Download all scenes as a single image file.
        Where multiple scenes overlap, only data from the scene that comes last
        in the SceneCollection is used.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
        ctx : :class:`~descarteslabs.common.geo.geocontext.GeoContext`
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading the Scenes
        dest : str or path-like object, default None
            Where to write the image file.

            * If None (default), it's written to an image file of the given ``format``
              in the current directory, named by the requested bands,
              like ``"mosaic-red-green-blue.tif"``
            * If a string or path-like object, it's written to that path.

              Any file already existing at that path will be overwritten.

              Any intermediate directories will be created if they don't exist.

              Note that path-like objects (such as pathlib.Path) are only supported
              in Python 3.6 or later.
        format : str, default "tif"
            If None is given as ``dest``: one of "tif", "png", or "jpg".

            If a str or path-like object is given as ``dest``, ``format`` is ignored
            and determined from the extension on the path (one of ".tif", ".png", or ".jpg").
        resampler : str, default "near"
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or SRS. Possible values are
            ``near`` (nearest-neighbor), ``bilinear``, ``cubic``, ``cubicsplice``,
            ``lanczos``, ``average``, ``mode``, ``max``, ``min``, ``med``, ``q1``, ``q3``.
        processing_level : str, optional
            How the processing level of the underlying data should be adjusted. Possible
            values depend on the product and bands in use. Legacy products support
            ``toa`` (top of atmosphere) and in some cases ``surface``. Consult the
            available ``processing_levels`` in the product bands to understand what
            is available.
        scaling : None, str, list, dict
            Band scaling specification. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        data_type : None, str
            Output data type. Please see :meth:`scaling_parameters` for a full
            description of this parameter.
        mask_alpha : bool or str or None, default None
            Whether to mask pixels in all bands where the alpha band of all scenes is 0.
            Provide a string to use an alternate band name for masking.
            If the alpha band is available for all scenes in the collection and
            ``mask_alpha`` is None, ``mask_alpha`` is set to True. If not,
            mask_alpha is set to False.
        nodata : None, number
            NODATA value for a geotiff file. Will be assigned to any masked pixels.
        progress : None, bool
            Controls display of a progress bar.

        Returns
        -------
        path : str or None
            If ``dest`` is a path or None, the path where the image file was written is returned.
            If ``dest`` is file-like, nothing is returned.

        Example
        -------
        >>> import descarteslabs as dl
        >>> tile = dl.scenes.DLTile.from_key("256:0:75.0:15:-5:230")  # doctest: +SKIP
        >>> scenes, _ = dl.scenes.search(tile, products=["landsat:LC08:PRE:TOAR"], limit=5)  # doctest: +SKIP
        >>> scenes.download_mosaic("nir red", tile)  # doctest: +SKIP
        'mosaic-nir-red-alpha.jpg'
        >>> scenes.download_mosaic("nir red", tile, dest="mosaics/{}.png".format(tile.key))  # doctest: +SKIP
        'mosaics/256:0:75.0:15:-5:230.png'


        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the
            :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
            If the SceneCollection is empty.
            If ``format`` is invalid, or the path has an invalid extension.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given unrecognized parameters
        """
        return ImageCollection(s._image for s in self).download_mosaic(
            bands=bands,
            geocontext=ctx,
            dest=dest,
            format=format,
            resampler=resampler,
            processing_level=processing_level,
            scaling=scaling,
            data_type=data_type,
            mask_alpha=mask_alpha,
            nodata=nodata,
            progress=progress,
        )


def __repr__(self):
    parts = [
        "SceneCollection of {} scene{}".format(len(self), "" if len(self) == 1 else "s")
    ]
    try:
        first = min(self.each.properties.date)
        last = max(self.each.properties.date)
        dates = "  * Dates: {:%b %d, %Y} to {:%b %d, %Y}".format(first, last)
        parts.append(dates)
    except Exception:
        pass

    try:
        products = self.each.properties.product.combine(collections.Counter)
        if len(products) > 0:
            products = ", ".join("{}: {}".format(k, v) for k, v in products.items())
            products = "  * Products: {}".format(products)
            parts.append(products)
    except Exception:
        pass

    return "\n".join(parts)
