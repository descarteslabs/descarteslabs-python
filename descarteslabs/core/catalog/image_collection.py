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
import concurrent.futures
import json
import os

import numpy as np

from descarteslabs.exceptions import NotFoundError, BadRequestError

from ..common.collection import Collection
from ..common.geo import GeoContext, AOI
from ..client.services.raster import Raster

from .attributes import ResolutionUnit
from .band import DerivedBand
from .image_types import ResampleAlgorithm, DownloadFileFormat
from .helpers import bands_to_list, cached_bands_by_product, download, is_path_like
from .scaling import multiproduct_scaling_parameters, append_alpha_scaling


class ImageCollection(Collection):
    """
    Holds Images, with methods for loading their data.

    As a subclass of `Collection`, the `filter`, `map`, and `groupby`
    methods and `each` property simplify inspection and subselection of
    contained Images.

    `stack` and `mosaic` rasterize all contained images into an ndarray
    using the a :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
    """

    # _item_type set below due to circular imports

    def __init__(self, iterable=None, geocontext=None):
        super(ImageCollection, self).__init__(iterable)

        # try to form a default context
        if geocontext is not None:
            if not isinstance(geocontext, GeoContext):
                geocontext = AOI(geometry=geocontext)

        if len(self) > 0 and isinstance(geocontext, AOI):
            # possibly update from contained images
            if geocontext.crs is None:
                crs = collections.Counter(
                    i.cs_code or i.projection for i in self._list
                ).most_common(1)[0][0]
                geocontext = geocontext.assign(crs=crs)

            if geocontext.resolution is None and geocontext.shape is None:
                product_bands = self._product_bands()

                # The resolution must be in the same units as the CRS. However,
                # we don't have any means here to determine the units of the CRS.
                # Instead the best we can do is trust the band resolution definitions.
                resolution = None

                # gather up all the resolutions for all the bands
                resolutions = [
                    band.resolution
                    for product_id in product_bands
                    for band in product_bands[product_id].values()
                    if not isinstance(band, DerivedBand)
                    and band.resolution is not None
                    and band.resolution.value
                ]

                if resolutions:
                    # determine whether degrees or meters is more common
                    unit_counter = collections.Counter(
                        resolution.unit
                        for resolution in resolutions
                        if resolution.unit is not None
                    )
                    if len(unit_counter) > 0:
                        unit = unit_counter.most_common(1)[0][0]
                    else:
                        unit = ResolutionUnit.METERS

                    # define factors to convert to most common unit
                    if unit == ResolutionUnit.DEGREES:
                        factors = {
                            ResolutionUnit.METERS: 1 / 111111,
                            ResolutionUnit.DEGREES: 1,
                        }
                    else:
                        factors = {
                            ResolutionUnit.METERS: 1,
                            ResolutionUnit.DEGREES: 111111,
                        }

                    # find the minimum of all values
                    values = (
                        resolution.value
                        * factors[resolution.unit or ResolutionUnit.METERS]
                        for resolution in resolutions
                    )
                    resolution = min(values)

                geocontext = geocontext.assign(resolution=resolution)

        self._geocontext = geocontext

    @property
    def _client(self):
        # pick a client, any client. Sure hope they're all the same
        return self._list[0]._client

    @property
    def geocontext(self):
        return self._geocontext

    def filter_coverage(self, geom, minimum_coverage=1):
        """
        Include only images overlapping with ``geom`` by some fraction.

        See `Image.coverage <descarteslabs.catalog.image.Image.coverage>`
        for getting coverage information for an image.

        Parameters
        ----------
        geom : GeoJSON-like dict, :class:`~descarteslabs.common.geo.geocontext.GeoContext`, or object with __geo_interface__  # noqa: E501
            Geometry to which to compare each image's geometry.
        minimum_coverage : float
            Only include images that cover ``geom`` by at least this fraction.

        Returns
        -------
        images : ImageCollection

        Example
        -------
        >>> import descarteslabs as dl
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-95, 42],[-93, 42],[-93, 40],[-95, 41],[-95, 42]]]}
        >>> product = dl.catalog.Product.get("landsat:LC08:PRE:TOAR")  # doctest: +SKIP
        >>> images = product.images().intersects(aoi_geometry).limit(20).collect()  # doctest: +SKIP
        >>> filtered_images = images.filter_coverage(images.geocontext, 0.01)  # doctest: +SKIP
        >>> assert len(filtered_images) < len(images)  # doctest: +SKIP
        """

        return self.filter(lambda i: i.coverage(geom) >= minimum_coverage)

    def stack(
        self,
        bands,
        geocontext=None,
        crs=None,
        resolution=None,
        all_touched=None,
        flatten=None,
        mask_nodata=True,
        mask_alpha=None,
        bands_axis=1,
        raster_info=False,
        resampler=ResampleAlgorithm.NEAR,
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
        max_workers=None,
    ):
        """
        Load bands from all images and stack them into a 4D ndarray,
        optionally masking invalid data.

        If the selected bands and images have different data types the resulting
        ndarray has the most general of those data types. See
        `Image.ndarray() <descarteslabs.catalog.image.Image.ndarray>` for details
        on data type conversions.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
            If the alpha band is requested, it must be last in the list
            to reduce rasterization errors.
        geocontext : :class:`~descarteslabs.common.geo.geocontext.GeoContext`, default None
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading each image.
            If ``None`` then the default context of the collection will be used. If
            this is ``None``, a ValueError is raised.
        crs : str, default None
            if not None, update the gecontext with this value to set the output CRS.
        resolution : float, default None
            if not None, update the geocontext with this value to set the output resolution
            in the units native to the specified or defaulted output CRS.
        all_touched : float, default None
            if not None, update the geocontext with this value to control rastering behavior.
        flatten : str, Sequence[str], callable, or Sequence[callable], default None
            "Flatten" groups of images in the stack into a single layer by mosaicking
            each group (such as images from the same day), then stacking the mosaics.

            ``flatten`` takes the same predicates as `Collection.groupby`, such as
            ``"properties.date"`` to mosaic images acquired at the exact same timestamp,
            or ``["properties.date.year", "properties.date.month", "properties.date.day"]``
            to combine images captured on the same day (but not necessarily the same time).

            This is especially useful when ``geocontext`` straddles an image boundary
            and contains one image captured right after another. Instead of having
            each as a separate layer in the stack, you might want them combined.

            Note that indicies in the returned ndarray will no longer correspond to
            indicies in this ImageCollection, since multiple images may be combined into
            one layer in the stack. You can call ``groupby`` on this ImageCollection
            with the same parameters to iterate through groups of images in equivalent
            order to the returned ndarray.

            Additionally, the order of images in the ndarray will change:
            they'll be sorted by the parameters to ``flatten``.
        mask_nodata : bool, default True
            Whether to mask out values in each band of each image that equal
            that band's ``nodata`` sentinel value.
        mask_alpha : bool or str or None, default None
            Whether to mask pixels in all bands where the alpha band of all images is 0.
            Provide a string to use an alternate band name for masking.
            If the alpha band is available for all images in the collection and
            ``mask_alpha`` is None, ``mask_alpha`` is set to True. If not,
            mask_alpha is set to False.
        bands_axis : int, default 1
            Axis along which bands should be located.
            If 1, the array will have shape ``(image, band, y, x)``, if -1,
            it will have shape ``(image, y, x, band)``, etc.
            A bands_axis of 0 is currently unsupported.
        raster_info : bool, default False
            Whether to also return a list of dicts about the rasterization of
            each image, including the coordinate system WKT and geotransform matrix.
            Generally only useful if you plan to upload data derived from this
            image back to the Descartes catalog, or use it with GDAL.
        resampler : `ResampleAlgorithm`, default `ResampleAlgorithm.NEAR`
            Algorithm used to interpolate pixel values when scaling and transforming
            each image to its new resolution or SRS.
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
            calls to each image.
            If None, it defaults to the number of processors on the machine,
            multiplied by 5.
            Note that unnecessary threads *won't* be created if ``max_workers``
            is greater than the number of images in the ImageCollection.

        Returns
        -------
        arr : ndarray
            Returned array's shape is ``(image, band, y, x)`` if bands_axis is 1,
            or ``(image, y, x, band)`` if bands_axis is -1.
            If ``mask_nodata`` or ``mask_alpha`` is True, arr will be a masked array.
            The data type ("dtype") of the array is the most general of the data
            types among the images being rastered.
        raster_info : List[dict]
            If ``raster_info=True``, a list of raster information dicts for each image
            is also returned

        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If the context is None and no default context for the ImageCollection
            is defined, or if not all required parameters are specified in the
            :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
            If the ImageCollection is empty.
        NotFoundError
            If a Image's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given unrecognized parameters
        """
        if len(self) == 0:
            raise ValueError("This ImageCollection is empty")

        if geocontext is None:
            geocontext = self.geocontext
            if geocontext is None:
                raise ValueError(
                    "No geocontext supplied, and no default geocontext is defined for this ImageCollection"
                )
        if crs is not None or resolution is not None:
            try:
                params = {}
                if crs is not None:
                    params["crs"] = crs
                if resolution is not None:
                    params["resolution"] = resolution
                geocontext = geocontext.assign(**params)
            except TypeError:
                raise ValueError(
                    f"{type(geocontext)} geocontext does not support modifying crs or resolution"
                ) from None
        if all_touched is not None:
            geocontext = geocontext.assign(all_touched=all_touched)

        kwargs = dict(
            mask_nodata=mask_nodata,
            mask_alpha=mask_alpha,
            bands_axis=bands_axis,
            raster_info=raster_info,
            resampler=resampler,
            processing_level=processing_level,
            progress=progress,
        )

        if bands_axis == 0 or bands_axis == -4:
            raise NotImplementedError(
                "bands_axis of 0 is currently unsupported for `ImageCollection.stack`. "
                "If you require this shape, try ``np.moveaxis(my_stack, 1, 0)`` on the returned ndarray."
            )
        elif bands_axis > 0:
            kwargs["bands_axis"] = (
                bands_axis - 1
            )  # the bands axis for each component ndarray call in the stack

        if flatten is not None:
            if isinstance(flatten, str) or not hasattr(flatten, "__len__"):
                flatten = [flatten]
            images = [
                ic if len(ic) > 1 else ic[0] for group, ic in self.groupby(*flatten)
            ]
        else:
            images = self

        full_stack = None
        mask = None
        if raster_info:
            raster_infos = [None] * len(images)

        bands = bands_to_list(bands)
        product_bands = self._product_bands()
        (bands, scaling, mask_alpha, pop_alpha) = self._mask_alpha_if_applicable(
            product_bands, bands, mask_alpha=mask_alpha, scaling=scaling
        )
        scales, data_type = multiproduct_scaling_parameters(
            product_bands, bands, processing_level, scaling, data_type
        )

        if pop_alpha:
            bands.pop(-1)
            if scales:
                scales.pop(-1)

        kwargs["scaling"] = scales
        kwargs["data_type"] = data_type

        def threaded_ndarrays():
            def data_loader(image_or_imagecollection, bands, geocontext, **kwargs):
                if isinstance(image_or_imagecollection, self.__class__):
                    return lambda: image_or_imagecollection.mosaic(
                        bands, geocontext, **kwargs
                    )
                else:
                    return lambda: image_or_imagecollection._ndarray(
                        bands, geocontext, **kwargs
                    )

            with concurrent.futures.ThreadPoolExecutor(
                max_workers=max_workers
            ) as executor:
                future_ndarrays = {}
                for i, image_or_imagecollection in enumerate(images):
                    future_ndarray = executor.submit(
                        data_loader(
                            image_or_imagecollection, bands, geocontext, **kwargs
                        )
                    )
                    future_ndarrays[future_ndarray] = i
                for future in concurrent.futures.as_completed(future_ndarrays):
                    i = future_ndarrays[future]
                    result = future.result()
                    yield i, result

        for i, arr in threaded_ndarrays():
            if raster_info:
                arr, raster_meta = arr
                raster_infos[i] = raster_meta

            if full_stack is None:
                stack_shape = (len(images),) + arr.shape
                full_stack = np.empty(stack_shape, dtype=arr.dtype)
                if isinstance(arr, np.ma.MaskedArray):
                    mask = np.empty(stack_shape, dtype=bool)

            if isinstance(arr, np.ma.MaskedArray):
                full_stack[i] = arr.data
                mask[i] = arr.mask
            else:
                full_stack[i] = arr

        if mask is not None:
            full_stack = np.ma.MaskedArray(full_stack, mask, copy=False)
        if raster_info:
            return full_stack, raster_infos
        else:
            return full_stack

    def mosaic(
        self,
        bands,
        geocontext=None,
        crs=None,
        resolution=None,
        all_touched=None,
        mask_nodata=True,
        mask_alpha=None,
        bands_axis=0,
        resampler=ResampleAlgorithm.NEAR,
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
        raster_info=False,
    ):
        """
        Load bands from all images, combining them into a single 3D ndarray
        and optionally masking invalid data.

        Where multiple images overlap, only data from the image that comes last
        in the ImageCollection is used.

        If the selected bands and images have different data types the resulting
        ndarray has the most general of those data types. See
        `Image.ndarray() <descarteslabs.catalog.image.Image.ndarray>` for details
        on data type conversions.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
            If the alpha band is requested, it must be last in the list
            to reduce rasterization errors.
        geocontext : :class:`~descarteslabs.common.geo.geocontext.GeoContext`, default None
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading each image.
            If ``None`` then the default context of the collection will be used. If
            this is ``None``, a ValueError is raised.
        crs : str, default None
            if not None, update the gecontext with this value to set the output CRS.
        resolution : float, default None
            if not None, update the geocontext with this value to set the output resolution
            in the units native to the specified or defaulted output CRS.
        all_touched : float, default None
            if not None, update the geocontext with this value to control rastering behavior.
        mask_nodata : bool, default True
            Whether to mask out values in each band that equal
            that band's ``nodata`` sentinel value.
        mask_alpha : bool or str or None, default None
            Whether to mask pixels in all bands where the alpha band of all images is 0.
            Provide a string to use an alternate band name for masking.
            If the alpha band is available for all images in the collection and
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
            of the images, including the coordinate system WKT and geotransform matrix.
            Generally only useful if you plan to upload data derived
            from this image back to the Descartes catalog, or use it with GDAL.
        resampler : `ResampleAlgorithm`, default `ResampleAlgorithm.NEAR`
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or SRS.
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
            types among the images being rastered.
        raster_info : dict
            If ``raster_info=True``, a raster information dict is also returned.

        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the
            :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
            If the ImageCollection is empty.
        NotFoundError
            If a Image's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given unrecognized parameters
        """
        if len(self) == 0:
            raise ValueError("This ImageCollection is empty")

        if geocontext is None:
            geocontext = self.geocontext
            if geocontext is None:
                raise ValueError(
                    "No geocontext supplied, and no default geocontext is defined for this ImageCollection"
                )
        if crs is not None or resolution is not None:
            try:
                params = {}
                if crs is not None:
                    params["crs"] = crs
                if resolution is not None:
                    params["resolution"] = resolution
                geocontext = geocontext.assign(**params)
            except TypeError:
                raise ValueError(
                    f"{type(geocontext)} geocontext does not support modifying crs or resolution"
                ) from None
        if all_touched is not None:
            geocontext = geocontext.assign(all_touched=all_touched)

        if not (-3 < bands_axis < 3):
            raise ValueError(
                "Invalid bands_axis; axis {} would not exist in a 3D array".format(
                    bands_axis
                )
            )

        bands = bands_to_list(bands)
        product_bands = self._product_bands()
        (bands, scaling, mask_alpha, drop_alpha) = self._mask_alpha_if_applicable(
            product_bands, bands, mask_alpha=mask_alpha, scaling=scaling
        )
        scales, data_type = multiproduct_scaling_parameters(
            product_bands, bands, processing_level, scaling, data_type
        )

        raster_params = geocontext.raster_params
        full_raster_args = dict(
            inputs=[image.id for image in self],
            order="gdal",
            bands=bands,
            scales=scales,
            data_type=data_type,
            resampler=resampler,
            processing_level=processing_level,
            mask_nodata=bool(mask_nodata),
            mask_alpha=mask_alpha,
            drop_alpha=drop_alpha,
            masked=mask_nodata or mask_alpha,
            progress=progress,
            **raster_params,
        )
        try:
            arr, info = Raster.get_default_client().ndarray(**full_raster_args)
        except NotFoundError:
            raise NotFoundError(
                "Some or all of these IDs don't exist in the Descartes catalog: {}".format(
                    full_raster_args["inputs"]
                )
            )
        except BadRequestError as e:
            msg = (
                "Error with request:\n"
                "{err}\n"
                "For reference, Raster.ndarray was called with these arguments:\n"
                "{args}"
            )
            msg = msg.format(err=e, args=json.dumps(full_raster_args, indent=2))
            raise BadRequestError(msg) from None

        if len(arr.shape) == 2:
            # if only 1 band requested, still return a 3d array
            arr = arr[np.newaxis]

        if bands_axis != 0:
            arr = np.moveaxis(arr, 0, bands_axis)
        if raster_info:
            return arr, info
        else:
            return arr

    def download(
        self,
        bands,
        geocontext=None,
        crs=None,
        resolution=None,
        all_touched=None,
        dest=None,
        format=DownloadFileFormat.TIF,
        resampler=ResampleAlgorithm.NEAR,
        processing_level=None,
        scaling=None,
        data_type=None,
        progress=None,
        max_workers=None,
    ):
        """
        Download images as image files in parallel.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
        geocontext : :class:`~descarteslabs.common.geo.geocontext.GeoContext`, default None
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading each image.
            If ``None`` then the default context of the collection will be used. If
            this is ``None``, a ValueError is raised.
        crs : str, default None
            if not None, update the gecontext with this value to set the output CRS.
        resolution : float, default None
            if not None, update the geocontext with this value to set the output resolution
            in the units native to the specified or defaulted output CRS.
        all_touched : float, default None
            if not None, update the geocontext with this value to control rastering behavior.
        dest : str, path-like, or sequence of str or path-like, default None
            Directory or sequence of paths to which to write the image files.

            If ``None``, the current directory is used.

            If a directory, files within it will be named by
            their image IDs and the bands requested, like
            ``"sentinel-2:L1C:2018-08-10_10TGK_68_S2A_v1-red-green-blue.tif"``.

            If a sequence of paths of the same length as the ImageCollection is given,
            each Image will be written to the corresponding path. This lets you use your
            own naming scheme, or even write images to multiple directories.

            Any intermediate paths are created if they do not exist,
            for both a single directory and a sequence of paths.
        format : `DownloadFileFormat`, default `DownloadFileFormat.TIF`
            Output file format to use.
            If ``dest`` is a sequence of paths, ``format`` is ignored
            and determined by the extension on each path.
        resampler : `ResampleAlgorithm`, default `ResampleAlgorithm.NEAR`
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or SRS.
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
            calls to each Image.
            If None, it defaults to the number of processors on the machine,
            multiplied by 5.
            Note that unnecessary threads *won't* be created if ``max_workers``
            is greater than the number of Images in the ImageCollection.

        Returns
        -------
        paths : Sequence[str]
            A list of all the paths where files were written.

        Example
        -------
        >>> import descarteslabs as dl
        >>> tile = dl.common.geo.DLTile.from_key("256:0:75.0:15:-5:230")  # doctest: +SKIP
        >>> product = dl.catalog.Product.get("landsat:LC08:PRE:TOAR")  # doctest: +SKIP
        >>> images = product.images().intersects(tile).limit(5).collect()  # doctest: +SKIP
        >>> images.download("red green blue", tile, "rasters")  # doctest: +SKIP
        ["rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013108_v1-red-green-blue.tif",
         "rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013124_v1-red-green-blue.tif",
         "rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013140_v1-red-green-blue.tif",
         "rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013156_v1-red-green-blue.tif",
         "rasters/landsat:LC08:PRE:TOAR:meta_LC80260322013172_v1-red-green-blue.tif"]
        >>> # use explicit paths for a custom naming scheme:
        >>> paths = [
        ...     "{tile.key}/l8-{image.date:%Y-%m-%d-%H:%m}.tif".format(tile=tile, image=image)
        ...     for image in images
        ... ]  # doctest: +SKIP
        >>> images.download("nir red", tile, paths)  # doctest: +SKIP
        ["256:0:75.0:15:-5:230/l8-2013-04-18-16:04.tif",
         "256:0:75.0:15:-5:230/l8-2013-05-04-16:05.tif",
         "256:0:75.0:15:-5:230/l8-2013-05-20-16:05.tif",
         "256:0:75.0:15:-5:230/l8-2013-06-05-16:06.tif",
         "256:0:75.0:15:-5:230/l8-2013-06-21-16:06.tif"]

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
            If the ImageCollection is empty.
            If ``dest`` is a sequence not equal in length to the ImageCollection.
            If ``format`` is invalid, or a path has an invalid extension.
        TypeError
            If ``dest`` is not a string or a sequence type.
        NotFoundError
            If a Image's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given unrecognized parameters
        """
        if len(self) == 0:
            raise ValueError("This ImageCollection is empty")

        if geocontext is None:
            geocontext = self.geocontext
            if geocontext is None:
                raise ValueError(
                    "No geocontext supplied, and no default geocontext is defined for this ImageCollection"
                )
        if crs is not None or resolution is not None:
            try:
                params = {}
                if crs is not None:
                    params["crs"] = crs
                if resolution is not None:
                    params["resolution"] = resolution
                geocontext = geocontext.assign(**params)
            except TypeError:
                raise ValueError(
                    f"{type(geocontext)} geocontext does not support modifying crs or resolution"
                ) from None
        if all_touched is not None:
            geocontext = geocontext.assign(all_touched=all_touched)

        if dest is None:
            dest = "."

        bands = bands_to_list(bands)
        scales, data_type = multiproduct_scaling_parameters(
            self._product_bands(), bands, processing_level, scaling, data_type
        )

        if is_path_like(dest):
            default_pattern = "{image.id}-{bands}.{ext}"
            bands_str = "-".join(bands)
            try:
                dest = [
                    os.path.join(
                        dest,
                        default_pattern.format(
                            image=image, bands=bands_str, ext=format
                        ),
                    )
                    for image in self
                ]
            except Exception as e:
                raise RuntimeError(
                    "Error while generating default filenames:\n{}\n"
                    "This is likely due to missing or unexpected data "
                    "in Images in this ImageCollection.".format(e)
                ) from None

        try:
            if len(dest) != len(self):
                raise ValueError(
                    "`dest` contains {} items, but the ImageCollection contains {}".format(
                        len(dest), len(self)
                    )
                )
        except TypeError:
            raise TypeError(
                "`dest` should be a sequence of strings or path-like objects; "
                "instead found type {}, which has no length".format(type(dest))
            ) from None

        # check for duplicate paths to prevent the confusing situation where
        # multiple rasters overwrite the same filename
        unique = set()
        for path in dest:
            if path in unique:
                raise RuntimeError(
                    "Paths must be unique, but '{}' occurs multiple times".format(path)
                )
            else:
                unique.add(path)

        download_args = dict(
            resampler=resampler,
            processing_level=processing_level,
            scaling=scales,
            data_type=data_type,
            progress=progress,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    image._download, bands, geocontext, dest=path, **download_args
                ): path
                for image, path in zip(self, dest)
            }
            exceptions = []
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as ex:
                    exceptions.append((futures[future], ex))
            if exceptions:
                raise RuntimeError(
                    "One or more downloads failed: {}".format(exceptions)
                )
        return dest

    def download_mosaic(
        self,
        bands,
        geocontext=None,
        crs=None,
        resolution=None,
        all_touched=None,
        dest=None,
        format=DownloadFileFormat.TIF,
        resampler=ResampleAlgorithm.NEAR,
        processing_level=None,
        scaling=None,
        data_type=None,
        mask_alpha=None,
        nodata=None,
        progress=None,
    ):
        """
        Download all images as a single image file.
        Where multiple images overlap, only data from the image that comes last
        in the ImageCollection is used.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
        geocontext : :class:`~descarteslabs.common.geo.geocontext.GeoContext`, default None
            A :class:`~descarteslabs.common.geo.geocontext.GeoContext` to use when loading each image.
            If ``None`` then the default context of the collection will be used. If
            this is ``None``, a ValueError is raised.
        crs : str, default None
            if not None, update the gecontext with this value to set the output CRS.
        resolution : float, default None
            if not None, update the geocontext with this value to set the output resolution
            in the units native to the specified or defaulted output CRS.
        all_touched : float, default None
            if not None, update the geocontext with this value to control rastering behavior.
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
        format : `DownloadFileFormat`, default `DownloadFileFormat.TIF`
            Output file format to use.
            If a str or path-like object is given as ``dest``, ``format`` is ignored
            and determined from the extension on the path (one of ".tif", ".png", or ".jpg").
        resampler : `ResampleAlgorithm`, default `ResampleAlgorithm.NEAR`
            Algorithm used to interpolate pixel values when scaling and transforming
            the image to its new resolution or SRS.
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
            Whether to mask pixels in all bands where the alpha band of all images is 0.
            Provide a string to use an alternate band name for masking.
            If the alpha band is available for all images in the collection and
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
        >>> tile = dl.common.geo.DLTile.from_key("256:0:75.0:15:-5:230")  # doctest: +SKIP
        >>> product = dl.catalog.Product.get("landsat:LC08:PRE:TOAR")  # doctest: +SKIP
        >>> images = product.images().intersects(tile).limit(5).collect()  # doctest: +SKIP
        >>> images.download_mosaic("nir red", mask_alpha=False)  # doctest: +SKIP
        'mosaic-nir-red.tif'
        >>> images.download_mosaic("red green blue", dest="mosaics/{}.png".format(tile.key))  # doctest: +SKIP
        'mosaics/256:0:75.0:15:-5:230.tif'


        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the
            :class:`~descarteslabs.common.geo.geocontext.GeoContext`.
            If the ImageCollection is empty.
            If ``format`` is invalid, or the path has an invalid extension.
        NotFoundError
            If a Image's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs Platform is given unrecognized parameters
        """
        if len(self) == 0:
            raise ValueError("This ImageCollection is empty")

        if geocontext is None:
            geocontext = self.geocontext
            if geocontext is None:
                raise ValueError(
                    "No geocontext supplied, and no default geocontext is defined for this ImageCollection"
                )
        if crs is not None or resolution is not None:
            try:
                params = {}
                if crs is not None:
                    params["crs"] = crs
                if resolution is not None:
                    params["resolution"] = resolution
                geocontext = geocontext.assign(**params)
            except TypeError:
                raise ValueError(
                    f"{type(geocontext)} geocontext does not support modifying crs or resolution"
                ) from None
        if all_touched is not None:
            geocontext = geocontext.assign(all_touched=all_touched)

        bands = bands_to_list(bands)
        product_bands = self._product_bands()
        (bands, scaling, mask_alpha, drop_alpha) = self._mask_alpha_if_applicable(
            product_bands, bands, mask_alpha=mask_alpha, scaling=scaling
        )
        scales, data_type = multiproduct_scaling_parameters(
            product_bands, bands, processing_level, scaling, data_type
        )

        return download(
            inputs=self.each.id.collect(list),
            bands_list=bands,
            geocontext=geocontext,
            scales=scales,
            data_type=data_type,
            dest=dest,
            format=format,
            resampler=resampler,
            processing_level=processing_level,
            nodata=nodata,
            progress=progress,
        )

    def scaling_parameters(
        self, bands, processing_level=None, scaling=None, data_type=None
    ):
        """
        Computes fully defaulted scaling parameters and output data_type
        from provided specifications.

        This method is provided as a convenience to the user to help with
        understanding how ``scaling`` and ``data_type`` parameters passed
        to other methods on this class (e.g. :meth:`stack` or :meth:`mosaic`)
        will be interpreted. It would not usually be used in a normal
        workflow.

        A image collection may contain images from more than one product,
        introducing the possibility that the band properties for a band
        of a given name may differ from product to product. This method
        works in a similar fashion to the
        :meth:`Image.scaling_parameters <descarteslabs.catalog.image.Image.scaling_parameters>`
        method, but it additionally ensures that the resulting scale
        elements are compatible across the multiple products. If there
        is an incompatibility, an appropriate ValueError will be raised.

        Parameters
        ----------
        bands : list(str)
            List of bands to be scaled.
        processing_level : str, optional
            How the processing level of the underlying data should be adjusted. Possible
            values depend on the product and bands in use. Legacy products support
            ``toa`` (top of atmosphere) and in some cases ``surface``. Consult the
            available ``processing_levels`` in the product bands to understand what
            is available.
        scaling : None or str or list or dict
            Band scaling specification. See
            :meth:`Image.scaling_parameters <descarteslabs.catalog.image.Image.scaling_parameters>`
            for a full description of this parameter.
        data_type : None or str
            Result data type desired, as a standard data type string (e.g.
            ``"Byte"``, ``"Uint16"``, or ``"Float64"``). If not specified,
            will be deduced from the ``scaling`` specification. See
            :meth:`Image.scaling_parameters <descarteslabs.catalog.image.Image.scaling_parameters>`
            for a full description of this parameter.

        Returns
        -------
        scales : list(tuple)
            The fully specified scaling parameter, compatible with the
            :class:`~descarteslabs.client.services.raster.Raster` API and the
            output data type.
        data_type : str
            The result data type as a standard GDAL type string.

        Raises
        ------
        ValueError
            If any invalid or incompatible value is passed to any of the
            three parameters.

        See Also
        --------
        :doc:`Catalog Guide </guides/catalog>` : This contains many examples of the use of
        the ``scaling`` and ``data_type`` parameters.
        """
        bands = bands_to_list(bands)
        return multiproduct_scaling_parameters(
            self._product_bands(), bands, processing_level, scaling, data_type
        )

    def __repr__(self):
        parts = [
            "ImageCollection of {} image{}".format(
                len(self), "" if len(self) == 1 else "s"
            )
        ]
        try:
            first = min(self.each.date)
            last = max(self.each.date)
            dates = "  * Dates: {:%b %d, %Y} to {:%b %d, %Y}".format(first, last)
            parts.append(dates)
        except Exception:
            pass

        try:
            products = self.each.product_id.combine(collections.Counter)
            if len(products) > 0:
                products = ", ".join("{}: {}".format(k, v) for k, v in products.items())
                products = "  * Products: {}".format(products)
                parts.append(products)
        except Exception:
            pass

        return "\n".join(parts)

    def _product_bands(self):
        product_ids = set(map(lambda i: i.product_id, self._list))
        return {
            product_id: cached_bands_by_product(product_id, self._client)
            for product_id in product_ids
        }

    def _mask_alpha_if_applicable(
        self, product_bands, bands, mask_alpha=None, scaling=None
    ):
        alpha_band_name = "alpha"
        if isinstance(mask_alpha, str):
            alpha_band_name = mask_alpha
            mask_alpha = True
        elif mask_alpha is None:
            mask_alpha = all(
                map(lambda b: alpha_band_name in b, product_bands.values())
            )
        elif type(mask_alpha) is not bool:
            raise ValueError("'mask_alpha' must be None, a band name, or a bool.")

        drop_alpha = False
        if mask_alpha:
            try:
                alpha_i = bands.index(alpha_band_name)
            except ValueError:
                bands.append(alpha_band_name)
                drop_alpha = True
                scaling = append_alpha_scaling(scaling)
            else:
                if alpha_i != len(bands) - 1:
                    raise ValueError(
                        "Alpha must be the last band in order to reduce rasterization errors"
                    )
        return (bands, scaling, mask_alpha, drop_alpha)


# deal with circular import problem
from .image import Image  # noqa: E402

ImageCollection._item_type = Image
