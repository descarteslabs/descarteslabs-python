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
Create a SceneCollection by searching:

.. ipython::

    In [1]: import descarteslabs as dl

    In [2]: import numpy as np

    In [3]: aoi_geometry = {'type': 'Polygon',
       ...:  'coordinates': (((-95.27841503861751, 42.76556057019057),
       ...:    (-93.15675252485482, 42.36289849433184),
       ...:    (-93.73350276458868, 40.73810018004927),
       ...:    (-95.79766011799035, 41.13809376845988),
       ...:    (-95.27841503861751, 42.76556057019057)),)}

    In [4]: scenes, ctx = dl.scenes.search(aoi_geometry, products=["landsat:LC08:PRE:TOAR"], limit=10)

    @doctest
    In [5]: scenes
    Out[5]:
    SceneCollection of 10 scenes
      * Dates: Apr 18, 2013 to Sep 09, 2013
      * Products: landsat:LC08:PRE:TOAR: 10

Use `SceneCollection.each` and `SceneCollection.filter` to subselect Scenes you want:

.. ipython::

    In [6]: # which month is each scene from?

    @doctest
    In [7]: scenes.each.properties.date.month.combine()
    Out[7]: [4, 5, 5, 6, 6, 7, 7, 8, 8, 9]

    In [8]: spring_scenes = scenes.filter(lambda s: s.properties.date.month <= 6)

    @doctest
    In [9]: spring_scenes
    Out[9]:
    SceneCollection of 5 scenes
      * Dates: Apr 18, 2013 to Jun 21, 2013
      * Products: landsat:LC08:PRE:TOAR: 5

Operate on related Scenes with `SceneCollection.groupby`:

.. ipython::

    @doctest
    In [10]: for month, month_scenes in spring_scenes.groupby("properties.date.month"):
       ....:    print("Month {}: {} scenes".format(month, len(month_scenes)))
       ....:
    Month 4: 1 scenes
    Month 5: 2 scenes
    Month 6: 2 scenes

Load data with `SceneCollection.stack` or `SceneCollection.mosaic`:

.. ipython::

    In [11]: ctx_lowres = ctx.assign(resolution=120)

    In [12]: stack = spring_scenes.stack("red green blue", ctx_lowres)

    In [13]: stack.shape
    Out[13]: (5, 3, 3690, 3724)

Download georeferenced images with `SceneCollection.download` and `SceneCollection.download_mosaic`:

.. ipython::

    @doctest
    In [14]: spring_scenes.download("red green blue", ctx_lowres, "rasters")
    Out[14]:
    ['rasters/landsat:LC08:PRE:TOAR:meta_LC80260312013108_v1-red-green-blue.tif',
     'rasters/landsat:LC08:PRE:TOAR:meta_LC80260312013124_v1-red-green-blue.tif',
     'rasters/landsat:LC08:PRE:TOAR:meta_LC80260312013140_v1-red-green-blue.tif',
     'rasters/landsat:LC08:PRE:TOAR:meta_LC80260312013156_v1-red-green-blue.tif',
     'rasters/landsat:LC08:PRE:TOAR:meta_LC80260312013172_v1-red-green-blue.tif']

    @doctest
    In [15]: spring_scenes.download_mosaic("nir red", ctx_lowres)
    Out[15]: 'mosaic-nir-red.tif'
"""

from __future__ import division
import collections
import six
import logging
import json
import os.path

from descarteslabs.client.addons import concurrent, numpy as np

from descarteslabs.client.services.raster import Raster
from descarteslabs.client.exceptions import NotFoundError, BadRequestError

from .collection import Collection
from .scene import Scene
from . import _download


class SceneCollection(Collection):
    """
    Holds Scenes, with methods for loading their data.

    As a subclass of `Collection`, the `filter`, `map`, and `groupby`
    methods and `each` property simplify inspection and subselection of
    contianed Scenes.

    `stack` and `mosaic` rasterize all contained Scenes into an ndarray
    using the a GeoContext.
    """
    def __init__(self, iterable=None, raster_client=None):
        super(SceneCollection, self).__init__(iterable)
        self._raster_client = raster_client if raster_client is not None else Raster()

    def map(self, f):
        """
        Returns list of ``f`` applied to each item in self,
        or SceneCollection if ``f`` returns Scenes
        """
        res = super(SceneCollection, self).map(f)
        if all(isinstance(x, Scene) for x in res):
            return res
        else:
            return list(res)

    def filter_coverage(self, ctx, minimum_coverage=1):
        """
        Include scenes having footprints with a minimum
        coverage compared to the geometry of the `GeoContext`.

        See :func:`Scene.coverage <descarteslabs.scenes.scene.Scene.coverage>`
        for getting coverage information for a scene.

        Parameters
        ----------
        ctx : `GeoContext`
            A `GeoContext` to use when filtering scenes
        minimum_coverage : float
            Include scenes with greater than or equal to this
            fraction of coverage.

        Returns
        -------
        scenes : SceneCollection

        Example
        -------
        >>> import descarteslabs as dl
        >>> aoi_geometry = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-95, 42],[-93, 42],[-93, 40],[-95, 41],[-95, 42]]]}
        >>> scenes, ctx = dl.scenes.search(aoi_geometry, products=["landsat:LC08:PRE:TOAR"], limit=20,
        ...    sort_field='processed')
        >>> filtered_scenes = scenes.filter_coverage(ctx, 0.50)  # doctest: +SKIP
        >>> assert len(filtered_scenes) < len(scenes)  # doctest: +SKIP
        """

        return self.filter(lambda scene: scene.coverage(ctx) >= minimum_coverage)

    def stack(self,
              bands,
              ctx,
              flatten=None,
              mask_nodata=True,
              mask_alpha=True,
              bands_axis=1,
              raster_info=False,
              resampler="near",
              max_workers=None,
              ):
        """
        Load bands from all scenes and stack them into a 4D ndarray,
        optionally masking invalid data.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
            If the alpha band is requested, it must be last in the list
            to reduce rasterization errors.
        ctx : `GeoContext`
            A `GeoContext` to use when loading each Scene
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
        mask_alpha : bool, default True
            Whether to mask pixels in all bands of each scene where
            the alpha band is 0.
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
        raster_info : List[dict]
            If ``raster_info=True``, a list of raster information dicts for each scene
            is also returned

        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the GeoContext.
            If the SceneCollection is empty.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs platform is given unrecognized parameters
        """
        if len(self) == 0:
            raise ValueError("This SceneCollection is empty")

        kwargs = dict(
            mask_nodata=mask_nodata,
            mask_alpha=mask_alpha,
            bands_axis=bands_axis,
            raster_info=raster_info,
            resampler=resampler,
        )

        if bands_axis == 0 or bands_axis == -4:
            raise NotImplementedError(
                "bands_axis of 0 is currently unsupported for `SceneCollection.stack`. "
                "If you require this shape, try ``np.moveaxis(my_stack, 1, 0)`` on the returned ndarray."
            )
        elif bands_axis > 0:
            kwargs['bands_axis'] = bands_axis - 1  # the bands axis for each component ndarray call in the stack

        if flatten is not None:
            if isinstance(flatten, six.string_types) or not hasattr(flatten, "__len__"):
                flatten = [flatten]
            scenes = [sc if len(sc) > 1 else sc[0] for group, sc in self.groupby(*flatten)]
        else:
            scenes = self

        full_stack = None
        mask = None
        if raster_info:
            raster_infos = [None] * len(scenes)

        bands = Scene._bands_to_list(bands)
        pop_alpha = False
        if mask_alpha and "alpha" not in bands:
            pop_alpha = True
            bands.append("alpha")
        # Pre-check that all bands and alpha are available in all Scenes, and all have the same dtypes
        self._common_data_type(bands)
        if pop_alpha:
            bands.pop(-1)

        def threaded_ndarrays():
            def data_loader(scene_or_scenecollection, bands, ctx, **kwargs):
                ndarray_kwargs = dict(kwargs, raster_client=self._raster_client)
                if isinstance(scene_or_scenecollection, self.__class__):
                    return lambda: scene_or_scenecollection.mosaic(bands, ctx, **kwargs)
                else:
                    return lambda: scene_or_scenecollection.ndarray(bands, ctx, **ndarray_kwargs)

            try:
                futures = concurrent.futures
            except ImportError:
                logging.warning(
                    "Failed to import concurrent.futures. ndarray calls will be serial."
                )
                for i, scene_or_scenecollection in enumerate(scenes):
                    yield i, data_loader(scene_or_scenecollection, bands, ctx, **kwargs)()
            else:
                with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                    future_ndarrays = {}
                    for i, scene_or_scenecollection in enumerate(scenes):
                        future_ndarray = executor.submit(data_loader(scene_or_scenecollection, bands, ctx, **kwargs))
                        future_ndarrays[future_ndarray] = i
                    for future in futures.as_completed(future_ndarrays):
                        i = future_ndarrays[future]
                        result = future.result()
                        yield i, result

        for i, arr in threaded_ndarrays():
            if raster_info:
                arr, raster_meta = arr
                raster_infos[i] = raster_meta

            if full_stack is None:
                stack_shape = (len(scenes),) + arr.shape
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

    def mosaic(self,
               bands,
               ctx,
               mask_nodata=True,
               mask_alpha=True,
               bands_axis=0,
               resampler="near",
               raster_info=False,
               ):
        """
        Load bands from all scenes, combining them into a single 3D ndarray
        and optionally masking invalid data.

        Where multiple scenes overlap, only data from the scene that comes last
        in the SceneCollection is used.

        Parameters
        ----------
        bands : str or Sequence[str]
            Band names to load. Can be a single string of band names
            separated by spaces (``"red green blue"``),
            or a sequence of band names (``["red", "green", "blue"]``).
            If the alpha band is requested, it must be last in the list
            to reduce rasterization errors.
        ctx : `GeoContext`
            A `GeoContext` to use when loading each Scene
        mask_nodata : bool, default True
            Whether to mask out values in each band that equal
            that band's ``nodata`` sentinel value.
        mask_alpha : bool, default True
            Whether to mask pixels in all bands where the alpha band of all scenes is 0.
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


        Returns
        -------
        arr : ndarray
            Returned array's shape will be ``(band, y, x)`` if ``bands_axis``
            is 0, and ``(y, x, band)`` if ``bands_axis`` is -1.
            If ``mask_nodata`` or ``mask_alpha`` is True, arr will be a masked array.
        raster_info : dict
            If ``raster_info=True``, a raster information dict is also returned.

        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the GeoContext.
            If the SceneCollection is empty.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs platform is given unrecognized parameters
        """
        if len(self) == 0:
            raise ValueError("This SceneCollection is empty")

        if not (-3 < bands_axis < 3):
            raise ValueError("Invalid bands_axis; axis {} would not exist in a 3D array".format(bands_axis))

        bands = Scene._bands_to_list(bands)

        if mask_alpha:
            try:
                alpha_i = bands.index("alpha")
            except ValueError:
                bands.append("alpha")
                drop_alpha = True
            else:
                if alpha_i != len(bands) - 1:
                    raise ValueError("Alpha must be the last band in order to reduce rasterization errors")
                drop_alpha = False

        common_data_type = self._common_data_type(bands)

        raster_params = ctx.raster_params
        full_raster_args = dict(
            inputs=[scene.properties["id"] for scene in self],
            order="gdal",
            bands=bands,
            scales=None,
            data_type=common_data_type,
            resampler=resampler,
            **raster_params
        )

        try:
            arr, info = self._raster_client.ndarray(**full_raster_args)
        except NotFoundError as e:
            raise NotFoundError(
                "Some or all of these IDs don't exist in the Descartes catalog: {}".format(full_raster_args["inputs"])
            )
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
                # collect all possible nodata values per band,
                # in case different products have different nodata values for the same-named band
                # QUESTION: is this overkill?
                band_nodata_values = collections.defaultdict(set)
                for scene in self:
                    scene_bands = scene.properties["bands"]
                    for bandname in bands:
                        band_nodata_values[bandname].add(scene_bands[bandname].get('nodata'))

                for i, bandname in enumerate(bands):
                    for nodata in band_nodata_values[bandname]:
                        if nodata is not None:
                            mask[i] |= arr[i] == nodata

            if mask_alpha:
                mask |= alpha == 0

            arr = np.ma.MaskedArray(arr, mask, copy=False)

        if bands_axis != 0:
            arr = np.moveaxis(arr, 0, bands_axis)
        if raster_info:
            return arr, info
        else:
            return arr

    def download(self,
                 bands,
                 ctx,
                 dest,
                 format="tif",
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
        ctx : `GeoContext`
            A `GeoContext` to use when loading each Scene
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
        >>> tile = dl.scenes.DLTile.from_key("256:0:75.0:15:-5:230")
        >>> scenes, _ = dl.scenes.search(tile, products=["landsat:LC08:PRE:TOAR"], limit=5)
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
        ... ]
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
            If not all required parameters are specified in the GeoContext.
            If the SceneCollection is empty.
            If ``dest`` is a sequence not equal in length to the SceneCollection.
            If ``format`` is invalid, or a path has an invalid extension.
        TypeError
            If ``dest`` is not a string or a sequence type.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs platform is given unrecognized parameters
        """
        if len(self) == 0:
            raise ValueError("This SceneCollection is empty")

        bands = Scene._bands_to_list(bands)
        # Pre-check that all bands are available in all Scenes, and all have the same dtypes
        self._common_data_type(bands)

        if _download._is_path_like(dest):
            default_pattern = "{scene.properties.id}-{bands}.{ext}"
            bands_str = "-".join(bands)
            try:
                dest = [
                    os.path.join(dest, default_pattern.format(scene=scene, bands=bands_str, ext=format))
                    for scene in self
                ]
            except Exception as e:
                six.raise_from(
                    RuntimeError(
                        "Error while generating default filenames:\n{}\n"
                        "This is likely due to missing or unexpected data "
                        "in Scenes in this SceneCollection.".format(e)
                    ), None
                )

        try:
            if len(dest) != len(self):
                raise ValueError(
                    "`dest` contains {} items, but the SceneCollection contains {}".format(len(dest), len(self))
                )
        except TypeError:
            six.raise_from(TypeError(
                "`dest` should be a sequence of strings or path-like objects; "
                "instead found type {}, which has no length".format(type(dest))
            ), None)

        # check for duplicate paths to prevent the confusing situation where
        # multiple rasters overwrite the same filename
        unique = set()
        for path in dest:
            if path in unique:
                raise RuntimeError("Paths must be unique, but '{}' occurs multiple times".format(path))
            else:
                unique.add(path)

        try:
            futures = concurrent.futures
        except ImportError:
            logging.warning(
                "Failed to import concurrent.futures. Download calls will be serial."
            )
            for scene, path in zip(self, dest):
                scene.download(bands, ctx, dest=path, raster_client=self._raster_client)
        else:
            with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(
                        scene.download, bands, ctx, dest=path, raster_client=self._raster_client
                    )
                    for scene, path in zip(self, dest)
                ]
                concurrent.futures.wait(futures)
        return dest

    def download_mosaic(self,
                        bands,
                        ctx,
                        dest=None,
                        format="tif",
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
        ctx : `GeoContext`
            A `GeoContext` to use when loading the Scenes
        dest : str, path-like object, or file-like object, default None
            Where to write the image file.

            * If None (default), it's written to an image file of the given ``format``
              in the current directory, named by the requested bands,
              like ``"mosaic-red-green-blue.tif"``
            * If a string or path-like object, it's written to that path.

              Any file already existing at that path will be overwritten.

              Any intermediate directories will be created if they don't exist.

              Note that path-like objects (such as pathlib.Path) are only supported
              in Python 3.6 or later.
            * If a file-like object, it's written into that file.
        format : str, default "tif"
            If a file-like object or None is given as ``dest``: one of "tif", "png", or "jpg".

            If a str or path-like object is given as ``dest``, ``format`` is ignored
            and determined from the extension on the path (one of ".tif", ".png", or ".jpg").

        Returns
        -------
        path : str or None
            If ``dest`` is a path or None, the path where the image file was written is returned.
            If ``dest`` is file-like, nothing is returned.

        Example
        -------
        >>> import descarteslabs as dl
        >>> tile = dl.scenes.DLTile.from_key("256:0:75.0:15:-5:230")
        >>> scenes, _ = dl.scenes.search(tile, products=["landsat:LC08:PRE:TOAR"], limit=5)
        >>> scenes.download_mosaic("nir red", tile)  # doctest: +SKIP
        'mosaic-nir-red.jpg'
        >>> scenes.download_mosaic("nir red", tile, dest="mosaics/{}.png".format(tile.key))  # doctest: +SKIP
        'mosaics/256:0:75.0:15:-5:230.png'
        >>> with open("another_mosaic.jpg", "wb") as f:
        ...     scenes.download_mosaic("swir2", tile, dest=f, format="jpg")  # doctest: +SKIP


        Raises
        ------
        ValueError
            If requested bands are unavailable, or band names are not given
            or are invalid.
            If not all required parameters are specified in the GeoContext.
            If the SceneCollection is empty.
            If ``format`` is invalid, or the path has an invalid extension.
        NotFoundError
            If a Scene's ID cannot be found in the Descartes Labs catalog
        BadRequestError
            If the Descartes Labs platform is given unrecognized parameters
        """
        if len(self) == 0:
            raise ValueError("This SceneCollection is empty")

        bands = Scene._bands_to_list(bands)
        common_data_type = self._common_data_type(bands)

        return _download._download(
            inputs=self.each.properties["id"].combine(),
            bands_list=bands,
            ctx=ctx,
            dtype=common_data_type,
            dest=dest,
            format=format,
            raster_client=self._raster_client,
        )

    def __repr__(self):
        parts = ["SceneCollection of {} scene{}".format(len(self), "" if len(self) == 1 else "s")]
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
                products = ", ".join("{}: {}".format(k, v) for k, v in six.iteritems(products))
                products = "  * Products: {}".format(products)
                parts.append(products)
        except Exception:
            pass

        return "\n".join(parts)

    def _common_data_type(self, bands):
        data_types = [scene._common_data_type_of_bands(bands) for scene in self]
        common_data_type = None
        for i, data_type in enumerate(data_types):
            if common_data_type is None:
                common_data_type = data_type
            else:
                if data_type != common_data_type:
                    raise ValueError(
                        "Bands must all have the same dtype in every Scene. "
                        "The requested bands in Scene {} have dtype '{}', but all prior Scenes had dtype '{}'"
                        .format(i, data_type, common_data_type)
                    )
        return common_data_type
