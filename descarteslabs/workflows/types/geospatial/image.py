import json
import uuid

from descarteslabs import scenes

from ... import _channel, env
from ...cereal import serializable
from ...models import XYZ
from ..containers import Dict, KnownDict, Struct, Tuple
from ..core import typecheck_promote
from ..datetimes import Datetime
from ..primitives import Any, Bool, Float, Int, Str
from .feature import Feature
from .featurecollection import FeatureCollection
from .geometry import Geometry
from .mixins import BandsMixin


def _DelayedImageCollection():
    from .imagecollection import ImageCollection

    return ImageCollection


ImageBase = Struct[
    {
        "properties": KnownDict[
            {
                "id": Str,
                "date": Datetime,
                "product": Str,
                "crs": Str,
                "geotrans": Tuple[Float, Float, Float, Float, Float, Float],
            },
            Str,
            Any,
        ],
        "bandinfo": Dict[
            Str,
            KnownDict[
                {
                    "id": Str,
                    "name": Str,
                    # "unit": Str,
                    "data_range": Tuple[Float, Float],
                    # "physical_range": Tuple[Float, Float],
                },
                Str,
                Any,
            ],
        ],
    }
]


@serializable(is_named_concrete_type=True)
class Image(ImageBase, BandsMixin):
    _doc = {
        "properties": """\
            Metadata for the `Image`.

            ``properties`` is a `Dict` which always contains these fields:

            * ``id`` (`.Str`): the Descartes Labs ID of the Image
            * ``product`` (`.Str`): the Descartes Labs ID of the product the Image belogs to
            * ``date`` (`.Datetime`): the UTC date the Image was acquired
            * ``crs`` (`.Str`): the original Coordinate Reference System of the Image
            * ``geotrans`` (`.Tuple`): The original 6-tuple GDAL geotrans for the Image.

            Accessing other fields will return instances of `.Any`.
            Accessing fields that don't actually exist on the data is a compute-time error.

            Example
            -------
            >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
            >>> img.properties['date']
            <descarteslabs.workflows.types.datetimes.datetime_.Datetime at 0x7f45e8a3d5d0>
            >>> img.properties['date'].year
            <descarteslabs.workflows.types.primitives.number.Int at 0x7f45c6808cd0>
            >>> img.properties['id']
            <descarteslabs.workflows.types.primitives.string.Str at 0x7f45c9e37250>
            >>> img.properties['foobar']  # almost certainly a compute-time error
            <descarteslabs.workflows.types.primitives.any_.Any at 0x7f45c6808650>
            """,
        "bandinfo": """\
            Metadata about the bands of the `Image`.

            ``bandinfo`` is a `Dict`, where keys are band names and values are Dicts
            which always contain these fields:

            * ``id`` (`.Str`): the Descartes Labs ID of the band
            * ``name`` (`.Str`): the name of the band. Equal to the key the Dict
              is stored under in ``bandinfo``
            * ``data_range`` (`.Tuple`): The ``(min, max)`` values the original data had.
              However, data in Images is automatically rescaled to physical range,
              or ``[0, 1]`` if physical range is undefined, so it won't be in ``data_range``
              anymore.

            Accessing other fields will return instances of `.Any`.
            Accessing fields that don't actually exist on the data is a compute-time error.

            Example
            -------
            >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
            >>> img.bandinfo['red']['data_rage']
            <descarteslabs.workflows.types.containers.tuple_.Tuple[Float, Float] at 0x7f45c6801950>
            >>> img.bandinfo['red']['foobar']  # almost certainly a compute-time error
            <descarteslabs.workflows.types.primitives.any_.Any at 0x7f45c681be50>
            >>> img.bandinfo['foobar']['id']  # also likely a compute-time error
            <descarteslabs.workflows.types.primitives.any_.Any at 0x7f45c681be50>
            """,
    }

    def __init__(self):
        raise TypeError(
            "Please use a classmethod such as `Image.from_id` or `Image.from_scene` to instantiate an Image."
        )

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, scenes.Scene):
            return cls.from_scene(obj)
        return super(Image, cls)._promote(obj)

    @classmethod
    @typecheck_promote(Str)
    def from_id(cls, image_id, resampler=None):
        "Create a proxy `Image` from an ID in the Descartes Labs catalog"
        if resampler is not None and resampler not in [
            "near",
            "bilinear",
            "cubic",
            "cubicsplice",
            "lanczos",
            "average",
            "mode",
            "max",
            "min",
            "med",
            "q1",
            "q3",
        ]:
            raise ValueError("Unknown resampler type: {}".format(resampler))
        return cls._from_apply(
            "Image.load",
            image_id,
            geocontext=env.geoctx,
            token=env._token,
            resampler=resampler,
        )

    @classmethod
    def from_scene(cls, scene):
        "Create a proxy image from a `~descarteslabs.scenes.scene.Scene` object"
        return cls.from_id(scene.properties["id"])

    @typecheck_promote(lambda: Image)
    def concat_bands(self, other_image):
        """
        New `Image`, with the bands in ``other_image`` appended to this one.

        If band names overlap, the band from the *other* `Image` will be suffixed with "_1".
        """
        return self._from_apply("Image.concat_bands", self, other_image)

    @typecheck_promote(
        (lambda: Image, Geometry, Feature, FeatureCollection), replace=Bool
    )
    def mask(self, mask, replace=False):
        """
        New `Image`, masked with a boolean `Image` or vector object.

        Parameters
        ----------
        mask: `Image`, `Geometry`, `~.workflows.types.geospatial.Feature`, `~.workflows.types.geospatial.FeatureCollection`
            A single-band `Image` of boolean values,
            (such as produced by ``img > 2``, for example)
            where True means masked (invalid).

            Or, a vector (`Geometry`, `~.workflows.types.geospatial.Feature`,
            or `~.workflows.types.geospatial.FeatureCollection`),
            in which case pixels *outside* the vector are masked.
        replace: Bool, default False
            If False (default), adds this mask to the current one,
            so already-masked pixels remain masked,
            or replaces the current mask with this new one if True.
        """  # noqa
        if isinstance(mask, (Geometry, Feature, FeatureCollection)):
            mask = mask.rasterize().getmask()
        return self._from_apply("mask", self, mask, replace=replace)

    def getmask(self):
        "Mask of this `Image`, as a new `Image` with one boolean band named ``'mask'``"
        return self._from_apply("getmask", self)

    def colormap(self, named_colormap="viridis", vmin=None, vmax=None):
        """
        Apply a colormap to an `Image`. Image must have a single band.

        Parameters
        ----------
        named_colormap: str, default "viridis"
            The name of the Colormap registered with matplotlib.
            See https://matplotlib.org/users/colormaps.html for colormap options.
        vmin: float, default None
            The minimum value of the range to normalize the bands within.
            If specified, vmax must be specified as well.
        vmax: float, default None
            The maximum value of the range to normalize the bands within.
            If specified, vmin must be specified as well.

        Note: If neither vmin nor vmax are specified, the min and max values in the `Image` will be used.
        """
        if (vmin is not None and vmax is None) or (vmin is None and vmax is not None):
            raise ValueError("Must specify both vmin and vmax, or neither.")
        if named_colormap not in [
            "viridis",
            "plasma",
            "inferno",
            "magma",
            "cividis",
            "Greys",
            "Purples",
            "Blues",
            "Greens",
            "Oranges",
            "Reds",
            "YlOrBr",
            "YlOrRd",
            "OrRd",
            "PuRd",
            "RdPu",
            "BuPu",
            "GnBu",
            "PuBu",
            "YlGnBu",
            "PuBuGn",
            "BuGn",
            "YlGn",
            "binary",
            "gist_yarg",
            "gist_gray",
            "gray",
            "bone",
            "pink",
            "spring",
            "summer",
            "autumn",
            "winter",
            "cool",
            "Wistia",
            "hot",
            "afmhot",
            "gist_heat",
            "copper",
            "PiYG",
            "PRGn",
            "BrBG",
            "PuOr",
            "RdGy",
            "RdBu",
            "RdYlBu",
            "RdYlGn",
            "Spectral",
            "coolwarm",
            "bwr",
            "seismic",
            "twilight",
            "twilight_shifted",
            "hsv",
            "Pastel1",
            "Pastel2",
            "Paired",
            "Accent",
            "Dark2",
            "Set1",
            "Set2",
            "Set3",
            "tab10",
            "tab20",
            "tab20b",
            "tab20c",
            "flag",
            "prism",
            "ocean",
            "gist_earth",
            "terrain",
            "gist_stern",
            "gnuplot",
            "gnuplot2",
            "CMRmap",
            "cubehelix",
            "brg",
            "gist_rainbow",
            "rainbow",
            "jet",
            "nipy_spectral",
            "gist_ncar",
        ]:
            raise ValueError("Unknown colormap type: {}".format(named_colormap))
        return self._from_apply("colormap", self, named_colormap, vmin, vmax)

    def minpixels(self):
        """
        Dict[Str, Float] of each band's minimum pixel value

        Note: Each band name in the dictionary will have '_amin' appended to it.
        """
        return Dict[Str, Float]._from_apply("min", self)

    # def minbands(self):
    #     """
    #     New Image with 1 band, 'min',
    #     containing the minimum value for each pixel across all bands
    #     """
    #     raise NotImplementedError()
    #     return self._from_apply("Image.minbands", self)

    def maxpixels(self):
        """
        Dict[Str, Float] of each band's maximum pixel value

        Note: Each band name in the dictionary will have '_amax' appended to it.
        """
        return Dict[Str, Float]._from_apply("max", self)

    # def maxbands(self):
    #     """
    #     New Image with 1 band, 'max',
    #     containing the maximum value for each pixel across all bands
    #     """
    #     raise NotImplementedError()
    #     return self._from_apply("Image.maxbands", self)

    def meanpixels(self):
        """
        Dict[Str, Float] of each band's mean pixel value

        Note: Each band name in the dictionary will have '_mean' appended to it.
        """
        return Dict[Str, Float]._from_apply("mean", self)

    # def meanbands(self):
    #     """
    #     New Image with 1 band, 'mean',
    #     containing the mean value for each pixel across all bands
    #     """
    #     raise NotImplementedError()
    #     return self._from_apply("Image.meanbands", self)

    def medianpixels(self):
        """
        Dict[Str, Float] of each band's median pixel value

        Note: Each band name in the dictionary will have '_median' appended to it.
        """
        return Dict[Str, Float]._from_apply("median", self)

    # def medianbands(self):
    #     """
    #     New Image with 1 band, 'median',
    #     containing the median value for each pixel across all bands
    #     """
    #     raise NotImplementedError()
    #     return self._from_apply("Image.medianbands", self)

    def sumpixels(self):
        """
        Dict[Str, Float] of each band's sum pixel value

        Note: Each band name in the dictionary will have '_sum' appended to it.
        """
        return Dict[Str, Float]._from_apply("sum", self)

    # def sumbands(self):
    #     """
    #     New Image with 1 band, 'sum',
    #     containing the sum for each pixel across all bands
    #     """
    #     raise NotImplementedError()
    #     return self._from_apply("Image.sumbands", self)

    def stdpixels(self):
        """
        Dict[Str, Float] of each band's std pixel value

        Note: Each band name in the dictionary will have '_std' appended to it.
        """
        return Dict[Str, Float]._from_apply("std", self)

    # def stdbands(self):
    #     """
    #     New Image with 1 band, 'std',
    #     containing the standard deviation for each pixel across all bands
    #     """
    #     raise NotImplementedError()
    #     return self._from_apply("Image.stdbands", self)

    def countpixels(self):
        """
        Dict[Str, Float] of each band's count pixel value

        Note: Each band name in the dictionary will have '_get_mask_sum' appended to it.
        """
        return Dict[Str, Float]._from_apply("count", self)

    # def countbands(self):
    #     """
    #     New Image with 1 band, 'count',
    #     containing the number of unmasked pixels across all bands
    #     """
    #     raise NotImplementedError()
    #     return self._from_apply("Image.countbands", self)

    # Binary comparators
    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __lt__(self, other):
        return _result_type(other)._from_apply("lt", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __le__(self, other):
        return _result_type(other)._from_apply("le", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float, Bool)
    )
    def __eq__(self, other):
        return _result_type(other)._from_apply("eq", self, other)

    @typecheck_promote(
        (lambda: Image, lambda: _DelayedImageCollection(), Int, Float, Bool)
    )
    def __ne__(self, other):
        return _result_type(other)._from_apply("ne", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __gt__(self, other):
        return _result_type(other)._from_apply("gt", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __ge__(self, other):
        return _result_type(other)._from_apply("ge", self, other)

    # Bitwise operators
    def __invert__(self):
        return self._from_apply("invert", self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __and__(self, other):
        return _result_type(other)._from_apply("and", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __or__(self, other):
        return _result_type(other)._from_apply("or", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __xor__(self, other):
        return _result_type(other)._from_apply("xor", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int))
    def __lshift__(self, other):
        return _result_type(other)._from_apply("lshift", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int))
    def __rshift__(self, other):
        return _result_type(other)._from_apply("rshift", self, other)

    # Reflected bitwise operators
    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __rand__(self, other):
        return _result_type(other)._from_apply("rand", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __ror__(self, other):
        return _result_type(other)._from_apply("ror", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Bool))
    def __rxor__(self, other):
        return _result_type(other)._from_apply("rxor", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int))
    def __rlshift__(self, other):
        return _result_type(other)._from_apply("rlshift", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int))
    def __rrshift__(self, other):
        return _result_type(other)._from_apply("rrshift", self, other)

    # Arithmetic operators
    def log(img):
        "Element-wise natural log of an `Image`"
        from ..toplevel import arithmetic

        return arithmetic.log(img)

    def log2(img):
        "Element-wise base 2 log of an `Image`"
        from ..toplevel import arithmetic

        return arithmetic.log2(img)

    def log10(img):
        "Element-wise base 10 log of an `Image`"
        from ..toplevel import arithmetic

        return arithmetic.log10(img)

    def sqrt(self):
        "Element-wise square root of an `Image`"
        from ..toplevel import arithmetic

        return arithmetic.sqrt(self)

    def cos(self):
        "Element-wise cosine of an `Image`"
        from ..toplevel import arithmetic

        return arithmetic.cos(self)

    def sin(self):
        "Element-wise sine of an `Image`"
        from ..toplevel import arithmetic

        return arithmetic.sin(self)

    def tan(self):
        "Element-wise tangent of an `Image`"
        from ..toplevel import arithmetic

        return arithmetic.tan(self)

    def __neg__(self):
        return self._from_apply("neg", self)

    def __pos__(self):
        return self._from_apply("pos", self)

    def __abs__(self):
        return self._from_apply("abs", self)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __add__(self, other):
        return _result_type(other)._from_apply("add", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __sub__(self, other):
        return _result_type(other)._from_apply("sub", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __mul__(self, other):
        return _result_type(other)._from_apply("mul", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __div__(self, other):
        return _result_type(other)._from_apply("div", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __truediv__(self, other):
        return _result_type(other)._from_apply("div", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __floordiv__(self, other):
        return _result_type(other)._from_apply("floordiv", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __mod__(self, other):
        return _result_type(other)._from_apply("mod", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __pow__(self, other):
        return _result_type(other)._from_apply("pow", self, other)

    # Reflected arithmetic operators
    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __radd__(self, other):
        return _result_type(other)._from_apply("radd", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rsub__(self, other):
        return _result_type(other)._from_apply("rsub", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rmul__(self, other):
        return _result_type(other)._from_apply("rmul", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rdiv__(self, other):
        return _result_type(other)._from_apply("rdiv", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rtruediv__(self, other):
        return _result_type(other)._from_apply("rtruediv", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rfloordiv__(self, other):
        return _result_type(other)._from_apply("rfloordiv", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rmod__(self, other):
        return _result_type(other)._from_apply("rmod", self, other)

    @typecheck_promote((lambda: Image, lambda: _DelayedImageCollection(), Int, Float))
    def __rpow__(self, other):
        return _result_type(other)._from_apply("rpow", self, other)

    def tile_layer(self, name=None, scales=None, colormap=None):
        """
        A `.WorkflowsLayer` for this `Image`.

        Generally, use `Image.visualize` for displaying on map.
        Only use this method if you're managing your own ipyleaflet Map instances,
        and creating more custom visualizations.

        Parameters
        ----------
        name: str
            The name of the layer.
        scales: list of lists, default None
            The scaling to apply to each band in the `Image`.

            If `Image` contains 3 bands, ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

            If `Image` contains 1 band, ``scales`` must be a list like ``[(0, 1)]``,
            or just ``(0, 1)`` for convenience
        colormap: str, default None
            The name of the colormap to apply to the `Image`. Only valid if the `Image` has a single band.

        Returns
        -------
        layer: `.WorkflowsLayer`
        """
        from ... import interactive

        layer = interactive.WorkflowsLayer(self, name=name)
        layer.set_scales(scales, new_colormap=colormap)

        return layer

    def visualize(self, name, scales=None, colormap=None, map=None):
        """
        Add this `Image` to `wf.map <.interactive.map>`, or replace a layer with the same name.

        Parameters
        ----------
        name: str
            The name of the layer.

            If a layer with this name already exists on `wf.map <.interactive.map>`,
            it will be replaced with this `Image`, scales, and colormap.
            This allows you to re-run cells in Jupyter calling `visualize`
            without adding duplicate layers to the map.
        scales: list of lists, default None
            The scaling to apply to each band in the `Image`.

            If `Image` contains 3 bands, ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

            If `Image` contains 1 band, ``scales`` must be a list like ``[(0, 1)]``,
            or just ``(0, 1)`` for convenience

            If None, each 256x256 tile will be scaled independently
            based on the min and max values of its data.
        colormap: str, default None
            The name of the colormap to apply to the `Image`. Only valid if the `Image` has a single band.
        map: `.Map` or `.MapApp`, optional, default None
            The `.Map` (or plain ipyleaflet Map) instance on which to show the `Image`.
            If None (default), uses `wf.map <.interactive.map>`, the singleton Workflows `.MapApp` object.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> nir, red = col.unpack_bands(["nir", "red"])
        >>> ndvi = wf.normalized_difference(nir, red)
        >>> max_ndvi = ndvi.max()
        >>> max_ndvi.visualize(
        ...     name="My Cool Max NDVI",
        ...     scales=[(-1, 1)],
        ...     colormap="viridis",
        ... )  # doctest: +SKIP
        >>> wf.map  # doctest: +SKIP
        >>> # `wf.map` actually displays the map; right click and open in new view in JupyterLab
        """
        from ... import interactive

        if map is None:
            map = interactive.map

        for layer in map.layers:
            if layer.name == name:
                with layer.hold_trait_notifications():
                    layer.image = self
                    layer.set_scales(scales, new_colormap=colormap)
                break
        else:
            layer = self.tile_layer(name=name, scales=scales, colormap=colormap)
            map.add_layer(layer)

    def tile_url(self, name=None, scales=None, colormap=None):
        """
        Generate a new tile server URL for this `Image`.

        Publishes ``self`` as a `Workflow`.

        Parameters
        ----------
        name: str, default None
            The name of the published workflow that will encapsulate this image.
        scales: list of lists, default None
            The scaling to apply to each band in the image.
        colormap: str, default None
            The colormap to apply to the image.

        Returns
        -------
        str
            The tile server URL.
        """
        xyz = XYZ.build(self, name=name)
        xyz.save()

        tile_server_base_url = "https://workflows.descarteslabs.com/{}".format(
            _channel.__channel__
        )
        url = "{}/xyz/{}/{{z}}/{{x}}/{{y}}.png?session_id={}".format(
            tile_server_base_url, xyz.id, uuid.uuid4().hex[:6]
        )

        if scales:
            url += "&scales={}".format(json.dumps(scales))
        if colormap:
            url += "&colormap={}".format(colormap)

        return url


def _result_type(other):
    ImageCollection = _DelayedImageCollection()
    return ImageCollection if isinstance(other, ImageCollection) else Image
