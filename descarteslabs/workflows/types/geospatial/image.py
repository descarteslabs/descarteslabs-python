import json

from descarteslabs import scenes

from ... import env
from ... import _channel

from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Any, Str, Bool, Float, Int
from ..containers import Tuple, Dict, Struct, KnownDict
from ..datetimes import Datetime

from .geometry import Geometry
from .feature import Feature
from .featurecollection import FeatureCollection
from .mixins import BandsMixin
from ...interactive import Map, LayerControl, TileLayer


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
    def from_id(cls, image_id):
        "Create a proxy `Image` from an ID in the Descartes Labs catalog"
        return cls._from_apply(
            "Image.load", image_id, geocontext=env.geoctx, token=env._token
        )

    @classmethod
    def from_scene(cls, scene):
        "Create a proxy image from a `~descarteslabs.scenes.scene.Scene` object"
        return cls.from_id(scene.properties["id"])

    @typecheck_promote(lambda: Image)
    def concat(self, other_image):
        """
        New `Image`, with the bands in ``other_image`` appended to this one.

        If band names overlap, the band from the *other* `Image` will be suffixed with "_1".
        """
        return self._from_apply("Image.concat", self, other_image)

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

    def visualize(self, name=None, scales=None, location=None, zoom_start=5, m=None):
        """
        Visualize this image on a slippy map widget, within a Jupyter Notebook cell.
        The map will be centered at the provided ``location`` and set at the provided ``zoom``
        level, at which point you will be able to pan and zoom around the map.

        Parameters
        ----------
        name: str, default None
            The name of the published workflow that will encapsulate this image.
        scales: list of lists, default None
            The scaling to apply to each band in the image.
        location: list of ints or floats, default None
            Latitude/longitude of where the map should initially be centered.
        zoom_start: int, default 5
            The initial zoom level for the map.
        m: Map, default None
            The `~descarteslabs.workflows.interactive.Map` to which we'll add a tile layer
            containing the outputs of the image workflow.

        Returns
        -------
        Map
            The `~descarteslabs.workflows.interactive.Map` instance containing a tile layer for
            this image workflow, that will be rendered by the Jupyter Notebook.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("landsat:LC08:01:RT:TOAR")
        >>> nir, red = col.unpack_bands(["nir", "red"])
        >>> ndvi = wf.normalized_difference(nir, red)
        >>> max_ndvi = ndvi.max()
        >>> max_ndvi.visualize(
        ...     name="My Cool Max NDVI",
        ...     location=[-105.93780, 35.6870],
        ...     zoom_start=14
        ... )  # doctest: +SKIP
        """
        if location is None:
            # Defaults to Santa Fe, New Mexico.
            location = [-105.93780, 35.6870]

        if m is None:
            m = Map(tiles="Stamen Terrain", zoom_start=zoom_start, location=location)

        if name is None:
            name = "    "

        TileLayer(
            self.tile_url(name, scales=scales),
            name=name,
            attr="Descartes Labs",
            active=True,
            overlay=True,
        ).add_to(m)

        # FIXME: This should only be added if no map provided but it did not show up if layers added afterwards
        LayerControl().add_to(m)

        return m

    def tile_url(self, name=None, scales=None):
        """
        Get the tile server URL for this image workflow.

        Parameters
        ----------
        name: str, default None
            The name of the published workflow that will encapsulate this image.
        scales: list of lists, default None
            The scaling to apply to each band in the image.

        Returns
        -------
        str
            The tile server URL.
        """
        # TODO: separate publish and tiles?
        workflow = self.publish(name="Tiles For {}".format(name))
        tile_server_base_url = "https://workflows.descarteslabs.com/{}".format(
            _channel.__channel__
        )
        url = "{}/workflows/{}/xyz/{{z}}/{{x}}/{{y}}.png".format(
            tile_server_base_url, workflow.id
        )

        if scales:
            url += "?scales={}".format(json.dumps(scales))

        return url


def _result_type(other):
    ImageCollection = _DelayedImageCollection()
    return ImageCollection if isinstance(other, ImageCollection) else Image
