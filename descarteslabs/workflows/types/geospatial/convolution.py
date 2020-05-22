from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Int, Float
from ..containers import List, Struct, Tuple
from .image import Image
from .imagecollection import ImageCollection


KernelBase = Struct[{"dims": Tuple[Int, Int], "data": List[Float]}]


@serializable(is_named_concrete_type=True)
class Kernel(KernelBase):
    """
    A Kernel is a proxy object holding the kernel when performing a 2-dimensional
    convolution.

    Examples
    --------
    >>> from descarteslabs.workflows import Kernel
    >>> kernel = Kernel(dims=(5,5), data=[1.0, 1.0, 1.0, 1.0, 1.0,
    ...                                   1.0, 2.0, 3.0, 2.0, 1.0,
    ...                                   1.0, 3.0, 4.0, 3.0, 1.0,
    ...                                   1.0, 2.0, 3.0, 2.0, 1.0,
    ...                                   1.0, 1.0, 1.0, 1.0, 1.0])
    >>> kernel
    <descarteslabs.workflows.types.geospatial.convolution.Kernel object at 0x...>
    """

    _doc = {
        "dims": "Tuple containing the dimensions of the kernel",
        "data": "List containing the kernel data in row-major format",
    }
    _constructor = "wf.Kernel.load"

    def __init__(self, dims, data):
        return super(Kernel, self).__init__(dims=dims, data=data)


@typecheck_promote((Image, ImageCollection), Kernel)
def conv2d(obj, filt):
    """
    2-D spatial convolution of an `Image` or `ImageCollection`.

    Example
    -------
    >>> from descarteslabs.workflows import Image, Kernel, conv2d
    >>> img = Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
    >>> rgb = img.pick_bands("red green blue")
    >>> kernel = Kernel(dims=(3,3), data=[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0])
    >>> conv2d(rgb, kernel).compute(geoctx) # geoctx is an arbitrary geocontext for 'rgb' # doctest: +SKIP
    ImageResult:
      * ndarray: MaskedArray<shape=(3, 512, 512), dtype=float64>
      * properties: 'acquired', 'area', 'bits_per_pixel', 'bright_fraction', ...
      * bandinfo: 'red', 'green', 'blue'
      * geocontext: 'geometry', 'key', 'resolution', 'tilesize', ...
    """
    return obj._from_apply("wf.conv2d", obj, filt)
