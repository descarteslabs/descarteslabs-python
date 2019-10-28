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
    """

    _doc = {
        "dims": "Tuple containing the dimensions of the kernel",
        "data": "List containing the kernel data in row-major format",
    }
    _constructor = "Kernel.load"


@typecheck_promote((Image, ImageCollection), Kernel)
def conv2d(obj, filt):
    """
    2-D spatial convolution of `Image` or `ImageCollection`.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> img = wf.Image.from_id("landsat:LC08:PRE:TOAR:meta_LC80270312016188_v1")
    >>> rgb = img.pick_bands(["red", "green", "blue"])
    >>> w = wf.Kernel(dims=(3,3), data=[1.0,2.0,3.0,4.0,5.0,6.0,7.0,8.0,9.0])
    >>> t = wf.conv2d(rgb, w)
    """
    return obj._from_apply("conv2d", obj, filt)
