from ..core import typecheck_promote
from ..geospatial import Image
from ..geospatial import ImageCollection
from ..geospatial import Kernel


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
