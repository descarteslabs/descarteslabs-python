"""
==================================================
Scenes Example
==================================================

This example makes a simple request to our raster service
and displays the image.

"""

from pprint import pprint

from descarteslabs.scenes import Scene, display

scene, ctx = Scene.from_id("landsat:LC08:PRE:TOAR:meta_LC80330342017072_v1")

img, meta = scene.ndarray(
    bands=["swir1", "swir2", "nir"],
    ctx=ctx.assign(resolution=120),
    raster_info=True,
)

# visualize the image
display(img, size=2)

##############################
# We can also view the metadata returned by raster.
pprint(meta)
