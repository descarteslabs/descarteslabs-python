"""
==============
Scenes Example
==============

This example makes a simple request to our raster service
and displays the image.

"""

from pprint import pprint

from descarteslabs.scenes import Scene, display

scene, ctx = Scene.from_id(
    "usgs:landsat:oli-tirs:c2:l1:v0:LC08_L1TP_033034_20170313_20200904_02_T1"
)

img, meta = scene.ndarray(
    bands=["swir1", "swir2", "nir"],
    ctx=ctx.assign(resolution=120),
    data_type="Float64",
    raster_info=True,
)

#################################################
# Visualize the image.
display(img, size=2)

#################################################
# We can also view the metadata returned by raster.
pprint(meta)
