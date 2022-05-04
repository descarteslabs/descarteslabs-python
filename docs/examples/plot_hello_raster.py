"""
==================================================
Raster Example
==================================================

This example makes a simple request to our raster service
and displays the image.

"""

from pprint import pprint

from descarteslabs.client.services.raster import Raster
import matplotlib.pyplot as plt


raster_client = Raster()

aoi = {
    "type": "Polygon",
    "coordinates": [
        [
            [-105.86975097656249, 36.94550173495345],
            [-104.930419921875, 36.94550173495345],
            [-104.930419921875, 37.70120736474139],
            [-105.86975097656249, 37.70120736474139],
            [-105.86975097656249, 36.94550173495345],
        ]
    ],
}

img, meta = raster_client.ndarray(
    "landsat:LC08:PRE:TOAR:meta_LC80330342017072_v1",
    bands=["swir1", "swir2", "nir", "alpha"],
    scales=[[0, 4000], [0, 4000], [0, 4000], None],
    data_type="Byte",
    cutline=aoi,
    resolution=120,
)

# visualize the image
plt.figure(figsize=[8, 8])
plt.axis("off")
plt.imshow(img)
plt.show()

##############################
# We can also view the metadata returned by raster.
pprint(meta)
