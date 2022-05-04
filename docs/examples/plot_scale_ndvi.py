"""
==================================================
Scale NDVI
==================================================

This example shows how to request MODIS NDVI data
and scale the returned values.

"""

import descarteslabs.scenes
from descarteslabs.client.services.raster import Raster
import numpy as np
import matplotlib.pyplot as plt


raster_client = Raster()

# A bounding box geometry
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

# fetch scene from the MODIS product
scenes, ctx = descarteslabs.scenes.search(
    aoi,
    products=["modis:09:v2"],
    start_datetime="2017-05-01",
    end_datetime="2017-05-15",
    limit=1,
)
low_res = ctx.assign(resolution=480)
# speed up by requesting a lower resolution ndarray
masked_array = scenes[0].ndarray("ndvi alpha", low_res, bands_axis=2)

masked = np.where(masked_array.mask[:, :, -1], 0, masked_array.data[:, :, 0])

# plot the results on a histogram
plt.hist(masked)
