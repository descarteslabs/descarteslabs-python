"""
=====================
Save image to GeoTIFF
=====================

This example demonstrates how to save an image
to your local machine in GeoTiff format.
"""

import os
from descarteslabs.catalog import Product, properties as p

#################################################
# Create an aoi feature to clip imagery.
box = {
    "type": "Polygon",
    "coordinates": [
        [
            [-108.64292971398066, 33.58051349561343],
            [-108.27082685426221, 33.58051349561343],
            [-108.27082685426221, 33.83925599538719],
            [-108.64292971398066, 33.83925599538719],
            [-108.64292971398066, 33.58051349561343],
        ]
    ],
}

#################################################
# Find the images.
search = (
    Product.get("usgs:landsat:oli-tirs:c2:l2:v0")
    .images()
    .intersects(box)
    .filter("2018-06-02" <= p.acquired < "2018-06-03")
    .sort("acquired")
)
images = search.collect()

#################################################
# Mosaic and download.
files = images.download_mosaic(
    bands=["red", "green", "blue", "alpha"],
    resolution=60,
    dest="save-local.tif",
    data_type="Float64",
)

print(files)
