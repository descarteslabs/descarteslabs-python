# © 2025 EarthDaily Analytics Corp.

"""
===============================
Composite Multi-Product Imagery
===============================

Composite imagery from two data sources and
display as a single image.

"""

from descarteslabs.catalog import Image, properties as p
from descarteslabs.utils import display
import numpy as np

# Define a bounding box around Taos in a GeoJSON

taos = {
    "type": "Polygon",
    "coordinates": [
        [
            [-105.71868896484375, 36.33725319397006],
            [-105.2105712890625, 36.33725319397006],
            [-105.2105712890625, 36.73668306473141],
            [-105.71868896484375, 36.73668306473141],
            [-105.71868896484375, 36.33725319397006],
        ]
    ],
}

# Create an ImageCollection
search = (
    Image.search()
    .intersects(taos)
    .filter(
        p.product_id.any_of(["usgs:landsat:oli-tirs:c2:l2:v0", "esa:sentinel-2:l2a:v1"])
    )
    .filter("2018-05-01" <= p.acquired < "2018-06-01")
    .filter(p.cloud_fraction < 0.2)
    .sort("acquired")
    .limit(15)
)
images = search.collect()

#####################################################
# See which images we have, and how many per product:

print(images)

#########################################
# And if you're curious, which image IDs:

print(images.each.id)

#######################################
# Make a median composite of the images.

# Request a stack of all the images using the same GeoContext with lower resolution
arr_stack = images.stack("red green blue", resolution=60, data_type="Float64")

# Composite the images based on the median pixel value
composite = np.ma.median(arr_stack, axis=0)
display(composite, title="Taos Composite", size=2)
