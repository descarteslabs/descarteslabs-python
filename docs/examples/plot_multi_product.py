"""
==================================================
Composite Multi-Product Imagery
==================================================

Composite imagery from two data sources and
display as a single image.

"""

import descarteslabs as dl
import numpy as np

# Define a bounding box around Taos in a GeoJSON

taos = {
    "type": "Polygon",
    "coordinates": [
        [
            [
                -105.71868896484375,
                36.33725319397006
            ],
            [
                -105.2105712890625,
                36.33725319397006
            ],
            [
                -105.2105712890625,
                36.73668306473141
            ],
            [
                -105.71868896484375,
                36.73668306473141
            ],
            [
                -105.71868896484375,
                36.33725319397006
            ]
        ]
    ]
}

# Create a SceneCollection
scenes, ctx = dl.scenes.search(taos,
                               products=["landsat:LC08:01:RT:TOAR", "sentinel-2:L1C"],
                               start_datetime="2018-05-01",
                               end_datetime="2018-06-01",
                               cloud_fraction=0.2,
                               limit=15)

#####################################################
# See which Scenes we have, and how many per product:

print(scenes)

#########################################
# And if you're curious, which scene IDs:

print(scenes.each.properties.id)

#######################################
# Make a median composite of the scenes

# Make a lower-resolution GeoContext
ctx_lowres = ctx.assign(resolution=60)

# Request a NumPy stack of all the scenes using the same GeoContext
arr_stack = scenes.stack("red green blue", ctx_lowres)

# Composite the scenes based on the median pixel value
composite = np.ma.median(arr_stack, axis=0)
dl.scenes.display(composite, title="Taos Composite")
