"""
==================================================
Simple Image Visualization
==================================================

Visualize a true color Landsat 8 Scene.

"""

import descarteslabs.scenes

# Create a tile around Pisa, Italy
tile = descarteslabs.scenes.DLTile.from_latlon(
    43.7230, 10.3966, resolution=20.0, tilesize=1024, pad=0
)

# Use the Scenes API to search for imagery availble over the area of interest
scenes, ctx = descarteslabs.scenes.search(
    tile,
    products=["landsat:LC08:01:RT:TOAR"],
    start_datetime="2018-04-01",
    end_datetime="2018-05-01",
    limit=2,
    cloud_fraction=0.1,
)

# Pick just one scene
scene = scenes[0]

# Load the data as an ndarray
arr = scene.ndarray("red green blue", ctx)

# Display the scene
descarteslabs.scenes.display(arr, size=16, title=scene.properties.id)
