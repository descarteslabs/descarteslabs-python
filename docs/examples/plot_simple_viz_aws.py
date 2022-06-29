"""
==================================================
Simple Image Visualization
==================================================

Visualize a true color Landsat 8 Scene.

"""

from descarteslabs.scenes import DLTile, search, display

# Create a tile around Pisa, Italy
tile = DLTile.from_latlon(43.7230, 10.3966, resolution=20.0, tilesize=1024, pad=0)

# Use the Scenes API to search for imagery availble over the area of interest
scenes, ctx = search(
    tile,
    products=["usgs:landsat:oli-tirs:c2:l1:v0"],
    start_datetime="2022-04-01",
    end_datetime="2022-05-01",
    limit=2,
    cloud_fraction=0.1,
)

# Pick just one scene
scene = scenes[0]

# Load the data as an ndarray
arr = scene.ndarray("red green blue", ctx, data_type="Float64")

# Display the scene
display(arr, size=2, title=scene.properties.id)
