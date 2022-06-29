"""
==================================================
Save image to GeoTIFF
==================================================

This example demonstrates how to save an image
to your local machine in GeoTiff format.
"""

from descarteslabs.scenes import search

# Create an aoi feature to clip imagery to
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

# find the scenes
scenes, ctx = search(
    aoi=box,
    products=["usgs:landsat:oli-tirs:c2:l1:v0"],
    start_datetime="2018-06-02",
    end_datetime="2018-06-03",
)

# mosaic and download
scenes.download_mosaic(
    bands=["red", "green", "blue", "alpha"],
    ctx=ctx.assign(resolution=60),
    dest="save-local.tif",
    data_type="Float64",
)
