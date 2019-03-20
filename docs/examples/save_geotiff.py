"""
==================================================
Save image to GeoTIFF
==================================================

This example demonstrates how to save an image
to your local machine in GeoTiff format.
"""

import descarteslabs as dl

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

# Two predefined image IDs for mosaic and download. These can be obtained through a Metadata or Scenes API search
images = [
    "landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_035037_20180602_20180602_01_RT_v1",
    "landsat:LC08:01:RT:TOAR:meta_LC08_L1TP_035036_20180602_20180602_01_RT_v1",
]

# The Raster API call to download an image mosaic. Other parameters are available
# The file is written in to the same directory as the script.

raster_client = dl.Raster()
raster_client.raster(
    inputs=images,
    bands=["red", "green", "blue", "alpha"],
    scales=[[0, 5500], [0, 5500], [0, 5500], None],
    data_type="Byte",
    cutline=box,
    save=True,
    outfile_basename="save_local",
    resolution=60,
)
