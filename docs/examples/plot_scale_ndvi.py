"""
==================================================
Scale NDVI
==================================================

This example shows how to request MODIS NDVI data
and scale the returned values.

"""

import descarteslabs as dl
import numpy as np
import matplotlib.pyplot as plt


metadata_client = dl.Metadata()
raster_client = dl.Raster()

# A bounding box geometry
aoi = {
    "type": "Polygon",
    "coordinates": [
        [
            [
                -105.86975097656249,
                36.94550173495345
            ],
            [
                -104.930419921875,
                36.94550173495345
            ],
            [
                -104.930419921875,
                37.70120736474139
            ],
            [
                -105.86975097656249,
                37.70120736474139
            ],
            [
                -105.86975097656249,
                36.94550173495345
            ]
        ]
    ]
}


# Request Modis imagery, which contains indicies that need to be scaled
fc = metadata_client.search(products="modis:09:CREFL", geom=aoi, start_time="2017-05-01",
                            end_time="2018-05-15", limit=1)

# Fetch the band information using the Metadata API, including the NDVI ranges
band_info = metadata_client.get_bands_by_product("modis:09:CREFL")
physical_range = band_info['derived:ndvi']['physical_range']
valid_range = band_info['derived:ndvi']['data_range']


# Isolate the image IDs to pull data for
feat_ids = [feat['id'] for feat in fc['features']]


# Request the NDVI band and scale it accordingly and the alpha band for masking next
arr, meta = raster_client.ndarray(
    feat_ids,
    cutline=aoi,
    bands=['ndvi', 'alpha'],
    scales=[[valid_range[0], valid_range[1], physical_range[0], physical_range[1]], None],
    data_type='Float32',
    resolution=120)

# mask out nodata pixels
nodata = arr[:, :, -1] == 0
masked = np.where(nodata, 0, arr[:, :, 0])

plt.hist(masked)
