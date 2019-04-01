"""
==================================================
Upload ndarray to new product
==================================================

This example demonstrates how to create a product
in our Catalog and upload an example scene.
"""
import descarteslabs as dl
import numpy as np

catalog = dl.Catalog()

################################################
# Create a product entry in our Catalog
product = catalog.add_product(
    "Paris_final_3",
    title="Simple Image Upload_final_3",
    description="An example of creating a product, adding the visible band range, and ingesting a single scene.",
)

# Maintain the product id to upload scenes
product_id = product["data"]["id"]

################################################
# Add band information to the product
# This is a necessary step, and requires the user
# to know a bit about the data to be ingested
bands = ["red", "green", "blue"]

for val, band in enumerate(bands):
    catalog.add_band(
        product_id,  # product this band will belong to
        name=band,  # name of the band
        srcband=val + 1,  # 1 based index for storage and retrieval
        nbits=14,  # the number of bits used to store this band
        dtype="UInt16",  # data type for storage
        nodata=0,  # pixel value indicating no data available
        data_range=[0, 10000],  # list of the min and max data values
        type="spectral",  # spectral, derived, mask, or class
        default_range=(0, 4000),
    )  # a good default scale for display

################################################
# Search for Sentinel-2 imagery over an AOI
# Define a bounding box around Paris, France
paris = {
    "type": "Polygon",
    "coordinates": [
        [
            [2.165946315534452, 48.713171120067045],
            [2.5359015712706023, 48.713171120067045],
            [2.5359015712706023, 48.957687975409726],
            [2.165946315534452, 48.957687975409726],
            [2.165946315534452, 48.713171120067045],
        ]
    ],
}

scenes, geoctx = dl.scenes.search(
    paris,
    products=["sentinel-2:L1C"],
    start_datetime="2018-06-24",
    end_datetime="2018-06-30",
    limit=2,
    cloud_fraction=0.1,
)

ndarry_mosaic, raster_info = scenes.mosaic("red green blue", geoctx, raster_info=True)

################################################
# Upload the ndarray as a single scene in our new product
# Note: It can take up to ten minutes for the scene to
# appear in the Catalog and Viewer interfaces

# re-shape the array as (x, y, band)
ndarray_reshape = np.transpose(ndarry_mosaic, (1, 2, 0))

catalog.upload_ndarray(
    ndarray_reshape, product_id=product_id, image_id="Paris", raster_meta=raster_info
)
