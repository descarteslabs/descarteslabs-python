"""
==================================================
Upload ndarray to new product
==================================================

This example demonstrates how to create a product
in our Catalog and upload an example scene.
"""
from descarteslabs.catalog import Product, SpectralBand, Image
import descarteslabs.scenes

################################################
# Create a product entry in our Catalog
product = Product(
    id="Paris_final_3",
    name="Simple Image Upload_final_3",
    description="An example of creating a product, adding the visible band range, and ingesting a single scene.",
)
product.save()

################################################
# Add band information to the product
# This is a necessary step, and requires the user
# to know a bit about the data to be ingested
bands = ["red", "green", "blue"]

for band_index, band in enumerate(bands):
    SpectralBand(
        product=product,  # product this band will belong to
        name=band,  # name of the band
        band_index=band_index,  # 0 based index for storage and retrieval
        data_type="UInt16",  # data type for storage
        nodata=0,  # pixel value indicating no data available
        data_range=[0, 10000],  # list of the min and max data values
        display_range=(0, 4000),  # a good default scale for display
    ).save()

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

scenes, geoctx = descarteslabs.scenes.search(
    paris,
    products=["sentinel-2:L1C"],
    start_datetime="2018-06-24",
    end_datetime="2018-06-30",
    limit=2,
    cloud_fraction=0.1,
)

ndarray_mosaic, raster_info = scenes.mosaic("red green blue", geoctx, raster_info=True)

################################################
# Upload the ndarray as a single scene in our new product
# Note: It can take up to ten minutes for the scene to
# appear in the Catalog and Viewer interfaces

image = Image(
    name="Paris", product=product, acquired="2018-06-24", acquired_end="2018-06-30"
)

image.upload_ndarray(ndarray_mosaic, raster_meta=raster_info)
