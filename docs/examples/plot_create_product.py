# © 2025 EarthDaily Analytics Corp.

"""
=============================
Upload ndarray to new product
=============================

This example demonstrates how to create a product
in our Catalog and upload an example image.
"""

from descarteslabs.catalog import Product, SpectralBand, Image, properties as p
import uuid

################################################
# Create a unique product id (to avoid collisions).
product_id = uuid.uuid4().hex

################################################
# Create a product entry in our Catalog.
product = Product(
    id=product_id,
    name="Simple Image Upload",
    description="An example of creating a product, adding the visible band range, and ingesting a single scene.",
)
product.save()

################################################
# Add band information to the product.
# This is a necessary step, and requires the user
# to know a bit about the data to be ingested.
bands = ["red", "green", "blue"]

for band_index, band in enumerate(bands):
    SpectralBand(
        product=product,  # product this band will belong to
        name=band,  # name of the band
        band_index=band_index,  # 0 based index for storage and retrieval
        data_type="Float64",  # data type for storage
        nodata=0,  # pixel value indicating no data available
        data_range=(0.0, 1.0),  # list of the min and max data values
        display_range=(0.0, 0.4),  # a good default scale for display
    ).save()

################################################
# As an aside, we can add a writer to this product.
# The product that we just created doesn't have any writers.
print("Product writers: {}".format(product.writers))

################################################
# However, we can add a writer to this product.
product.writers = ["email:someuser@gmail.com"]
product.save()

################################################
# Now, ``'email:someuser@gmail.com'`` is a writer for this product.
# This user can now change the product metadata,
# add bands, and add imagery to this product.
print("Changed product writers: {}".format(product.writers))

################################################
# Search for Sentinel-2 imagery over an AOI.
# Define a bounding box around Paris, France.
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

search = (
    Product.get("esa:sentinel-2:l2a:v1")
    .images()
    .intersects(paris)
    .filter("2020-06-24" < p.acquired < "2020-06-30")
    .filter(p.cloud_fraction < 0.1)
    .limit(2)
)
images = search.collect()

################################################
# Mosaic the image collection to a single RGB image.
ndarray_mosaic, raster_info = images.mosaic("red green blue", raster_info=True)

################################################
# Upload the ndarray as a single scene in our new product.
# Note: It can take several minutes for the image to
# appear in various interfaces.
image = Image(
    name="Paris", product=product, acquired="2020-06-24", acquired_end="2020-06-30"
)

upload = image.upload_ndarray(ndarray_mosaic, raster_meta=raster_info)
upload.wait_for_completion()
print(upload.status)

################################################
# Now the image exists and can be found by search.
print(product.images().collect())

################################################
# Delete our product; we don't need it anymore.
task = product.delete_related_objects()
while task is not None:
    task.wait_for_completion()
    if task.status == "success":
        break
    task = product.delete_related_objects()
product.delete()
print("Product removed.")
