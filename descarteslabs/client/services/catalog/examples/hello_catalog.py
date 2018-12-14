"""Simple example of how to create a product, then add
some bands and imagery. We will use the included file `building_mask.tif`
as an example of some imagery you might want to upload with the catalog.
"""

import descarteslabs as dl
import os
from random import randint
from time import sleep


catalog_client = dl.Catalog()
metadata_client = dl.Metadata()
raster_client = dl.Raster()

# First step, create a product, which is a descriptive document you use to
# group related images.

product_id = catalog_client.add_product(
    "building_mask:osm:v0",
    title="OSM Building Mask",
    description="Rasterized OSM building footprints from vector data. Quality varies regionally",
)["data"]["id"]

# Next we need to add bands. The core function of a band is to tell us how data
# is encoded in the imagery that you are going to upload. For these building
# masks there is only one file per scene, and each scene has one 8 bit band.

band_id = catalog_client.add_band(
    product_id=product_id,  # id of the product we just created.
    name="footprint",  # this is a unique name to describe what the band encodes.
    jpx_layer=0,
    srcfile=0,
    srcband=1,  # src band is always a 1-based index (counting starts at 1)
    nbits=8,
    dtype="Byte",
    nodata=0,
    data_range=[0, 2 ** 8 - 1],
    type="mask",
)["data"]["id"]

# Now we want to add some actual imagery to our catalog. We will use the
# `upload_image` method. Processing of the uploaded image is asynchronous, the
# method will not return any information about when the image will be uploaded.
# For now you can use the metadata image searching functions to determine that your
# imagery has been processed.

image_path = os.path.join(os.path.dirname(__file__), "building_mask.tif")

catalog_client.upload_image(image_path, product_id)

# Poll for processed image
processed_image_id = "{}:{}".format(product_id, "building_mask")

image = None
while True:
    try:
        image = metadata_client.get(processed_image_id)
        break
    except Exception:
        sleep(2)

# Lets look at our data
raster_client.raster(
    [processed_image_id], save=True, outfile_basename="./processed_building_mask"
)

# Let's say we want to add a colormap to our data so that it is nicer to look
# at. We have some built in colormaps which you can reference by name, and
# you can also add a custom colormap.

catalog_client.change_band(product_id, band_id, colormap_name="magma")

# Now calls to raster.raster will produce false color images. Note that because
# of internal caching you will need to wait about 1 minute for your band
# changes to take effect in calls to raster..

# If you want to use your own color map you can, but you have to put it into
# a form we can understand. The general form is a list where each item is a map to
# the appropriate colorspace for the pixel value at that items index. Of course this only
# makes sense if the dtype is integral (not Float32).

bad_colormap = [[str(randint(0, 255)) for i in range(4)] for i in range(256)]
catalog_client.change_band(product_id, band_id, colormap=bad_colormap)

# Your custom colormap will take precedence over the named colormap from earlier.


# Maybe you play around with your new product and decide it has a wider appeal.
# Lets share the product with some other folks.

catalog_client.change_product(product_id, read=["some:group"])

# WAIT, by default we have only set permissions for the product, not the bands
# and images that belong to it. To do that we need to take advantage of the
# set_global_permissions flag on the `change_product` method.

catalog_client.change_product(
    product_id, read=["some:group"], set_global_permissions=True
)

# Don't worry if all your imagery isn't available to the new read group immediately.
# The update is handled in the background, so you don't have to wait for a (potentially long)
# time for your request to return, depending how many images need to be changed.


# Finally if you decide you are done with this layer and want to clean it up,
# use the remove_* methods. You aren't allowed to delete a product that has
# imagery or bands attached, because this would orphan those documents, and make
# them inaccessible.

for band in metadata_client.bands(products=product_id):
    catalog_client.remove_band(product_id, band["id"])

# Removing an image has the side effect that it will cleanup the image data
# associated with this metadata.
for _image in metadata_client.search(products=product_id)["features"]:
    catalog_client.remove_image(product_id, _image["id"])

sleep(3)  # need to wait for the database to register the deleted bands/images
# a fix for this is in the works.

catalog_client.remove_product(product_id)
