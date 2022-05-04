"""
===============================
Mosaicking imagery with Raster
===============================

Most often, our area of interest (AOI) does not conform the arbitrary boundaries
of an image as collected by the satellite. The Raster API enables us to
retrieve imagery mosaicked across our AOI. This example illustrates how Raster
mosaics imagery, and how we can format our call to Raster to group images
by acquisition date (and/or any other metadata property).


"""
from descarteslabs.client.services.raster import Raster
from descarteslabs.catalog import Image, properties as p
import matplotlib.pyplot as plt

print(__doc__)

raster_client = Raster()

# Define my area of interest
tile = raster_client.dltile_from_latlon(
    lat=38.8664364, lon=-107.238606300, resolution=20.0, tilesize=1024, pad=0
)

# Search for Sentinel-2 imagery collected between
# August 13 - August 21, 2017 over the AOI
image_metadata = list(
    Image.search()
    .filter(p.product_id == "sentinel-2:L1C")
    .filter("2017-08-13" < p.acquired < "2017-08-22")
    .intersects(tile)
)


################################################
# Let's first visualize each of these image acquisitions separately.

# Retrieve each image from Raster separately
# and store the ndarrays in a list

images = list()
for image in image_metadata:
    arr, meta = raster_client.ndarray(
        image.id,
        dltile=tile,
        bands=["nir", "red", "green", "alpha"],
        scales=[[0, 6000, 0, 255], [0, 4000, 0, 255], [0, 4000, 0, 255]],
        data_type="Byte",
    )
    images.append(arr)

# Plot using Matplotlib
fig = plt.figure(figsize=[6, 6])
for i, image in enumerate(images):
    # get acquisition date of the image
    date = image_metadata[i].acquired.date()

    # plot the image
    ax = fig.add_subplot(4, 4, i + 1)
    ax.imshow(image)
    ax.set_title(date, fontsize=8)
    plt.axis("off")

plt.tight_layout()

################################################

# We can see that our area of interest straddles multiple
# Sentinel-2 granules, which is why we see only partial coverage of our AOI
# in each image. From the acquisition dates, we can see that
# these fours images were actually acquired on only one of two dates, August 13
# and August 20, 2017. Instead of obtaining each image individually, we
# may instead want to group these by their acquisition date, and mosaic
# the images acquired on the same date.


################################################

# get list of all unique imagery dates
dates = list(set([image.acquired.date() for image in image_metadata]))

# mosaic the images acquired on each date
images = list()
for date in dates:
    # get ids
    ids = [image.id for image in image_metadata if image.acquired.date() == date]
    # retrieve mosaicked data
    arr, meta = raster_client.ndarray(
        ids,
        dltile=tile,
        bands=["nir", "red", "green", "alpha"],
        scales=[[0, 6000, 0, 255], [0, 4000, 0, 255], [0, 4000, 0, 255]],
        data_type="Byte",
    )
    images.append(arr)

# plot the mosaics
fig = plt.figure(figsize=[10, 5])
for i, image in enumerate(images):
    date = image_metadata[i].acquired.date()
    ax = fig.add_subplot(1, 2, i + 1)
    ax.imshow(image)
    ax.set_title(dates[i], fontsize=8)
    plt.axis("off")

plt.tight_layout()
################################################

################################################

# Raster will mosaic the imagery in the order in which
# they appear in the list of image IDs. Thus, if we instead
# passed all four image IDs to raster in the order in which
# they were returned by Metadata, it will return
# the latest image.

all_ids = [image.id for image in image_metadata]
arr, meta = raster_client.ndarray(
    all_ids,
    dltile=tile,
    bands=["nir", "red", "green", "alpha"],
    scales=[[0, 6000, 0, 255], [0, 4000, 0, 255], [0, 4000, 0, 255]],
    data_type="Byte",
)
# plot the image
plt.figure(figsize=(6, 6))
plt.imshow(arr)
plt.axis("off")

################################################

# Now, let's reverse the order of the list of image
# IDs, and pass that reversed list to Raster

all_ids_reversed = sorted(all_ids, reverse=True)
arr, meta = raster_client.ndarray(
    all_ids_reversed,
    dltile=tile,
    bands=["nir", "red", "green", "alpha"],
    scales=[[0, 6000, 0, 255], [0, 4000, 0, 255], [0, 4000, 0, 255]],
    data_type="Byte",
)
# plot the image
plt.figure(figsize=(6, 6))
plt.imshow(arr)
plt.axis("off")

################################################
