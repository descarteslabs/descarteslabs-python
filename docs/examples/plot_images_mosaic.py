"""
===================================
Compositing Imagery with Catalog V2
===================================

Most often, our area of interest (AOI) does not conform to the arbitrary boundaries
of an image as collected by the satellite. The Catalog V2 API enables us to
retrieve imagery mosaicked across our AOI. This example illustrates how Catalog
mosaics imagery, and how we can format our call to Catalog to group images
by acquisition date (and/or any other metadata property).


"""
from descarteslabs.catalog import Product, properties as p
from descarteslabs.geo import DLTile
from descarteslabs.utils import display

# Define my area of interest
tile = DLTile.from_latlon(
    lat=38.8664364, lon=-107.238606300, resolution=20.0, tilesize=1024, pad=0
)

# Search for Sentinel-2 imagery collected between
# August 13 - August 21, 2017 over the AOI
search = (
    Product.get("sentinel-2:L1C")
    .images()
    .intersects(tile)
    .filter("2017-08-13" <= p.acquired < "2017-08-22")
    .sort("acquired")
)
images = search.collect()

print(images)


################################################
# Let's first visualize each of these image acquisitions separately.

# Retrieve each image separately using stack
rasters = images.stack(
    "nir red green",
    scaling=[[0, 6000, 0, 255], [0, 4000, 0, 255], [0, 4000, 0, 255]],
    data_type="Byte",
)

# Plot
dates = [image.acquired.date().isoformat() for image in images]
display(*rasters, title=dates, size=2)

################################################
# We can see that our area of interest straddles multiple
# Sentinel-2 granules, which is why we see only partial coverage of our AOI
# in each image. From the acquisition dates, we can see that
# these fours images were actually acquired on only one of two dates, August 13
# and August 20, 2017. Instead of obtaining each image individually, we
# may instead want to group these by their acquisition date, and mosaic
# the images acquired on the same date.

flatten = ["acquired.year", "acquired.month", "acquired.day"]

rasters = images.stack(
    "nir red green",
    scaling=[[0, 6000, 0, 255], [0, 4000, 0, 255], [0, 4000, 0, 255]],
    data_type="Byte",
    flatten=flatten,
)

# plot the mosaics
dates = [ic[0].acquired.date().isoformat() for _, ic in images.groupby(*flatten)]
display(*rasters, title=dates, size=2)

################################################
# ImageCollection will mosaic the imagery in the order in which
# they appear in the list. By default this will be ordered
# by acquisition date, and the mosaic will return
# the latest image on top.

arr = images.mosaic(
    "nir red green",
    scaling=[[0, 6000, 0, 255], [0, 4000, 0, 255], [0, 4000, 0, 255]],
    data_type="Byte",
)

# plot the mosaic
display(arr, title="latest", size=2)

################################################
# Now, let's reverse the order of the collection,
# and mosaic will return the earliest image on top.

arr = images.sorted("acquired", reverse=True).mosaic(
    "nir red green",
    scaling=[[0, 6000, 0, 255], [0, 4000, 0, 255], [0, 4000, 0, 255]],
    data_type="Byte",
)

# plot the mosaic
display(arr, title="earliest", size=2)
