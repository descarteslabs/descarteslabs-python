"""
===============================
Compositing Imagery with Scenes
===============================

Most often, our area of interest (AOI) does not conform the arbitrary boundaries
of an image as collected by the satellite. The Scenes API enables us to
retrieve imagery mosaicked across our AOI. This example illustrates how Scenes
mosaics imagery, and how we can format our call to Scenes to group images
by acquisition date (and/or any other metadata property).


"""
from descarteslabs.scenes import DLTile, SceneCollection, search, display

print(__doc__)

# Define my area of interest
tile = DLTile.from_latlon(
    lat=38.8664364, lon=-107.238606300, resolution=20.0, tilesize=1024, pad=0
)

# Search for Sentinel-2 imagery collected between
# August 13 - August 21, 2017 over the AOI
scenes, ctx = search(
    aoi=tile,
    products=["esa:sentinel-2:l1c:v1"],
    start_datetime="2017-08-13",
    end_datetime="2017-08-22",
)
print(scenes)

################################################
# Let's first visualize each of these image acquisitions separately.

# Retrieve each image separately
# and store the ndarrays in a list

images = list()
for scene in scenes:
    arr = scene.ndarray(
        bands=["nir", "red", "green"],
        ctx=ctx,
        data_type="Float64",
    )
    images.append(arr)

# Plot
dates = [scene.properties.date.date().isoformat() for scene in scenes]
display(*images, title=dates, size=2)

################################################

# We can see that our area of interest straddles multiple
# Sentinel-2 granules, which is why we see only partial coverage of our AOI
# in each image. From the acquisition dates, we can see that
# these fours images were actually acquired on only one of two dates, August 13
# and August 20, 2017. Instead of obtaining each image individually, we
# may instead want to group these by their acquisition date, and mosaic
# the images acquired on the same date.

################################################

flatten = ["properties.date.year", "properties.date.month", "properties.date.day"]

images = scenes.stack(
    bands=["nir", "red", "green"],
    ctx=ctx,
    data_type="Float64",
    flatten=flatten,
)

# plot the mosaics
dates = [sc[0].properties.date.date().isoformat() for _, sc in scenes.groupby(*flatten)]
mosaics = [images[i, :, :, :] for i in range(images.shape[0])]
display(*mosaics, title=dates, size=2)

################################################

################################################

# Scenes will mosaic the imagery in the order in which
# they appear in the list. By default this will be ordered
# by acquisition date, and the mosaic will return
# the latest image on top.

arr = scenes.mosaic(
    bands=["nir", "red", "green"],
    ctx=ctx,
    data_type="Float64",
)

# plot the mosaic
display(arr, title="latest", size=2)

################################################

# Now, let's reverse the order of the collection,
# and mosaic will return the earliest image on top.

scenes2 = SceneCollection(reversed(scenes))
arr = scenes2.mosaic(
    bands=["nir", "red", "green"],
    ctx=ctx,
    data_type="Float64",
)

# plot the mosaic
display(arr, title="earliest", size=2)

################################################
