"""
============================
Create time stacks of images
============================

This example demonstrates how to aggregate the images returned from a Catalog V2 image search by date.
"""
from descarteslabs.catalog import Product, properties as p
from descarteslabs.utils import display

# Define a bounding box around Taos in a GeoJSON
taos = {
    "type": "Polygon",
    "coordinates": [
        [
            [-105.71868896484375, 36.33725319397006],
            [-105.2105712890625, 36.33725319397006],
            [-105.2105712890625, 36.73668306473141],
            [-105.71868896484375, 36.73668306473141],
            [-105.71868896484375, 36.33725319397006],
        ]
    ],
}

################################################
# Create an ImageCollection.
search = (
    Product.get("landsat:LC08:01:RT:TOAR").images()
    .intersects(taos)
    .filter("2018-01-01" <= p.acquired < "2018-12-31")
    .filter(p.cloud_fraction < 0.7)
    .sort("acquired")
    .limit(500)
)
images = search.collect()
print("There are {} images in the collection.".format(len(images)))

################################################
# To create subcollections using the ImageCollection API, we have
# the built in methods
# :meth:`ImageCollection.groupby <descarteslabs.catalog.ImageCollection.groupby>`
# and :meth:`ImageCollection.filter <descarteslabs.catalog.ImageCollection.filter>`.
#
# If we want to create multiple subsets based on those properties, we can use the
# :meth:`ImageCollection.groupby <descarteslabs.catalog.ImageCollection.groupby>` method.
for (year, month), month_images in images.groupby(
    "acquired.year", "acquired.month"
):
    print("{}: {} images".format(month, len(month_images)))

################################################
# You can further group the subsets using the built in
# :meth:`ImageCollection.filter <descarteslabs.catalog.ImageCollection.filter>` method.
spring_images = images.filter(
    lambda i: i.acquired.month > 2 and i.acquired.month < 6
)
fall_images = images.filter(
    lambda i: i.acquired.month > 8 and i.acquired.month < 12
)

print(
    "There are {} Spring images & {} Fall images.".format(
        len(spring_images), len(fall_images)
    )
)

################################################
# Mosaic and display these two image collections.
spring_arr = spring_images.mosaic("red green blue", scaling="display", resolution=120)

fall_arr = fall_images.mosaic("red green blue", scaling="display", resolution=120)
display(spring_arr, fall_arr, size=4, title=["Spring", "Fall"])
