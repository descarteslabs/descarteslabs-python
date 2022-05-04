"""
==================================================
Create time stacks of images
==================================================

The Scenes API returns a series of metadata. This
example demonstrates how to aggregate the
returned scenes by date.
"""
import descarteslabs.scenes

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
# Create a SceneCollection
scenes, ctx = descarteslabs.scenes.search(
    taos,
    products="landsat:LC08:01:RT:TOAR",
    start_datetime="2018-01-01",
    end_datetime="2018-12-31",
    cloud_fraction=0.7,
    limit=500,
)
print("There are {} scenes in the collection.".format(len(scenes)))

################################################
# To create subcollections using the Scenes API, we have
# the built in methods :meth:`SceneCollection.groupby <descarteslabs.scenes.scenecollection.SceneCollection.groupby>`
# and :meth:`SceneCollection.filter <descarteslabs.scenes.scenecollection.SceneCollection.filter>`

# If we want to create multiple subsets based
# on those properties, we can use the
# :meth:`SceneCollection.groupby <descarteslabs.scenes.scenecollection.SceneCollection.groupby>` method
for (year, month), month_scenes in scenes.groupby(
    "properties.date.year", "properties.date.month"
):
    print("{}: {} scenes".format(month, len(month_scenes)))

################################################
# You can further group the subsets using the built in
# :meth:`SceneCollection.filter <descarteslabs.scenes.scenecollection.SceneCollection.filter>` method
spring_scenes = scenes.filter(
    lambda s: s.properties.date.month > 2 and s.properties.date.month < 6
)
fall_scenes = scenes.filter(
    lambda s: s.properties.date.month > 8 and s.properties.date.month < 12
)

print(
    "There are {} Spring scenes & {} Fall scenes.".format(
        len(spring_scenes), len(fall_scenes)
    )
)
