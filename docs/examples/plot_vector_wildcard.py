"""
==================================================
Vector Example With Wildcard Search
==================================================

You can use wildcards to search for properties in the Vector service.

The API reference for these methods and classes is at :py:mod:`descarteslabs.vectors`.
"""

from descarteslabs.vectors import Feature, FeatureCollection, properties as p
import uuid

################################################
# Let's make a test :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# use :meth:`FeatureCollection.create <descarteslabs.vectors.featurecollection.FeatureCollection.create>`.

id_suffix = str(uuid.uuid4())
fc = FeatureCollection.create(product_id="my_test_product" + id_suffix,
                              title="My Test Product",
                              description="This product was created using an example file.")

print("Created Feature Collection with id {}".format(fc.id))

################################################
# Establish the geometry
polygon = {
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

################################################
# Let's create a bunch of :class:`Feature <descarteslabs.vectors.feature.Feature>` for this product and add it using
# :meth:`FeatureCollection.add <descarteslabs.vectors.featurecollection.FeatureCollection.add>`.
# Each feature has a 'count' property with a value 'number n' where 'n' is
# in the range 0 through 19.  In addition there is feature with value
# 'number _' and  a feature with value 'number %'.

features = [Feature(geometry=polygon, properties={"count": "number %d" % count})
            for count in range(0, 20)]

features.append(Feature(geometry=polygon, properties={"count": "number %"}))
features.append(Feature(geometry=polygon, properties={"count": "number _"}))

fc.add(features)

################################################
# Search for :class:`Feature <descarteslabs.vectors.feature.Feature>` in this
# :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# where ``count=number 5``.
# This returns a single result.

features = fc.filter(properties=(p.count.like("number 5"))).features()

for feature in features:
    print("{}".format(feature.properties["count"]))

################################################
# Search for :class:`Feature <descarteslabs.vectors.feature.Feature>` in this
# :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# with any 2-digit ``count`` that ends in ``5``.
# This returns one item.

features = fc.filter(properties=(p.count.like("number _5"))).features()

for feature in features:
    print("{}".format(feature.properties["count"]))

################################################
# Search for :class:`Feature <descarteslabs.vectors.feature.Feature>` in this
# :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# with any ``count`` that ends in ``5``.
# This returns 2 items (including ``number 5``).

features = fc.filter(properties=(p.count.like("number %5"))).features()

for feature in features:
    print("{}".format(feature.properties["count"]))

################################################
# Search for :class:`Feature <descarteslabs.vectors.feature.Feature>` in this
# :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# where ``count`` has the value ``number _``.
# This returns a single item.

features = fc.filter(properties=(p.count.like("number \\_"))).features()

for feature in features:
    print("{}".format(feature.properties["count"]))

################################################
# Search for :class:`Feature <descarteslabs.vectors.feature.Feature>` in this
# :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# where ``count`` has the value ``number %``.
# This returns a single item.

features = fc.filter(properties=(p.count.like("number \\%"))).features()

for feature in features:
    print("{}".format(feature.properties["count"]))

################################################
# Search for :class:`Feature <descarteslabs.vectors.feature.Feature>` where property ``count``
# with any ``count`` that starts with ``c``.
# There should be no results.

features = fc.filter(properties=(p.count.like("c%"))).features()

for feature in features:
    print("{}".format(feature.properties["count"]))

################################################
# And cleanup

print("Deleting Feature Collection with id {}".format(fc.id))
fc.delete()
