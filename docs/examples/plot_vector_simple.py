"""
===============================
Vector Example
===============================

The vector service exposes a highly scalable backend for ingesting
and searching over millions of features. This example creates a
new product, adds features, searches for features and then deletes
the product.

The API reference for these methods and classes is at :py:mod:`descarteslabs.vectors`.
"""

from descarteslabs.vectors import Feature, FeatureCollection, properties as p
import uuid

################################################
# Let's make a test :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# use :meth:`FeatureCollection.create <descarteslabs.vectors.featurecollection.FeatureCollection.create>`.
id_suffix = str(uuid.uuid4())
fc = FeatureCollection.create(
    product_id="my_test_product" + id_suffix,
    title="My Test Product",
    description="This product was created using an example file.",
)

print(fc)

################################################
# Let's load this existing :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# from an id.
id_ = fc.id

fc = FeatureCollection(id_)
print(fc)

################################################
# Let's create a :class:`Feature <descarteslabs.vectors.feature.Feature>` for this product and add it using
# :meth:`FeatureCollection.add <descarteslabs.vectors.featurecollection.FeatureCollection.add>`.

feature = Feature(
    geometry={
        "type": "Polygon",
        "coordinates": [
            [
                [-105.86975097656249, 36.94550173495345],
                [-104.930419921875, 36.94550173495345],
                [-104.930419921875, 37.70120736474139],
                [-105.86975097656249, 37.70120736474139],
                [-105.86975097656249, 36.94550173495345],
            ]
        ],
    },
    properties={"foo": "bar"},
)

fc.add([feature])

################################################
# Search for features in this product intersecting an aoi using
# :meth:`FeatureCollection.filter <descarteslabs.vectors.featurecollection.FeatureCollection.filter>`
# and :meth:`FeatureCollection.features <descarteslabs.vectors.featurecollection.FeatureCollection.features>`.

aoi = {
    "type": "Polygon",
    "coordinates": [
        [
            [-105.194091796875, 36.88181755936464],
            [-104.765625, 36.88181755936464],
            [-104.765625, 37.13404537126446],
            [-105.194091796875, 37.13404537126446],
            [-105.194091796875, 36.88181755936464],
        ]
    ],
}

fc = fc.filter(geometry=aoi)
print(list(fc.features()))  # this returns an iterator

################################################
# Search for :class:`Feature <descarteslabs.vectors.feature.Feature>` in this
# :class:`FeatureCollection <descarteslabs.vectors.featurecollection.FeatureCollection>`
# intersecting the aoi where ``foo=bar``.

print(list(fc.filter(properties=(p.foo == "bar")).features()))

################################################
# Search for :class:`Feature <descarteslabs.vectors.feature.Feature>` where property ``foo=foo``.
# There should be no results.
print(list(fc.filter(properties=(p.foo == "foo")).features()))

################################################
# And cleanup

fc.delete()
