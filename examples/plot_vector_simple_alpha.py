"""
===============================
Vector Example
===============================

The vector service exposes a highly scalable backend for ingesting
and searching over millions of features. This example creates a
new product, adds features, searches for features and then deletes
the product.

"""

from descarteslabs.client.services.vector import Vector

# Lets Instantiate a Vector Client
vector_client = Vector()


################################################
# Let's make a test product using the vector client.

product = vector_client.create_product(name="my_test_product",
                                       title="My Test Product",
                                       description="This product was created using an example file.")

print(product.data)

################################################
# Let's create a feature for this product.

feature = vector_client.create_feature(product.data.id, geometry={
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
}, properties={"foo": "bar"})


################################################
# Search for features in this product intersecting an aoi.

aoi = {
    "type": "Polygon",
    "coordinates": [
            [
                [
                    -105.194091796875,
                    36.88181755936464
                ],
                [
                    -104.765625,
                    36.88181755936464
                ],
                [
                    -104.765625,
                    37.13404537126446
                ],
                [
                    -105.194091796875,
                    37.13404537126446
                ],
                [
                    -105.194091796875,
                    36.88181755936464
                ]
            ]
    ]
}

print(vector_client.search_features(product.data.id, geometry=aoi))

################################################
# Search for features in this product intersecting aoi with matching property.

# Match all features where property `foo=bar`
search_properties = {"foo": "bar"}

print(vector_client.search_features(product.data.id, geometry=aoi, properties=search_properties))

################################################
# Search all features where property `foo=foo`. There should be no results.
search_properties = {"foo": "foo"}

print(vector_client.search_features(product.data.id, geometry=aoi, properties=search_properties))

################################################
# And cleanup

vector_client.delete_product(product.data.id)
