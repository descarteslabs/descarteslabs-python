"""
==================================================
Deploying a Keras model
==================================================

This example integrates many components of the Descartes Labs Platform.
We run a trained neural net built in to Keras over an area of interest (state of New Mexico).

First we break our AOI up into tiles that the neural net can consume. This net we are using
(resnet50) takes tiles of Height x Width (224, 224) pixels. Then we spawn an asynchronous task
for each tile we generated from the AOI. The Descartes Labs tasks service will parallelize the work
so that it can be scaled immensely.

"""
from __future__ import print_function

import requests

from descarteslabs.client.services.raster import Raster
from descarteslabs.client.services.tasks import Tasks
from descarteslabs.catalog import Product, ClassBand


def deploy_keras_model(dltile, src_product_id, dest_product_id):
    import tensorflow as tf
    import numpy as np

    from descarteslabs.catalog import Image, properties as p

    raster_client = Raster()
    # NOTE substitute your own trained model here.
    model = tf.keras.applications.resnet50.ResNet50()
    scene = next(
        Image.search()
        .filter(p.product_id == src_product_id)
        .intersects(raster_client.dltile(dltile))
        .sort("acquired")
        .limit(1)
    )
    tile, meta = raster_client.ndarray(
        scene.id,
        bands=["red", "green", "blue"],
        scales=[[0, 255]] * 3,
        ot="Byte",
        dltile=dltile,
    )
    # resnet50 expects the shape of the input array to be 4 dimensional, which allows for batch
    # predictions.
    tile_for_prediction = tile[np.newaxis, :]
    pred = model.predict(tile_for_prediction)
    # get predicted class with a simple maximum of class probabilities.
    class_ = np.argmax(pred, 1)
    # create a new raster of the tile area with one channel of the prediction from the model.
    image_ndarray = np.full(tile.shape[:-1], class_, dtype=np.uint16)
    # upload a tile of this "prediction" to catalog
    image_name = "-".join([src_product_id.replace(":", "_"), dltile.replace(":", "_")])
    image = Image(name=image_name, product_id=dest_product_id, acquired=scene.acquired)
    image.upload_ndarray(image_ndarray, raster_meta=meta)


if __name__ == "__main__":
    tasks = Tasks()
    raster_client = Raster()
    async_function = tasks.create_function(
        deploy_keras_model,
        image="us.gcr.io/dl-ci-cd/images/tasks/public/py2/default:v2018.06.20",
        name="deploy-resnet",
    )
    print("task group id", async_function.group_id)
    prod = Product.get(Product.namespace_id("resnet-predictions"))
    if not prod:
        prod = Product(
            id="resnet-predictions",
            name="Resnet Predictions",
            description="classification results of applying resnet trained with imagenet dataset over"
            "random satellite imagery tiles",
        )
        prod.save()
        band = ClassBand(
            product=prod,
            name="class",
            band_index=0,
            data_type="UInt16",
            data_range=[0, 999],
            colormap_name="magma",
        )
        band.save()

    r = requests.get(
        "https://raw.githubusercontent.com/whosonfirst-data/whosonfirst-data-admin-us/master/data/856/884/93/85688493.geojson"  # noqa
    )
    geom = r.json()["geometry"]
    tiles = raster_client.dltiles_from_shape(1000, 224, 0, geom)
    for tile in tiles["features"]:
        async_function(tile.properties.key, "modis:09:v2", prod.id)
        print("spawned task for tile", tile.properties.key)
