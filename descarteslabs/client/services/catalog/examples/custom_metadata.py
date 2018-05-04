"""In this example I will show you how to create a new imagery set
with some custom metadata that you supply. This example builds on the
hello_catalog.py example.
"""

import geojson
import arrow
import os
from time import sleep
import descarteslabs as dl
from descarteslabs.ext.catalog import catalog


product_id = catalog.add_product(
        'building_mask:osm:v1',
        title='OSM Building Mask v1',
        description='Rasterized OSM building footprints from vector data.'
                    ' Quality varies regionally. This product has user supplied'
                    ' geometry and aquired date.'
    )['data']['id']

band_id = catalog.add_band(
        product_id=product_id,  # id of the product we just created.
        name='footprint',  # this is a unique name to describe what the band encodes.
        jpx_layer=0,
        srcfile=0,
        srcband=1,  # src band is always a 1-based index (counting starts at 1)
        nbits=8,
        dtype='Byte',
        nodata=0,
        data_range=[0, 2**8 - 1],
        type='mask',
    )['data']['id']

image_path = os.path.join(os.path.dirname(__file__), 'building_mask.tif')

# You can add any valid image metadata here and it will be set on the auto
# generated image metadata during processing. Acquired date and geometry
# are two fields which may need overriding as they are hard to infer. The auto
# geometry calculation simple makes a north south aligned bounding box around
# the image.
custom_metadata = {
    'acquired': arrow.get(0).shift(days=20).isoformat(),
    'geometry': geojson.utils.generate_random(  # dont do this, just for example.
        'Polygon',
        numberVertices=4,
        boundingBox=[-125.00, 25.32, -59.53, 49.10]  # CONUS
    )
}

catalog.upload_image(image_path, product_id, metadata=custom_metadata)

processed_image_id = '{}:{}'.format(product_id, 'building_mask')
image = None
while True:
    try:
        image = dl.metadata.get(processed_image_id)
        break
    except Exception:
        sleep(2)

assert(image['acquired'] == custom_metadata['acquired'])
# clean up

for band in dl.metadata.bands(products=product_id):
    catalog.remove_band(product_id, band['id'])

for _image in dl.metadata.search(products=product_id)['features']:
    catalog.remove_image(product_id, _image['id'])

sleep(10)

catalog.remove_product(product_id)
