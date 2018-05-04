"""This example demonstrates how to upload an imagery product where each scene
has more than one data file associated with it. To upload many images at at time
use the `descarteslabs-catalog upload` script.
"""
import os
import descarteslabs as dl
from descarteslabs.ext.catalog import catalog
from time import sleep
import arrow

# As always we will instantiate a product and some bands. If you are not
# familiar with how to do that, refer to the `hello_catalog.py` example first.

product_id = catalog.add_product(
        'building_mask:osm:test_v1',
        title='Multi File OSM Building Mask Test',
        description='Rasterized OSM building footprints from vector data. '
                    'Quality varies regionally. Multi file scene test.'
    )['data']['id']

band0_id = catalog.add_band(
        product_id=product_id,  # id of the product we just created.
        name='footprint_file0',  # this is a unique name to describe what the band encodes.
        jpx_layer=0,
        srcfile=0,
        srcband=1,  # src band is always a 1-based index (counting starts at 1)
        nbits=8,
        dtype='Byte',
        nodata=0,
        data_range=[0, 2**8 - 1],
        type='mask',
    )['data']['id']


band1_id = catalog.add_band(
        product_id=product_id,  # id of the product we just created.
        band_id='footprint_file1',  # this is a unique name to describe what the band encodes.
        name='Footprint 1',  # More human friendly name for display purposes (can be same as id).
        jpx_layer=0,
        # Note the different srcfile index here. This band references data in the second file in the scene.
        srcfile=1,
        srcband=1,  # src band is always a 1-based index (counting starts at 1)
        nbits=8,
        dtype='Byte',
        nodata=0,
        data_range=[0, 2**8 - 1],
        type='mask',
    )['data']['id']

image_path = os.path.join(os.path.dirname(__file__), 'building_mask.tif')
other_image_path = os.path.join(os.path.dirname(__file__), 'other_building_mask.tif')
os.system('cp {src} {dest}'.format(src=image_path, dest=other_image_path))


# Now we will use the upload_file method with the `multi` flag enabled to do a
# multi file upload. When uploading scenes with multiple files you are required
# to specify the unique key with the `image_key` field. Failure to
# provide unique keys could result in data being overwritten.

image_key = '_'.join(['test_multi_image_scene', str(arrow.now().timestamp)])
catalog.upload_image(
    [image_path, other_image_path],
    product_id,
    multi=True,
    image_key=image_key,
)

# Poll for processed image
processed_image_id = '{}:{}'.format(product_id, image_key)

image = None
while True:
    try:
        image = dl.metadata.get(processed_image_id)
        break
    except Exception:
        sleep(2)

assert len(image['files']) == 2
