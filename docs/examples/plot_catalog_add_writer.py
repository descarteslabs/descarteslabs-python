"""
==================================================
Adding a writer to a Catalog product
==================================================

This example adds an additional writer to a product.

"""

from descarteslabs.client.services import Catalog
import uuid


catalog = Catalog()


product_id = str(uuid.uuid4())[:8]

product = catalog.add_product(
    product_id=product_id,
    title='My Product',
    description='This is my product.'
)
print('Product {} added.'.format(product['data']['id']))

product_attrs = product['data']['attributes']

##############################
# The product that we just created doesn't have any writers.
print('Product writers: {}'.format(product_attrs['writers']))

##############################
# However, we can add a writer to this product.
changed_product = catalog.change_product(
    product_id=product['data']['id'],
    writers=['email:someuser@gmail.com']  # Could be the user hash, email, group, or org of another user.
)
changed_product_attrs = changed_product['data']['attributes']

##############################
# Now, ``'email:someuser@gmail.com'`` is a writer for this product. This user can now change the product metadata,
# add bands, and add imagery to this product.
print('Changed product writers: {}'.format(changed_product_attrs['writers']))

#############################
# Delete our product; we don't need it anymore.
catalog.remove_product(product_id, add_namespace=True)
print('Product removed.')
