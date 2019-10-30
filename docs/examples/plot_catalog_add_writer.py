"""
==================================================
Adding a writer to a Catalog product
==================================================

This example adds an additional writer to a product.

"""

from descarteslabs.catalog import Product
import uuid


product_id = str(uuid.uuid4())[:8]

product = Product(id=product_id, name="My Product", description="This is my product.")
product.save()
print("Product {} added.".format(product.id))

##############################
# The product that we just created doesn't have any writers.
print("Product writers: {}".format(product.writers))

##############################
# However, we can add a writer to this product.
product.writers = ["email:someuser@gmail.com"]
product.save()

##############################
# Now, ``'email:someuser@gmail.com'`` is a writer for this product. This user can now change the product metadata,
# add bands, and add imagery to this product.
print("Changed product writers: {}".format(product.writers))

#############################
# Delete our product; we don't need it anymore.
product.delete()
print("Product removed.")
