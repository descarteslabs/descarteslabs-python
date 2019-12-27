
.. default-role:: py:obj

Base Catalog Objects
====================

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.CatalogClient
  ~descarteslabs.catalog.CatalogObject
  ~descarteslabs.catalog.NamedCatalogObject

-----

.. Don't include undoc members!
.. autoclass:: descarteslabs.catalog.CatalogClient
  :members:

.. autoclass:: descarteslabs.catalog.CatalogObject
  :autosummary:
  :exclude-members: id, created, modified, owners, readers, writers, extra_properties,
    tags
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.NamedCatalogObject
  :autosummary:
  :exclude-members: id, name, product, product_id
  :members:
  :undoc-members:
