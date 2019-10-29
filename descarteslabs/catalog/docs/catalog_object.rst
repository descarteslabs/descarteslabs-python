
.. default-role:: py:obj

.. role:: nontoc_heading

Catalog Objects
===============

:nontoc_heading:`Classes`

.. list-table::

  * - :py:class:`descarteslabs.catalog.CatalogClient`
  * - :py:class:`~descarteslabs.catalog.CatalogObject`
  * - :py:class:`~descarteslabs.catalog.NamedCatalogObject`
  * - :py:class:`~descarteslabs.catalog.AttributeValidationError`

:nontoc_heading:`Data Types`

.. list-table::

  * - :py:class:`~descarteslabs.catalog.attributes.DocumentState`

-----

.. autoclass:: descarteslabs.catalog.CatalogClient
  :members:
  :undoc-members:

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

.. autoclass:: descarteslabs.catalog.attributes.DocumentState

.. autoclass:: descarteslabs.catalog.AttributeValidationError
