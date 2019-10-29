.. default-role:: py:obj

.. the following role `nontoc_heading` is to avoid duplication of toc with
  headings and the required hidden toc at the bottom of this page.  It has
  been styled in `docs/static/scss/catalog.scss`.

.. role:: nontoc_heading

.. !!! If you change the class hierarchy, *make sure* to update the `Inheritance`
  action for all derived classes !!!

.. _catalog_v2_api_reference:

Catalog
-------

The tables below list the types and functions available through the Catalog API. For
information on how these objects work together, check out the
:ref:`Catalog Guide <catalog_v2_guide>`.

.. automodule:: descarteslabs.catalog

:nontoc_heading:`Catalog Objects`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.CatalogClient
  ~descarteslabs.catalog.CatalogObject
  ~descarteslabs.catalog.NamedCatalogObject
  ~descarteslabs.catalog.AttributeValidationError

:nontoc_heading:`Product`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.Product

:nontoc_heading:`Bands`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.Band
  ~descarteslabs.catalog.SpectralBand
  ~descarteslabs.catalog.MicrowaveBand
  ~descarteslabs.catalog.MaskBand
  ~descarteslabs.catalog.ClassBand
  ~descarteslabs.catalog.GenericBand
  ~descarteslabs.catalog.DerivedBand
  ~descarteslabs.catalog.Resolution

:nontoc_heading:`Image`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.Image
  ~descarteslabs.catalog.image_upload.ImageUpload
  ~descarteslabs.catalog.File

:nontoc_heading:`Search`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.search.Search
  ~descarteslabs.catalog.search.ImageSearch
  ~descarteslabs.catalog.search.SummaryResult

.. toctree::
  :hidden:
  :glob:

  docs/catalog_object.rst
  docs/product.rst
  docs/band.rst
  docs/image.rst
  docs/search.rst
