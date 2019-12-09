.. default-role:: py:obj

.. the following role `nontoc_heading` is to avoid duplication of toc with
  headings and the required hidden toc at the bottom of this page.  It has
  been styled in `docs/static/scss/catalog.scss`.

.. role:: nontoc_heading

.. !!! If you change the class hierarchy, *make sure* to update the `Inheritance`
  action for all derived classes !!!

.. admonition:: Compatibility Warnings

  Product ID:
    Instead of your Descartes Labs user ID, the product id is now prepended
    with your Descartes Labs organization ID if available, otherwise it defaults to your
    Descartes Labs user ID.  If the prefix is your Descartes Labs organization ID, it means that your product id must be unique within your organization.

  Image and Band Name:
    The allowed characters are now ``a`` through ``z``, ``A`` through ``Z``, ``0``
    through ``9``, ``.``, ``_`` and ``-``.  Note that ``:`` is not an allowed
    character any more.

  Image and Band Owners:
    The image and band owners will include the owners of the related product.  This
    means that if you're not the owner of the product (but you are the writer), you
    can add an image or band and the owners will not only include your Descartes Labs
    user ID but also the owners of the related product.

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
