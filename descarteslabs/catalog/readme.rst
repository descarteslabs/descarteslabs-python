.. default-role:: py:obj

.. the following role `nontoc_heading` is to avoid duplication of toc with
  headings and the required hidden toc at the bottom of this page.  It has
  been styled in `docs/static/scss/catalog.scss`.

.. role:: nontoc_heading

.. !!! If you change the class hierarchy, *make sure* to update the `Inheritance`
  action for all derived classes !!!

.. _catalog_v2_api_reference:

.. The automodule makes it possible to link here using :mod:
.. automodule:: descarteslabs.catalog

Catalog
-------

The Catalog API can be found as Python package ``descarteslabs.catalog``.  To install
please refer to `Installation </installation.html>`_.

.. admonition:: Compatibility Warnings

  There are a few minor differences from the previous API:

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

The tables below list the objects, types, and exceptions available through the Catalog
API.  For information on how these objects work together, check out the :ref:`Catalog
Guide <catalog_v2_guide>`.

:nontoc_heading:`Product`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.Product
  ~descarteslabs.catalog.DeletionTaskStatus
  ~descarteslabs.catalog.UpdatePermissionsTaskStatus

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

:nontoc_heading:`Image`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.Image
  ~descarteslabs.catalog.ImageUpload

:nontoc_heading:`Search`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.Search
  ~descarteslabs.catalog.ImageSearch
  ~descarteslabs.catalog.SummaryResult
  ~descarteslabs.catalog.properties

:nontoc_heading:`Types`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.DocumentState
  ~descarteslabs.catalog.TaskState
  ~descarteslabs.catalog.DataType
  ~descarteslabs.catalog.BandType
  ~descarteslabs.catalog.Colormap
  ~descarteslabs.catalog.Resolution
  ~descarteslabs.catalog.ResolutionUnit
  ~descarteslabs.catalog.File
  ~descarteslabs.catalog.StorageState
  ~descarteslabs.catalog.ImageUploadOptions
  ~descarteslabs.catalog.ImageUploadType
  ~descarteslabs.catalog.OverviewResampler
  ~descarteslabs.catalog.ImageUploadStatus

:nontoc_heading:`Exceptions`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.AttributeValidationError
  ~descarteslabs.catalog.DeletedObjectError
  ~descarteslabs.catalog.UnsavedObjectError

:nontoc_heading:`Base Catalog Objects`

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.CatalogClient
  ~descarteslabs.catalog.CatalogObject
  ~descarteslabs.catalog.NamedCatalogObject

.. toctree::
  :hidden:
  :glob:

  docs/product.rst
  docs/band.rst
  docs/image.rst
  docs/search.rst
  docs/types.rst
  docs/exceptions.rst
  docs/catalog_object.rst
