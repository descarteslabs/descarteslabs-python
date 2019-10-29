
.. default-role:: py:obj

.. role:: nontoc_heading

Bands
=====


:nontoc_heading:`Classes`

.. list-table::

  * - :py:class:`descarteslabs.catalog.Band`
  * - :py:class:`descarteslabs.catalog.SpectralBand`
  * - :py:class:`descarteslabs.catalog.MicrowaveBand`
  * - :py:class:`descarteslabs.catalog.MaskBand`
  * - :py:class:`descarteslabs.catalog.ClassBand`
  * - :py:class:`descarteslabs.catalog.GenericBand`
  * - :py:class:`descarteslabs.catalog.DerivedBand`

:nontoc_heading:`Data Types`

.. list-table::

  * - :py:class:`~descarteslabs.catalog.band.DataType`
  * - :py:class:`~descarteslabs.catalog.band.BandType`
  * - :py:class:`~descarteslabs.catalog.Resolution`
  * - :py:class:`~descarteslabs.catalog.ResolutionUnit`

-----

.. autoclass:: descarteslabs.catalog.Band
  :autosummary:
  :exclude-members: band_index, data_range, data_type, description, display_range,
    file_index, jpx_layer_index, nodata, resolution, sort_order, type
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.SpectralBand
  :autosummary:
  :exclude-members: wavelength_nm_center, wavelength_nm_fwhm, wavelength_nm_max,
    wavelength_nm_min
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.MicrowaveBand
  :autosummary:
  :exclude-members: bandwidth, frequency
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.MaskBand
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.ClassBand
  :autosummary:
  :exclude-members: class_labels, colormap, colormap_name
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.GenericBand
  :autosummary:
  :exclude-members: colormap, colormap_name, physical_range, physical_range_unit
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.DerivedBand
  :autosummary:
  :exclude-members: name, description, data_type, data_range, physical_range, bands,
    function_name
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.band.DataType

.. autoclass:: descarteslabs.catalog.band.BandType

.. autoclass:: descarteslabs.catalog.Resolution

.. autoclass:: descarteslabs.catalog.ResolutionUnit
