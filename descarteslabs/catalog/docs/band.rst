
.. default-role:: py:obj

Bands
=====

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.Band
  ~descarteslabs.catalog.SpectralBand
  ~descarteslabs.catalog.MicrowaveBand
  ~descarteslabs.catalog.MaskBand
  ~descarteslabs.catalog.ClassBand
  ~descarteslabs.catalog.GenericBand
  ~descarteslabs.catalog.DerivedBand

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
  :autosummary:
  :exclude-members: is_alpha, data_range, display_range
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
