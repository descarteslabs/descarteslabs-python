
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
  :members:
  :inherited-members:
  :undoc-members:
  :exclude-members: ATTRIBUTES, band_index, created, data_range, data_type, description,
    display_range, extra_properties, file_index, id, is_modified, jpx_layer_index,
    modified, name, nodata, owners, product, product_id, readers, resolution,
    sort_order, state, tags, type, vendor_order, processing_levels, derived_params,
    vendor_band_name, writers, reload, save, serialize, update, get_or_create, v1_properties

.. autoclass:: descarteslabs.catalog.SpectralBand
  :autosummary:
  :members:
  :inherited-members:
  :undoc-members:
  :exclude-members: v1_properties

.. autoclass:: descarteslabs.catalog.MicrowaveBand
  :autosummary:
  :members:
  :inherited-members:
  :undoc-members:
  :exclude-members: processing_levels, derived_params, v1_properties

.. autoclass:: descarteslabs.catalog.MaskBand
  :autosummary:
  :members:
  :inherited-members:
  :undoc-members:
  :exclude-members: processing_levels, derived_params, v1_properties

.. autoclass:: descarteslabs.catalog.ClassBand
  :autosummary:
  :members:
  :inherited-members:
  :undoc-members:
  :exclude-members: processing_levels, derived_params, v1_properties

.. autoclass:: descarteslabs.catalog.GenericBand
  :autosummary:
  :members:
  :inherited-members:
  :undoc-members:
  :exclude-members: v1_properties

.. autoclass:: descarteslabs.catalog.DerivedBand
  :autosummary:
  :members:
  :inherited-members:
  :undoc-members:
  :exclude-members: processing_levels, derived_params, v1_properties
