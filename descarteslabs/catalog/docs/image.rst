
.. default-role:: py:obj

.. role:: nontoc_heading

Image
=====

:nontoc_heading:`Classes`

.. list-table::

  * - :py:class:`descarteslabs.catalog.Image`
  * - :py:class:`~descarteslabs.catalog.image_upload.ImageUpload`

:nontoc_heading:`Data Types`

.. list-table::

  * - :py:class:`descarteslabs.catalog.File`
  * - :py:class:`~descarteslabs.catalog.search.SummaryResult`
  * - :py:class:`~descarteslabs.catalog.StorageState`
  * - :py:class:`~descarteslabs.catalog.image_upload.ImageUploadOptions`
  * - :py:class:`~descarteslabs.catalog.image_upload.ImageUploadType`
  * - :py:class:`~descarteslabs.catalog.image_upload.OverviewResampler`
  * - :py:class:`~descarteslabs.catalog.image_upload.ImageUploadStatus`

-----

.. autoclass:: descarteslabs.catalog.Image
  :autosummary:
  :exclude-members: acquired, acquired_end, alt_cloud_fraction, area, azimuth_angle,
    bits_per_pixel, bright_fraction, cloud_fraction, cs_code, files, fill_fraction, geometry,
    geotrans, incidence_angle, preview_file, preview_url,
    processing_pipeline_id, projection, provider_id, provider_url, published,
    reflectance_scale, roll_angle, satellite_id, solar_azimuth_angle,
    solar_elevation_angle, storage_state, view_angle, x_pixels, y_pixels
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.search.SummaryResult
  :autosummary:
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.image_upload.ImageUpload
  :autosummary:
  :exclude-members: end_datetime, errors, events, image, image_upload_options, job_id,
    product_id, resumable_urls, start_datetime, status
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.File

.. autoclass:: descarteslabs.catalog.StorageState

.. autoclass:: descarteslabs.catalog.image_upload.ImageUploadOptions

.. autoclass:: descarteslabs.catalog.image_upload.ImageUploadType

.. autoclass:: descarteslabs.catalog.image_upload.OverviewResampler

.. autoclass:: descarteslabs.catalog.image_upload.ImageUploadStatus
