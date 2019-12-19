
.. default-role:: py:obj

Image
=====

.. autosummary::
  :nosignatures:

  ~descarteslabs.catalog.Image
  ~descarteslabs.catalog.ImageUpload

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

.. autoclass:: descarteslabs.catalog.ImageUpload
  :autosummary:
  :exclude-members: end_datetime, errors, events, image, image_upload_options, job_id,
    product_id, image_id, resumable_urls, start_datetime, status
  :members:
  :undoc-members:
