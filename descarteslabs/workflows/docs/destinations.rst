.. _output-destinations:
.. default-role:: py:obj

Output Destinations
-------------------

.. note::
  Output destinations control *where* results are stored -- like a download link versus the DL Catalog. You can use output formats, which control *how* the results are stored, in conjunction with output destinations.

  For example, with ``destination='email'`` you might use ``format='geotiff'`` or ``format='json'``. Both would send emails; the emails would have links to download data in GeoTIFF versus JSON format.

  Some destinations can only be used with certain formats. For example, with the Catalog destination, you can only use the GeoTIFF format.

.. contents::
  :local:
  :depth: 2
  :backlinks: none

When calling `~.models.compute`, you can pick the destination for the results using the ``destination`` argument.

If you don't need to supply any options for the destination, you can use the destination's *shorthand*::

  >>> two = wf.Int(1) + 1
  >>> two.compute(destination="download")

If you would like to provide more destination options, you pass the destination as a dictionary::

  >>> two = wf.Int(1) + 1
  >>> two.compute(destination={"type": "email", "subject": "My Computation is Done"})

Note that when passing the destination as a dictionary, it must include a ``type`` key with the destination's name.

Available Destinations
^^^^^^^^^^^^^^^^^^^^^^

The following is a list of the available destinations and their options. The keys in the destination dictionary must match the keys listed here.

Download
~~~~~~~~

Shorthand: ``"download"``

Download (the default) stores a result (in any :ref:`format <output-formats>`) and gives you a link to download it. The link is valid for 10 days after job completion, then the data is automatically deleted. Download is good for getting results back locally, whether to continue using results in Python (with the ``pyarrow`` format) or save results to disk (with formats like ``geotiff``).

Options
*******

- No options

Compatible Formats
******************

- All :ref:`formats <output-formats>`

Examples
********
  >>> two = wf.Int(1) + 1
  >>> two.compute(destination="download")
  1
  >>> two.compute(destination="download", format="json")
  b'1'

Email
~~~~~

Shorthand: ``"email"``

Email is equivalent to `Download`_, but also sends you an email when the job is done. The email contains a link to download the data. Anyone with that link can download the data, so be careful forwarding it. As with `Download`_, the link will expire after 10 days.

Options
*******

- ``subject``, str, default "Your job has completed": The subject of the email. Always prefixed with ``Workflows:``.
- ``body``, str, default "Your Workflows job is done.": The body of the email.

Compatible Formats
******************

- All :ref:`formats <output-formats>`. However, widely-used formats like JSON or GeoTIFF usually make the most sense for email. With formats like MsgPack and especially PyArrow, you'd have to write code to parse the data, instead of clicking the download link and getting a file you can easily work with.

Examples
********
  >>> two = wf.Int(1) + 1
  >>> two.compute(destination="email", format="json")

  >>> two = wf.Int(1) + 1
  >>> two.compute(destination={"type": "email", "subject": "My Computation is Done"}, format="json")

Catalog
~~~~~~~

Shorthand: a `.catalog.Image`

Uploads a Workflows `~.geospatial.Image` to the :ref:`Catalog <catalog_v2_guide>`. Can only be used when computing a Workflows `~.geospatial.Image`.

`.Image.compute` or `.Job.result` with this destination will just return the `.catalog.Image` object, not the data uploaded.

Options
*******

Usually, you should set ``rescale=True`` and ``change_dtype=True``. However, since they can change your data in unexpected ways, they are off by default.

- ``image``, `.catalog.Image`: The Catalog `~.catalog.Image` object to upload to.
- ``overwrite``, bool, default False: Overwrites the image if it already exists.
- ``rescale``, bool, default False: Rescales pixel values in each band from ``physical_range`` to ``data_range``, only if ``physical_range`` is set for the band. (When loading imagery, Workflows automatically rescales values into ``physical_range``, so this reverses that.)
- ``change_dtype``, bool, default False: changes the data type of the uploaded array to match the data type of the `~.catalog.Product`. Usually combined with ``rescale``. (When loading imagery, Workflows converts to float64, so this undoes that.)

  Beware of data loss before setting ``change_dtype``: whether with ``rescale``, `.Image.scale_values`, or plain arithmetic, be sure the values can be represented in the Product's data type. For example, if the product in Catalog was ``uint16``, and your Workflows `~.geospatial.Image` currently held ``float64`` values from 0.0 to 1.0, converting those to ``uint16`` would just give you only 0s and 1s, so you'd want to rescale to a range like 0-10000 first to avoid data loss.


Transformations
***************

Workflows does a number of transformations to make your Workflows `~.geospatial.Image` compatible with the Catalog, such as reordering bands to match by name, rescaling and changing dtype, if requested, and using the Image's mask to fill in `~.catalog.GenericBand.nodata` values and generate an alpha band. Here are the full details:

- If `~.Image.acquired` is not already set on the `.catalog.Image` you pass, it's taken from ``properties['date']`` on the Workflows `~.geospatial.Image`, or if that's also not set, then from the current timestamp.
- Reorders the Workflows `~.geospatial.Image` bands to the product's band order, matching by band name. If the names don't match, assumes the bands are already in order.
- If ``rescale`` is True: Rescales pixel values from ``physical_range`` to ``data_range``, for each band where ``physical_range`` is set.
- If ``change_dtype`` is True: Converts to the Product's dtype.
- Fills in nodata values from the Image's mask, for bands with a ``nodata`` value.
- Create an alpha band from the Image's mask if:

  - The Product has exactly 1 alpha band, which must be a `~.catalog.MaskBand` with `~.catalog.MaskBand.is_alpha` set to True.
  - The Workflows `~.geospatial.Image` doesn't have a band for alpha (it has one less band than the product, and doesn't have a band with the alpha band's name).

Requirements
************

- The Workflows `~.geospatial.Image` must have the same number (and ideally same names) of bands as the Catalog Product you're uploading to (except an alpha band, which is generated automatically).
- All the bands in the Catalog Product must have the same data type.
- The Catalog Product must either have an alpha band (a `~.catalog.MaskBand` with `~.catalog.MaskBand.is_alpha` set to True), or every band must have a `~.catalog.GenericBand.nodata` value set (nodata is preferrable).
- You must have write access to the `~.catalog.Product`.

Compatible Formats
******************

- Only GeoTIFF. Though if you don't set ``format=`` and it defaults to ``pyarrow``, it's automatically switched to GeoTIFF for you. To control the details of the GeoTIFF that's uploaded to Catalog (overviews, overview resampler, etc.), specify ``format={"type": "geotiff", ...}`` with the parameters you want.

Examples
********

>>> import descarteslabs as dl
>>> import descarteslabs.workflows as wf
>>> composite = (
...     wf.ImageCollection.from_id("sentinel-1:GRD", "2020-01-01", "2020-05-01")
...     .mean(axis="images")
... )

Assume the product ``org:my_product_id`` already has the same bands as ``composite`` (in this case, ``vv`` and ``vh``), and the bands have `~.catalog.GenericBand.nodata` values set.

We can upload a single Catalog `~.catalog.Image`:

>>> image = dl.catalog.Image(name="my_image", product_id="org:my_product_id")
>>> tile = dl.scenes.DLTle.from_latlon(35.6870, -105.9378, 10, 1024, 0)
>>> 
>>> composite.compute(tile, destination=image)
Job ID: 8b21474899b177431d404e42e25a958cc32302af37646f7e
[######] | Steps: 21/21 | Stage: SUCCEEDED
Image: my_image
  id: org:my_product_id:my_image
  product: org:my_product_id
  created: Wed Jan  1 12:00:00 2020

Or, if you need to set options:

>>> composite.compute(
...     tile,
...     destination={
...         "type": "catalog",
...         "image": image,
...         "overwrite": True,
...         "rescale": True,
...         "change_dtype": True,
...     },
... )

More commonly, you'd upload many Images by splitting the area into tiles and launching concurrent upload Jobs:

>>> tiles = dl.scenes.DLTile.from_shape(aoi_geometry, 10, 1024, 0)
>>> images = [dl.catalog.Image(name=tile.key.replace(":", "_"), product_id="org:my_product_id") for tile in tiles]
>>> jobs = [composite.compute(tile, destination=image, block=False) for tile, image in zip(tiles, images)]