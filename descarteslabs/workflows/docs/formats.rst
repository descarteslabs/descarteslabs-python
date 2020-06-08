.. _output-formats:
.. default-role:: py:obj

Output Formats
--------------

.. note::
  Output formats control *how* results are stored -- like GeoTIFF, JSON, etc. You can use output destinations, which control *where* the results are stored, in conjunction with output formats.

  For example, with ``format='geotiff'`` you might use ``destination='email@example.com'`` or ``destination='download'``. Both would produce GeoTIFFs; one would send an email with a link to the file, and the other would download the GeoTIFF within your script.

..
  TODO: Add "Some output formats must be used with certain destinations. For example, with the Catalog destination you can only use the GeoTIFF format." when we have the Catalog destination

.. contents::
  :local:
  :depth: 2
  :backlinks: none

When calling `~.models.compute`, you can pick the output format for the results using the ``format`` argument. The supported formats are "pyarrow" (default), "geotiff", "json", and "msgpack".

If you don't need to supply any options for the formatter, you can pass the format name as a string::

  >>> two = wf.Int(1) + 1
  >>> two.compute(format="json")

If you would like to provide more format options, you pass the format as a dictionary::

  >>> two = wf.Int(1) + 1
  >>> two.compute(format={"type": "pyarrow", "compression": "brotli"})

Note that when passing the format as a dictionary, it must include a ``type`` key corresponding to the desired format.

The results will be returned differently depending on the ``format`` specified. When using the "pyarrow" format, results will be deserialized and unpacked into :ref:`result-types`. When using the "json" format, results will be deserialized into a Python dictionary. For all other formats, the results will not be deserialized and will be returned as raw bytes.

Format Options
^^^^^^^^^^^^^^

The following is a list of the available options for each format. The keys in the format dictionary must match the keys listed here.

PyArrow
~~~~~~~

Shorthand: ``"pyarrow"``

`PyArrow <https://arrow.apache.org/docs/python/ipc.html#arbitrary-object-serialization>`_ (the default) is the best format for loading data back into Python for further use. It's fast and memory-efficient, especially for NumPy arrays, and also automatically unpacks results into :ref:`result-types`.

- ``compression``: the type of compression used for the data (string, default "lz4", one of "lz4" or "brotli")

GeoTIFF
~~~~~~~

Shorthand: ``"geotiff"``

`GeoTIFF <https://en.wikipedia.org/wiki/GeoTIFF>`_ is the best format for using raster data with other geospatial software, such as ArcGIS or QGIS. Only `~.geospatial.Image` objects can be computed in GeoTIFF format. GeoTIFF data is returned in raw bytes, so in most cases, you'll want to write the data out to a file.

- ``overviews``: whether to include overviews, overview levels are automatically calculated (bool, default True)
- ``tiled``: whether to create a tiled GeoTIFF (bool, default True)
- ``compression``: the compression to use (string, default "LZW", one of "LZW", "None", or "JPEG")
- ``overviewresampler``: the resampler to use for calculating overviews (string, default "nearest", one of "nearest", "average", "bilinear", "cubic", or "mode")

JSON
~~~~

Shorthand: ``"json"``

`JSON <json.org/json-en.html>`_ is the best format for using the data in other languages because it is language independent.

- No options

MsgPack
~~~~~~~

Shorthand: ``"msgpack"``

`MsgPack <https://msgpack.org/index.html>`_ is similar to JSON. It is a good format for using the data in other languages, but it is faster and smaller than JSON, especially for NumPy Arrays. Note that array data (`Array`, `~.geospatial.Image`, etc.) is encoded in raw bytes using the `msgpack-numpy <https://github.com/lebedov/msgpack-numpy>`_ library, so msgpack is only recommended for use with Python when computing data containing arrays.

- No options
