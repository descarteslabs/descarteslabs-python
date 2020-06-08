.. _output-destinations:
.. default-role:: py:obj

Output Destinations
-------------------

.. note::
  Output destinations control *where* results are stored -- like an email versus a download link. You can use output formats, which control *how* the results are stored, in conjunction with output destinations.

  For example, with ``destination='email@example.com'`` you might use ``format='geotiff'`` or ``format='json'``. Both would send emails; the emails would have links to download data in GeoTIFF versus JSON format.

..
  TODO: Add "Some output destinations can only be used with certain output formats. For example, with the Catalog destination you can only use the GeoTIFF format." when we have the Catalog destination

.. contents::
  :local:
  :depth: 2
  :backlinks: none

When calling `~.models.compute`, you can pick the destination for the results using the ``destination`` argument.

If you don't need to supply any options for the destination, you can pass the destination name as a string::

  >>> two = wf.Int(1) + 1
  >>> two.compute(destination="download")

For the email destination, you can pass the receiver's email address as a string for convenience::

  >>> two = wf.Int(1) + 1
  >>> two.compute(destination="example@email.com")

If you would like to provide more destination options, you pass the destination as a dictionary::

  >>> two = wf.Int(1) + 1
  >>> two.compute(destination={"type": "email", "to": "example@email.com", "subject": "My Computation is Done"})

Note that when passing the destination as a dictionary, it must include a ``type`` key corresponding to the desired destination.

Destination Options
^^^^^^^^^^^^^^^^^^^

The following is a list of the available options for each destination. The keys in the destination dictionary must match the keys listed here.

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

  >>> two = wf.Int(1) + 1
  >>> two.compute(destination={"type": "download"})

Email
~~~~~

Shorthand: an email address, like ``"example@email.com"``

Email is equivalent to `Download`_, but also sends an email when the job is done. The email contains a link to download the data. Anyone with that link can download the data. As with `Download`_, that link will expire after 10 days.

Options
*******

- ``to``: the email address to send the email to (string or list of strings)
- ``cc``: the email address to cc on the email (string or list of strings)
- ``bcc``: the email address to bcc on the email (string or list of strings)
- ``subject``: the subject of the email (string, default "Your Computation is Finished")
- ``body``: the body of the email (string, default "The computation of your Job has finished.")

Compatible Formats
******************

- All :ref:`formats <output-formats>`

Examples
********
  >>> two = wf.Int(1) + 1
  >>> two.compute(destination="example@email.com")

  >>> two = wf.Int(1) + 1
  >>> two.compute(destination={"type": "email", "to": "example@email.com", "subject": "My Computation is Done"})
