.. _result-types:
.. default-role:: py:obj

Result Types
------------

When calling `~.models.compute`, results are packaged into these types (or returned as builtin Python types when appropriate, such as for `.Dict`, `.Int`, or `.Datetime`).

Note that these are just simple containers, and aren't interoparable with their corresponding Proxytypes. For instance, you can't do ``wf.Image.from_id("foo") + my_image_result``, where ``my_image_result`` is an `.ImageResult` instance.

.. https://plaintexttools.github.io/plain-text-table/ sure makes this easier to format!

.. automodule:: descarteslabs.workflows.result_types
  :members:
  :autosummary:
