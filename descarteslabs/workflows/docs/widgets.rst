.. default-role:: py:obj

.. note::
  Rendering widgets in a Jupyter notebook requires `ipywidgets <https://ipywidgets.readthedocs.io/en/stable/index.html>`_, an optional dependency. See :ref:`the guide <installing-ipyleaflet>` for installation and troubleshooting instructions.

  However, you can still create widgets, and publish Workflows using them, without ipywidgets installed.

Widgets make it easy to interactively explore your Workflows code, without dealing with ipywidgets or callback functions. You can use widgets just like normal Workflows values.

All widget functions return subclasses of the `~.widget.Widget` base class.

Read more in the :ref:`widget section of the Workflows guide <workflows-widgets>`.

Widgets
-------
.. automodule:: descarteslabs.workflows.interactive.widgets
  :members:
  :autosummary:
  :undoc-members:


.. autoclass:: descarteslabs.workflows.types.widget.Widget
  :members:
