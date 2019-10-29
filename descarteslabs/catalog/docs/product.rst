
.. default-role:: py:obj

.. role:: nontoc_heading

Product
=======

:nontoc_heading:`Classes`

.. list-table::

  * - :py:class:`descarteslabs.catalog.Product`

:nontoc_heading:`Data Types`

.. list-table::

  * - :py:class:`~descarteslabs.catalog.product.TaskStatus`
  * - :py:class:`~descarteslabs.catalog.product.DeletionTaskStatus`
  * - :py:class:`~descarteslabs.catalog.product.UpdatePermissionsTaskStatus`
  * - :py:class:`~descarteslabs.catalog.product.TaskState`

-----

.. autoclass:: descarteslabs.catalog.Product
  :autosummary:
  :exclude-members: description, end_datetime, name, resolution_max,
    resolution_min, revisit_period_minutes_max, revisit_period_minutes_min,
    start_datetime
  :members:
  :undoc-members:

.. autoclass:: descarteslabs.catalog.product.TaskStatus
  :members:

.. autoclass:: descarteslabs.catalog.product.DeletionTaskStatus
  :members:

.. autoclass:: descarteslabs.catalog.product.UpdatePermissionsTaskStatus
  :members:

.. autoclass:: descarteslabs.catalog.product.TaskState
