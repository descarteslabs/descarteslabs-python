.. default-role:: py:obj

.. the following role `workflow_api_nontoc_heading` is to avoid duplication of toc with headings and
  the required hidden toc at the bottom of this page

.. role:: workflow_api_nontoc_heading

.. _workflows_api_reference:

Workflows
-------------

The tables below list the types and functions available through the Workflows API. For information on how these objects work together, checkout out the :ref:`Workflows Guide <workflows_guide>`.

.. include:: /descarteslabs/workflows/docs/request.rst

:workflow_api_nontoc_heading:`Geospatial`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.geospatial.Feature
  ~descarteslabs.workflows.types.geospatial.FeatureCollection
  ~descarteslabs.workflows.types.geospatial.GeoContext
  ~descarteslabs.workflows.types.geospatial.Geometry
  ~descarteslabs.workflows.types.geospatial.Image
  ~descarteslabs.workflows.types.geospatial.ImageCollection
  ~descarteslabs.workflows.types.geospatial.ImageCollectionGroupby
  ~descarteslabs.workflows.types.geospatial.load_geojson
  ~descarteslabs.workflows.types.geospatial.load_geojson_file

:workflow_api_nontoc_heading:`Primitives`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.primitives.Primitive
  ~descarteslabs.workflows.types.primitives.Any
  ~descarteslabs.workflows.types.primitives.NoneType
  ~descarteslabs.workflows.types.primitives.Bool
  ~descarteslabs.workflows.types.primitives.Number
  ~descarteslabs.workflows.types.primitives.Int
  ~descarteslabs.workflows.types.primitives.Float
  ~descarteslabs.workflows.types.primitives.Str

:workflow_api_nontoc_heading:`Containers`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.containers.Dict
  ~descarteslabs.workflows.types.containers.Tuple
  ~descarteslabs.workflows.types.containers.List
  ~descarteslabs.workflows.types.containers.Struct
  ~descarteslabs.workflows.types.containers.zip

:workflow_api_nontoc_heading:`Datetimes`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.datetimes.Datetime
  ~descarteslabs.workflows.types.datetimes.Timedelta

:workflow_api_nontoc_heading:`Function`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.function.Function

:workflow_api_nontoc_heading:`Arithmetic`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.toplevel.arithmetic.log
  ~descarteslabs.workflows.types.toplevel.arithmetic.log2
  ~descarteslabs.workflows.types.toplevel.arithmetic.log10
  ~descarteslabs.workflows.types.toplevel.arithmetic.sqrt
  ~descarteslabs.workflows.types.toplevel.arithmetic.cos
  ~descarteslabs.workflows.types.toplevel.arithmetic.sin
  ~descarteslabs.workflows.types.toplevel.arithmetic.tan

:workflow_api_nontoc_heading:`Conditionals`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.toplevel.conditionals.where

:workflow_api_nontoc_heading:`Constants`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.constants.constants.e
  ~descarteslabs.workflows.types.constants.constants.inf
  ~descarteslabs.workflows.types.constants.constants.nan
  ~descarteslabs.workflows.types.constants.constants.pi

:workflow_api_nontoc_heading:`Interactive`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.interactive.map
  ~descarteslabs.workflows.interactive.Map
  ~descarteslabs.workflows.interactive.MapApp
  ~descarteslabs.workflows.interactive.WorkflowsLayer
  ~descarteslabs.workflows.interactive.LayerController
  ~descarteslabs.workflows.interactive.LayerControllerList

:workflow_api_nontoc_heading:`Parameter`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.identifier.parameter

:workflow_api_nontoc_heading:`Execution`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.models.Workflow
  ~descarteslabs.workflows.models.Job
  ~descarteslabs.workflows.models.XYZ
  ~descarteslabs.workflows.models.XYZErrorListener
  ~descarteslabs.workflows.models.compute
  ~descarteslabs.workflows.models.publish
  ~descarteslabs.workflows.models.use

:workflow_api_nontoc_heading:`Result Types`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.results.ImageResult
  ~descarteslabs.workflows.results.ImageCollectionResult
  ~descarteslabs.workflows.results.GeometryResult
  ~descarteslabs.workflows.results.GeometryCollectionResult
  ~descarteslabs.workflows.results.FeatureResult
  ~descarteslabs.workflows.results.FeatureCollectionResult

:workflow_api_nontoc_heading:`Exceptions`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.core.ProxyTypeError
  ~descarteslabs.workflows.models.exceptions.JobComputeError
  ~descarteslabs.workflows.models.exceptions.TimeoutError

:workflow_api_nontoc_heading:`gRPC Client`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.client.Client

.. toctree::
  :hidden:
  :glob:

  docs/types/geospatial
  docs/types/primitives
  docs/types/containers
  docs/types/datetimes
  docs/types/function
  docs/types/toplevel
  docs/types/constants
  docs/types/identifier
  docs/interactive
  docs/execution
  docs/results
  docs/exceptions
  docs/client
  /descarteslabs/common/retry/readme
