.. default-role:: py:obj

.. the following role `workflow_api_nontoc_heading` is to avoid duplication of toc with headings and
  the required hidden toc at the bottom of this page

.. role:: workflow_api_nontoc_heading

.. _workflows_api_reference:

Workflows
-------------

The tables below list the types and functions available through the Workflows API. For information on how these objects work together, check out out the :ref:`Workflows Guide <workflows_guide>`.

.. include:: /descarteslabs/workflows/docs/request.rst

.. toctree::
  :hidden:
  :glob:

  docs/types/geospatial
  docs/types/array
  docs/types/numpy
  docs/types/primitives
  docs/types/containers
  docs/types/datetimes
  docs/types/function
  docs/types/math
  docs/types/constants
  docs/types/identifier
  docs/types/conditional
  docs/interactive
  docs/widgets
  docs/execution
  docs/result_types
  docs/exceptions
  docs/client
  /descarteslabs/common/retry/readme


:workflow_api_nontoc_heading:`Geospatial`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.geospatial.Feature
  ~descarteslabs.workflows.types.geospatial.FeatureCollection
  ~descarteslabs.workflows.types.geospatial.GeoContext
  ~descarteslabs.workflows.types.geospatial.Geometry
  ~descarteslabs.workflows.types.geospatial.GeometryCollection
  ~descarteslabs.workflows.types.geospatial.Image
  ~descarteslabs.workflows.types.geospatial.ImageCollection
  ~descarteslabs.workflows.types.geospatial.ImageCollectionGroupby
  ~descarteslabs.workflows.types.geospatial.Kernel
  ~descarteslabs.workflows.types.geospatial.concat
  ~descarteslabs.workflows.types.geospatial.conv2d
  ~descarteslabs.workflows.types.geospatial.load_geojson
  ~descarteslabs.workflows.types.geospatial.load_geojson_file
  ~descarteslabs.workflows.types.geospatial.where

:workflow_api_nontoc_heading:`Array`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.array.Array
  ~descarteslabs.workflows.types.array.MaskedArray

:workflow_api_nontoc_heading:`NumPy Functions`

Workflows exposes a large chunk of the NumPy API. Either use the ``workflows.numpy`` submodule (preferred), or pass Workflows `.Array` or `.MaskedArray` objects into NumPy functions directly.

See :ref:`numpy-functions` for a full list of available functions.

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

:workflow_api_nontoc_heading:`Math`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.math.log
  ~descarteslabs.workflows.types.math.log2
  ~descarteslabs.workflows.types.math.log10
  ~descarteslabs.workflows.types.math.log1p
  ~descarteslabs.workflows.types.math.sqrt
  ~descarteslabs.workflows.types.math.exp
  ~descarteslabs.workflows.types.math.square
  ~descarteslabs.workflows.types.math.cos
  ~descarteslabs.workflows.types.math.arccos
  ~descarteslabs.workflows.types.math.sin
  ~descarteslabs.workflows.types.math.arcsin
  ~descarteslabs.workflows.types.math.tan
  ~descarteslabs.workflows.types.math.arctan
  ~descarteslabs.workflows.types.math.arctan2

:workflow_api_nontoc_heading:`Constants`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.constants.constants.e
  ~descarteslabs.workflows.types.constants.constants.inf
  ~descarteslabs.workflows.types.constants.constants.nan
  ~descarteslabs.workflows.types.constants.constants.pi

:workflow_api_nontoc_heading:`Parameter`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.identifier.parameter

:workflow_api_nontoc_heading:`Conditionals`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.conditional.ifelse

:workflow_api_nontoc_heading:`Interactive`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.interactive.map
  ~descarteslabs.workflows.interactive.flows
  ~descarteslabs.workflows.interactive.Map
  ~descarteslabs.workflows.interactive.MapApp
  ~descarteslabs.workflows.interactive.WorkflowsLayer
  ~descarteslabs.workflows.interactive.LayerController
  ~descarteslabs.workflows.interactive.LayerControllerList
  ~descarteslabs.workflows.interactive.ParameterSet
  ~descarteslabs.workflows.interactive.PixelInspector
  ~descarteslabs.workflows.interactive.WorkflowsBrowser

:workflow_api_nontoc_heading:`Widgets`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.interactive.widgets.checkbox
  ~descarteslabs.workflows.interactive.widgets.date
  ~descarteslabs.workflows.interactive.widgets.input
  ~descarteslabs.workflows.interactive.widgets.select
  ~descarteslabs.workflows.interactive.widgets.slider

:workflow_api_nontoc_heading:`Execution`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.models.Workflow
  ~descarteslabs.workflows.models.VizOption
  ~descarteslabs.workflows.models.Job
  ~descarteslabs.workflows.models.XYZ
  ~descarteslabs.workflows.models.XYZLogListener
  ~descarteslabs.workflows.models.compute
  ~descarteslabs.workflows.inspect
  ~descarteslabs.workflows.models.publish
  ~descarteslabs.workflows.models.use

:workflow_api_nontoc_heading:`Output Formats`

Workflows exposes a number of :ref:`serialization formats <output-formats>` for results. Use the ``format`` argument to `~.models.compute` to specify a format.

.. toctree::
  :maxdepth: 3

  docs/formats

:workflow_api_nontoc_heading:`Output Destinations`

Workflows exposes a number of :ref:`destinations <output-destinations>` for results. Use the ``destination`` argument to `~.models.compute` to specify a destination.

.. toctree::
  :maxdepth: 3

  docs/destinations

:workflow_api_nontoc_heading:`Result Types`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.result_types.ImageResult
  ~descarteslabs.workflows.result_types.ImageCollectionResult
  ~descarteslabs.workflows.result_types.GeometryResult
  ~descarteslabs.workflows.result_types.GeometryCollectionResult
  ~descarteslabs.workflows.result_types.FeatureResult
  ~descarteslabs.workflows.result_types.FeatureCollectionResult

:workflow_api_nontoc_heading:`Exceptions`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.core.ProxyTypeError
  ~descarteslabs.workflows.models.exceptions.JobComputeError
  ~descarteslabs.workflows.models.exceptions.JobOOM
  ~descarteslabs.workflows.models.exceptions.JobAuth
  ~descarteslabs.workflows.models.exceptions.JobInvalid
  ~descarteslabs.workflows.models.exceptions.JobInvalidTyping
  ~descarteslabs.workflows.models.exceptions.JobDeadlineExceeded
  ~descarteslabs.workflows.models.exceptions.JobTerminated
  ~descarteslabs.workflows.models.exceptions.JobInterrupt
  ~descarteslabs.workflows.models.exceptions.JobTimeoutError
  ~descarteslabs.workflows.models.exceptions.JobCancelled
  ~descarteslabs.workflows.models.exceptions.JobSerialization

:workflow_api_nontoc_heading:`gRPC Client`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.client.Client
