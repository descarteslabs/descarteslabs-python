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

For NumPy functionality that can be used with `.Array`, see :ref:`numpy-functions`.

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.array.Array
  ~descarteslabs.workflows.types.array.MaskedArray

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

:workflow_api_nontoc_heading:`Interactive`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.interactive.map
  ~descarteslabs.workflows.interactive.Map
  ~descarteslabs.workflows.interactive.MapApp
  ~descarteslabs.workflows.interactive.WorkflowsLayer
  ~descarteslabs.workflows.interactive.LayerController
  ~descarteslabs.workflows.interactive.LayerControllerList
  ~descarteslabs.workflows.interactive.ParameterSet

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
  ~descarteslabs.workflows.models.exceptions.JobOOM
  ~descarteslabs.workflows.models.exceptions.JobAuth
  ~descarteslabs.workflows.models.exceptions.JobInvalid
  ~descarteslabs.workflows.models.exceptions.JobInvalidTyping
  ~descarteslabs.workflows.models.exceptions.JobDeadlineExceeded
  ~descarteslabs.workflows.models.exceptions.JobTerminated
  ~descarteslabs.workflows.models.exceptions.JobInterrupt
  ~descarteslabs.workflows.models.exceptions.TimeoutError

:workflow_api_nontoc_heading:`gRPC Client`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.client.Client

.. toctree::
  :hidden:
  :glob:

  docs/types/geospatial
  docs/types/array
  docs/types/primitives
  docs/types/containers
  docs/types/datetimes
  docs/types/function
  docs/types/math
  docs/types/constants
  docs/types/identifier
  docs/interactive
  docs/execution
  docs/results
  docs/exceptions
  docs/client
  /descarteslabs/common/retry/readme


:workflow_api_nontoc_heading:`Numpy Functions`

.. autosummary::
  :nosignatures:

  ~descarteslabs.workflows.types.numpy.absolute
  ~descarteslabs.workflows.types.numpy.add
  ~descarteslabs.workflows.types.numpy.all
  ~descarteslabs.workflows.types.numpy.any
  ~descarteslabs.workflows.types.numpy.arccos
  ~descarteslabs.workflows.types.numpy.arccosh
  ~descarteslabs.workflows.types.numpy.arcsin
  ~descarteslabs.workflows.types.numpy.arcsinh
  ~descarteslabs.workflows.types.numpy.arctan
  ~descarteslabs.workflows.types.numpy.arctan2
  ~descarteslabs.workflows.types.numpy.argmax
  ~descarteslabs.workflows.types.numpy.argmin
  ~descarteslabs.workflows.types.numpy.cbrt
  ~descarteslabs.workflows.types.numpy.ceil
  ~descarteslabs.workflows.types.numpy.concatenate
  ~descarteslabs.workflows.types.numpy.conj
  ~descarteslabs.workflows.types.numpy.conjugate
  ~descarteslabs.workflows.types.numpy.copysign
  ~descarteslabs.workflows.types.numpy.cos
  ~descarteslabs.workflows.types.numpy.cosh
  ~descarteslabs.workflows.types.numpy.deg2rad
  ~descarteslabs.workflows.types.numpy.degrees
  ~descarteslabs.workflows.types.numpy.divide
  ~descarteslabs.workflows.types.numpy.equal
  ~descarteslabs.workflows.types.numpy.exp
  ~descarteslabs.workflows.types.numpy.exp2
  ~descarteslabs.workflows.types.numpy.expm1
  ~descarteslabs.workflows.types.numpy.fabs
  ~descarteslabs.workflows.types.numpy.float_power
  ~descarteslabs.workflows.types.numpy.floor
  ~descarteslabs.workflows.types.numpy.floor_divide
  ~descarteslabs.workflows.types.numpy.fmax
  ~descarteslabs.workflows.types.numpy.fmin
  ~descarteslabs.workflows.types.numpy.fmod
  ~descarteslabs.workflows.types.numpy.greater
  ~descarteslabs.workflows.types.numpy.greater_equal
  ~descarteslabs.workflows.types.numpy.histogram
  ~descarteslabs.workflows.types.numpy.isfinite
  ~descarteslabs.workflows.types.numpy.isinf
  ~descarteslabs.workflows.types.numpy.isnan
  ~descarteslabs.workflows.types.numpy.less
  ~descarteslabs.workflows.types.numpy.less_equal
  ~descarteslabs.workflows.types.numpy.log
  ~descarteslabs.workflows.types.numpy.log2
  ~descarteslabs.workflows.types.numpy.log10
  ~descarteslabs.workflows.types.numpy.log1p
  ~descarteslabs.workflows.types.numpy.logaddexp
  ~descarteslabs.workflows.types.numpy.logaddexp2
  ~descarteslabs.workflows.types.numpy.logical_and
  ~descarteslabs.workflows.types.numpy.logical_or
  ~descarteslabs.workflows.types.numpy.logical_xor
  ~descarteslabs.workflows.types.numpy.logical_not
  ~descarteslabs.workflows.types.numpy.maximum
  ~descarteslabs.workflows.types.numpy.minimum
  ~descarteslabs.workflows.types.numpy.mod
  ~descarteslabs.workflows.types.numpy.multiply
  ~descarteslabs.workflows.types.numpy.negative
  ~descarteslabs.workflows.types.numpy.nextafter
  ~descarteslabs.workflows.types.numpy.not_equal
  ~descarteslabs.workflows.types.numpy.power
  ~descarteslabs.workflows.types.numpy.rad2deg
  ~descarteslabs.workflows.types.numpy.radians
  ~descarteslabs.workflows.types.numpy.reciprocal
  ~descarteslabs.workflows.types.numpy.remainder
  ~descarteslabs.workflows.types.numpy.reshape
  ~descarteslabs.workflows.types.numpy.rint
  ~descarteslabs.workflows.types.numpy.sign
  ~descarteslabs.workflows.types.numpy.signbit
  ~descarteslabs.workflows.types.numpy.sin
  ~descarteslabs.workflows.types.numpy.sinh
  ~descarteslabs.workflows.types.numpy.spacing
  ~descarteslabs.workflows.types.numpy.sqrt
  ~descarteslabs.workflows.types.numpy.square
  ~descarteslabs.workflows.types.numpy.stack
  ~descarteslabs.workflows.types.numpy.subtract
  ~descarteslabs.workflows.types.numpy.tan
  ~descarteslabs.workflows.types.numpy.tanh
  ~descarteslabs.workflows.types.numpy.transpose
  ~descarteslabs.workflows.types.numpy.true_divide
  ~descarteslabs.workflows.types.numpy.trunc

.. toctree::
  :hidden:
  :glob:

  docs/types/numpy
