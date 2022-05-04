"""
The Descartes Labs Vector service allows you store vector features (points, polygons, etc.)
with associated key-value properties, and query that data by geometry or by properties.

It works best at the scale of millions of features. For small amounts of vector data
that easily fit in memory, working directly with a GeoJSON file or similar may be more efficient.

* :doc:`Feature <docs/feature>`: an object following the GeoJSON format
* :doc:`FeatureCollection <docs/featurecollection>`: a convenient helper class for working with millions of features
* :doc:`AsyncJob <docs/async_job>`: a convenient helper class that represents a delete job or copy job
* :doc:`UploadTask <docs/uploadtask>`: an object that represents an upload job
* :doc:`ExportTask <docs/exporttask>`: an object that represents an export job

It's available under ``descarteslabs.vectors``.
"""


from .feature import Feature
from .featurecollection import FeatureCollection
from ..client.services.vector import properties
from .exceptions import (
    VectorException,
    WaitTimeoutError,
    FailedJobError,
    InvalidQueryException,
    FailedCopyError,
)
from .async_job import DeleteJob, CopyJob

__all__ = [
    "Feature",
    "FeatureCollection",
    "properties",
    "WaitTimeoutError",
    "FailedJobError",
    "VectorException",
    "InvalidQueryException",
    "DeleteJob",
    "CopyJob",
    "FailedCopyError",
]
