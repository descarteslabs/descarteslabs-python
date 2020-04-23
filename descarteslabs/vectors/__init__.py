"""
The Descartes Labs Vector service allows you store vector features (points, polygons, etc.)
with associated key-value properties, and query that data by geometry or by properties.

It works best at the scale of millions of features. For small amounts of vector data
that easily fit in memory, working directly with a GeoJSON file or similar may be more efficient.

* :doc:`Feature <docs/feature>`: an object following the GeoJSON format
* :doc:`FeatureCollection <docs/featurecollection>`: a convenient helper class for working with millions of features

It's available under ``descarteslabs.vectors``.
"""


from .feature import Feature
from .featurecollection import FeatureCollection
from descarteslabs.client.services.vector import properties
from descarteslabs.vectors.exceptions import (
    VectorException,
    WaitTimeoutError,
    FailedJobError,
    InvalidQueryException,
    FailedCopyError,
)
from descarteslabs.vectors.async_job import DeleteJob, CopyJob

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
