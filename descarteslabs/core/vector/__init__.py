from ..common.property_filtering import Properties

from ..common.vector import models
from .vector import Feature, Table, TableOptions
from .vector_client import VectorClient

properties = Properties()

__all__ = [
    "Feature",
    "models",
    "properties",
    "Table",
    "TableOptions",
    "VectorClient",
]
