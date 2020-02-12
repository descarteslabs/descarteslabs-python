"""
The Catalog Service provides access to products, bands, and images
available from Descartes Labs.
"""

from .product import DeletionTaskStatus, Product, TaskState, UpdatePermissionsTaskStatus
from .band import (
    Band,
    BandType,
    ClassBand,
    Colormap,
    DataType,
    DerivedBand,
    GenericBand,
    MaskBand,
    MicrowaveBand,
    SpectralBand,
)
from .image import Image, StorageState
from .image_upload import (
    ImageUpload,
    ImageUploadOptions,
    ImageUploadStatus,
    ImageUploadType,
    OverviewResampler,
    ImageUploadEvent,
    ImageUploadEventType,
    ImageUploadEventSeverity,
)
from .search import ImageSearch, Search, Interval, AggregateDateField, SummaryResult
from .catalog_base import (
    CatalogClient,
    CatalogObject,
    DeletedObjectError,
    UnsavedObjectError,
)
from .named_catalog_base import NamedCatalogObject
from .attributes import (
    AttributeValidationError,
    DocumentState,
    Resolution,
    ResolutionUnit,
    File,
)

from descarteslabs.common.property_filtering import GenericProperties

properties = GenericProperties()

__all__ = [
    "AggregateDateField",
    "AttributeValidationError",
    "Band",
    "BandType",
    "CatalogClient",
    "CatalogObject",
    "ClassBand",
    "Colormap",
    "DataType",
    "DeletedObjectError",
    "DeletionTaskStatus",
    "DerivedBand",
    "DocumentState",
    "File",
    "GenericBand",
    "Image",
    "ImageSearch",
    "ImageUpload",
    "ImageUploadEvent",
    "ImageUploadEventSeverity",
    "ImageUploadEventType",
    "ImageUploadOptions",
    "ImageUploadStatus",
    "ImageUploadType",
    "Interval",
    "MaskBand",
    "MicrowaveBand",
    "NamedCatalogObject",
    "OverviewResampler",
    "Product",
    "properties",
    "Resolution",
    "ResolutionUnit",
    "Search",
    "SpectralBand",
    "StorageState",
    "SummaryResult",
    "TaskState",
    "UnsavedObjectError",
    "UpdatePermissionsTaskStatus",
]
