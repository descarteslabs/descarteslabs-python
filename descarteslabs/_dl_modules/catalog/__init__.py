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
    DerivedParamsAttribute,
    GenericBand,
    MaskBand,
    MicrowaveBand,
    ProcessingLevelsAttribute,
    ProcessingStepAttribute,
    SpectralBand,
)
from .image import Image, StorageState
from .image_upload import (
    ImageUpload,
    ImageUploadEvent,
    ImageUploadEventSeverity,
    ImageUploadEventType,
    ImageUploadOptions,
    ImageUploadStatus,
    ImageUploadType,
    OverviewResampler,
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
    File,
    Resolution,
    ResolutionUnit,
)

from ..common.property_filtering import GenericProperties

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
    "DerivedParamsAttribute",
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
    "ProcessingLevelsAttribute",
    "ProcessingStepAttribute",
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
