"""
The Catalog Service provides access to products, bands, and images
available from Descartes Labs.
"""

from .product import (
    DeletionTaskStatus,
    Product,
    ProductCollection,
    TaskState,
)
from .band import (
    Band,
    BandCollection,
    BandType,
    ClassBand,
    Colormap,
    DataType,
    DerivedBand,
    DerivedBandCollection,
    DerivedParamsAttribute,
    GenericBand,
    MaskBand,
    MicrowaveBand,
    ProcessingLevelsAttribute,
    ProcessingStepAttribute,
    SpectralBand,
)
from .image import Image, StorageState
from .image_types import ResampleAlgorithm, DownloadFileFormat
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
from .image_collection import ImageCollection
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

from ..common.property_filtering import Properties

properties = Properties()

__all__ = [
    "AggregateDateField",
    "AttributeValidationError",
    "Band",
    "BandCollection",
    "BandType",
    "CatalogClient",
    "CatalogObject",
    "ClassBand",
    "Colormap",
    "DataType",
    "DeletedObjectError",
    "DeletionTaskStatus",
    "DerivedBand",
    "DerivedBandCollection",
    "DerivedParamsAttribute",
    "DocumentState",
    "DownloadFileFormat",
    "File",
    "GenericBand",
    "Image",
    "ImageCollection",
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
    "ProductCollection",
    "properties",
    "ResampleAlgorithm",
    "Resolution",
    "ResolutionUnit",
    "Search",
    "SpectralBand",
    "StorageState",
    "SummaryResult",
    "TaskState",
    "UnsavedObjectError",
]
