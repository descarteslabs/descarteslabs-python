"""
The Catalog Service provides access to products, bands, and images
available from Descartes Labs.
"""

from .image import Image, StorageState
from .product import Product
from .image_upload import (
    ImageUpload,
    ImageUploadType,
    OverviewResampler,
    ImageUploadStatus,
)
from .band import (
    Band,
    BandType,
    Colormap,
    DataType,
    SpectralBand,
    MicrowaveBand,
    ClassBand,
    MaskBand,
    GenericBand,
    DerivedBand,
)
from .catalog_base import CatalogClient, CatalogObject, DeletedObjectError
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
    "AttributeValidationError",
    "Band",
    "BandType",
    "CatalogClient",
    "CatalogObject",
    "ClassBand",
    "Colormap",
    "DataType",
    "DeletedObjectError",
    "DerivedBand",
    "DocumentState",
    "File",
    "GenericBand",
    "Image",
    "ImageUpload",
    "ImageUploadStatus",
    "ImageUploadType",
    "MaskBand",
    "MicrowaveBand",
    "NamedCatalogObject",
    "OverviewResampler",
    "Product",
    "properties",
    "Resolution",
    "ResolutionUnit",
    "SpectralBand",
    "StorageState",
]
