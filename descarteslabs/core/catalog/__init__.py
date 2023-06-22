"""
The Catalog Service provides access to products, bands, and images
available from Descartes Labs.
"""

# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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
from .blob import Blob, BlobCollection, BlobSearch, BlobSummaryResult, StorageType
from .image import Image, ImageSearch, ImageSummaryResult
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
from .search import (
    AggregateDateField,
    GeoSearch,
    Interval,
    Search,
    SummarySearchMixin,
)
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
    StorageState,
)

from ..common.property_filtering import Properties

properties = Properties()

__all__ = [
    "AggregateDateField",
    "AttributeValidationError",
    "Band",
    "BandCollection",
    "BandType",
    "Blob",
    "BlobCollection",
    "BlobSearch",
    "BlobSummaryResult",
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
    "GeoSearch",
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
    "ImageSummaryResult",
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
    "StorageType",
    "SummarySearchMixin",
    "TaskState",
    "UnsavedObjectError",
]
