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

from .attributes import Attribute, TypedAttribute
from .blob import Blob
from .catalog_base import CatalogObjectBase, CatalogObjectReference


class BlobUpload(CatalogObjectBase):
    """Internal class used to initiate a blob upload."""

    _doc_type = "storage_upload"
    _url = "/storage/upload"
    _no_inherit = True

    storage_id = Attribute(
        mutable=False,
        serializable=False,
        sticky=True,
        readonly=True,
    )
    storage = CatalogObjectReference(
        Blob,
        require_unsaved=True,
        mutable=False,
        serializable=True,
        sticky=True,
        doc="""~descarteslabs.catalog.Blob: Blob instance with all desired metadata fields.""",
    )
    resumable_url = TypedAttribute(
        str,
        readonly=True,
        doc="""str: Upload URL to which the client will transfer the file contents.

        This field is for internal use by the client only.
        """,
    )
    signature = TypedAttribute(
        str,
        readonly=True,
        doc="""str: Signature for the upload operation.

        This field is for internal use by the client only.
        """,
    )
