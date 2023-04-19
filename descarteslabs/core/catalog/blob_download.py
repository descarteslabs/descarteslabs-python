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

from .attributes import TypedAttribute
from .catalog_base import CatalogObjectBase


class BlobDownload(CatalogObjectBase):
    """Internal class used to initiate a blob upload."""

    _doc_type = "storage_download"
    _url = "/storage/download"
    _no_inherit = True

    resumable_url = TypedAttribute(
        str,
        readonly=True,
        doc="""str: Download URL from which the client will transfer the file contents.

        This field is for internal use by the client only.
        """,
    )
