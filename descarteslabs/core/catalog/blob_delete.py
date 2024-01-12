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

from .attributes import ListAttribute, TypedAttribute
from .catalog_base import CatalogObjectBase


class BlobDelete(CatalogObjectBase):
    """Internal class used to perform bulk deleting of blobs."""

    _doc_type = "storage_delete"
    _url = "/storage/delete"
    _no_inherit = True

    ids = ListAttribute(
        TypedAttribute(str),
        mutable=False,
        serializable=True,
        doc="""list[str]: List of blob IDs to delete.""",
    )
