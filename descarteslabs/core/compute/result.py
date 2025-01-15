# © 2025 EarthDaily Analytics Corp.
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

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union

import shapely.geometry

AnyDate = Union[datetime, str]
AnyExtraProperties = Dict[str, Union[str, float]]
AnyGeometry = Union[shapely.geometry.base.BaseGeometry, dict, str]
AnyResult = Union[bytes, "Serializable", Any]
AnyTags = Union[Set[str], List[str]]


class Serializable:
    """Interface for serializing objects to bytes as
    a result of a Function invocation."""

    def serialize(self) -> bytes:
        raise NotImplementedError()

    @classmethod
    def deserialize(cls, data: bytes):
        raise NotImplementedError()


class ComputeResult:
    def __init__(
        self,
        value: Optional[AnyResult] = None,
        description: Optional[str] = None,
        expires: Optional[AnyDate] = None,
        extra_properties: Optional[AnyExtraProperties] = None,
        geometry: Optional[AnyGeometry] = None,
        tags: Optional[AnyTags] = None,
    ):
        """Used to store the result of a compute function with additional attributes.

        When returned from a compute function, the result will be serialized and stored
        in the Storage service with the given attributes.

        Notes
        -----
        Results that are None and have no attributes will not be stored. If you want to
        store a None result with attributes, you can do so by passing in a None value
        as well as any attributes you wish to set.

        Examples
        --------
        Result with raw binary data:
        >>> from descarteslabs.services.compute import ComputeResult
        >>> result = ComputeResult(value=b"result", description="result description")

        Null result with attributes:
        >>> from descarteslabs.services.compute import ComputeResult
        >>> result = ComputeResult(None, geometry=geometry, tags=["tag1", "tag2"])

        Parameters
        ----------
        value: bytes, Serializable, or Any
            The resulting value of a compute function.
            This can be any bytes, any JSON serializable type or any type implementing
            the Serializable interface.
        description: str or None
            A description with further details on this result blob. The description can be
            up to 80,000 characters and is used by :py:meth:`Search.find_text`.
        expires: datetime, str, or None
            The date the result should expire and be deleted from storage.
            If a string is given, it must be in ISO 8601 format.
        extra_properties: dict or None
            A dictionary of up to 50 key/value pairs.

            The keys of this dictionary must be strings, and the values of this dictionary
            can be strings or numbers.  This allows for more structured custom metadata
            to be associated with objects.
        geometry: shapely.geometry.base.BaseGeometry, dict, str, or None
            The geometry associated with the result if any.
        tags: set, list, or None
            The tags to set on the catalog object for the result.
        """
        type_ = type(value)

        # The result is null and should not be stored if all attributes are null
        # otherwise, we'll allow a user to store a null result with attributes.
        self.isnull = (
            value is None
            and description is None
            and expires is None
            and extra_properties is None
            and geometry is None
            and tags is None
        )

        # If the result is already bytes
        if isinstance(value, bytes):
            value = value
        # If the user implements serialize
        elif callable(getattr(value, "serialize", None)):
            value = value.serialize()

            if not isinstance(value, bytes):
                raise Exception(
                    f"Serializer on {type_} must return bytes got {type(value)}"
                )
        # No specific serialize implementation try json
        else:
            try:
                value = json.dumps(value).encode()
            except Exception:
                raise Exception(
                    "Unable to serialize result. Return value must be"
                    " JSON encodable or implement the Serializable interface"
                ) from None

        self.value = value
        self.description = description
        self.expires = expires
        self.extra_properties = extra_properties
        self.geometry = geometry
        self.tags = tags
