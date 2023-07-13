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

from enum import auto
from typing import Any, Dict, Tuple

from strenum import LowercaseStrEnum

from .attributes import Attribute


class DocumentState(LowercaseStrEnum):
    MODIFIED = auto()
    NEW = auto()
    SAVED = auto()
    DELETED = auto()


class Document(object):
    """An Object or Document in a Descartes Labs service."""

    def __init__(self, saved=False, **kwargs) -> None:
        self._attributes = dict()
        self._modified = set()

        if saved:
            self._load_from_remote(kwargs)
        else:
            self._fill(kwargs, remote=saved)

        self._saved = saved
        self._deleted = False

    def __getattribute__(self, name: str) -> Any:
        try:
            deleted = object.__getattribute__(self, "_deleted")
        except AttributeError:
            deleted = False

        if deleted and name != "state" and not name.startswith("_"):
            class_name = object.__getattribute__(self, "__class__").__name__
            raise AttributeError(f"{class_name} has been deleted")

        return object.__getattribute__(self, name)

    def _clear_modified(self):
        """Clears the list of modified attributes."""
        self._modified = set()

    def _get_attributes(self) -> Dict[str, Attribute]:
        """Returns all of the Attributes in the document.

        Returns
        -------
        Dict[str, Attribute]
            The attribute instances in the document.
        """
        return {
            name: instance
            for name, instance in vars(self.__class__).items()
            if isinstance(instance, Attribute)
        }

    def _fill(self, data: dict, ignore_missing: bool = False, remote: bool = False):
        """Sets document attributes from a dictionary of data.

        Parameters
        ----------
        ignore_missing : bool, False
            If set, unknown attributes will be ignored.
        remote : bool, False
            If set, the data is from the remote server.
            Data provided in this way will set immutable and readonly attributes.

            Additionally, the document will be forced into a `saved` status and
            all modified fields will be cleared.
        """
        attributes = self._get_attributes()

        for key, value in data.items():
            if ignore_missing and key not in attributes:
                continue

            attributes[key].__set__(self, value, force=remote)

        if remote:
            self._clear_modified()
            self._saved = True

    def _load_from_remote(self, data: dict):
        """Populates the document instance with data from the remote server.

        Parameters
        ----------
        data : dict
            The response json to populate the document with.
        """
        self._fill(data, ignore_missing=True, remote=True)

    @classmethod
    def _serialize_filter_attribute(cls, name: str, value: Any) -> Tuple[str, Any]:
        """Serializes a filter attribute.

        Parameters
        ----------
        name : str
            The name of the attribute.
        value : Any
            The value of the attribute.

        Returns
        -------
        Tuple[str, Any]
            The serialized attribute name and value.
        """
        attribute: Attribute = getattr(cls, name)
        return (attribute.name, attribute.serialize(attribute.deserialize(value)))

    def update(self, ignore_missing=False, **kwargs):
        """Updates the document setting multiple attributes at a time.

        Parameters
        ----------
        ignore_missing : bool, False
            If set, unknown attributes will be ignored.
        """
        self._fill(kwargs, ignore_missing=ignore_missing)

    @property
    def state(self) -> DocumentState:
        """Returns the state of the current document instance.

        Returns
        -------
        :py:class:`~descarteslabs.common.client.DocumentState`
        """

        if self._deleted:
            return DocumentState.DELETED

        if not self._saved:
            return DocumentState.NEW
        if self.is_modified:
            return DocumentState.MODIFIED
        else:
            return DocumentState.SAVED

    @property
    def is_modified(self) -> bool:
        """Determines if the document has been modified."""
        return bool(self._modified)

    def to_dict(
        self,
        only_modified: bool = False,
        exclude_readonly: bool = False,
        exclude_none: bool = False,
    ) -> Dict[str, Any]:
        """Converts the document to a dictionary.

        Attributes will be serialized to JSON encodable types.

        Parameters
        ----------
        only_modified : bool, False
            If set, only modified attributes and their values will be included.
        exclude_readonly : bool, False
            If set, readonly attributes and their values are excluded.
        exclude_none : bool, False
            If set, attributes with a value of None will be excluded.

        Returns
        -------
        Dict[str, Any]
            The attributes matching the call parameters. The result of this function
            can be json encoded without modification.
        """
        attributes = self._get_attributes()
        data = {}

        for key, attribute in attributes.items():
            if exclude_readonly and attribute.readonly:
                continue

            if only_modified and key not in self._modified:
                continue

            value = getattr(self, key)

            if exclude_none and value is None:
                continue

            value = attribute.serialize(value)
            data[key] = value

        return data

    def __repr__(self) -> str:
        indent = "    "
        separator = f"\n{indent}"

        attributes = self._get_attributes()
        pairs = [
            "{}={}".format(key, repr(getattr(self, key))) for key in attributes.keys()
        ]
        return "<{}{}{}>".format(
            self.__class__.__name__, separator, separator.join(pairs)
        )
