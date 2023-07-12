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

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Callable, Type, TypeVar, Union

from ..property_filtering import Property
from .sort import Sort

if TYPE_CHECKING:
    from .document import Document

T = TypeVar("T")


class Attribute(Property):
    """An attribute defined on a Document."""

    def __init__(
        self,
        type: Type[T] = None,
        default: Union[T, Callable] = None,
        doc: str = None,
        filterable: bool = False,
        mutable: bool = True,
        readonly: bool = False,
        sortable: bool = False,
        sticky: bool = False,
    ):
        """Defines a document attribute.

        Examples
        --------
        .. code::

            class MyDocument(Document):
                id: id = Attribute(readonly=True)
                name: str = Attribute(str)
                set_once: str = Attribute(str, mutable=False)

            doc = MyDocument(name="test", set_once="can only be set once")
            doc.set_once = "error"

        Parameters
        ----------
        default : Any, Callable, None
            The default value for the attribute when no value is defined.
            If a callable is provided, it will be called once when the attribute is first
            fetched.
        doc : str, None
            Sets the doc string for the attribute.
        filterable: bool, False
            If set, the attribute can be used as a filter.
        mutable : bool, True
            If not set, the attribute will be immutable and can only be set once.
        readonly : bool, False
            If set, the attribute cannot be modified by the user.
            This is designed for attributes set and managed exclusively by the server.
        sortable : bool, False
            If set, the attribute can be used to sort results.
        sticky : bool, False
            If set, the attribute exists on the client only.
            This attribute will be ignored when set by the server.
        """
        super().__init__(None)

        if sticky and readonly:
            raise ValueError("Using sticky and readonly together does not make sense.")

        self.type = type
        self.default = default
        self.filterable = filterable
        self.mutable = mutable
        self.readonly = readonly
        self.sortable = sortable
        self.sticky = sticky

        if doc is None and type:
            doc = type.__doc__

        doc_modifiers = []

        if not self.mutable:
            doc_modifiers.append(
                "The attribute is `immutable` and cannot be modified once set."
            )

        if self.readonly:
            doc_modifiers.append("The attribute is `readonly` and cannot be modified.")

        doc = "{}: {}".format(self._doc_type, doc)

        if doc_modifiers:
            doc += "\n\n" + "\n\n".join(doc_modifiers)

        self.__doc__ = doc

    @property
    def _doc_type(self) -> str:
        return "{} or {}".format(self.type.__name__, self.default)

    def __set_name__(self, owner, name):
        """Called when an attribute is defined on a document."""
        if not hasattr(owner, "_attributes"):
            setattr(owner, "_attributes", dict())

        if not hasattr(owner, "_modified"):
            setattr(owner, "_modified", set())

        self.name = name

    def __get__(self, instance: "Document", owner) -> T:
        """Called when an attribute value is accessed.

        If no value is defined for the attribute, the default will be applied.
        """
        # Instance will be None if accessed as a class property
        # this occurs when generating documentation with Sphinx.
        # In this case, return the attribute instance for documentation.
        if instance is None:
            return self

        if self.name not in instance._attributes:
            if callable(self.default):
                default = self.default()
            else:
                default = self.default

            instance._attributes[self.name] = self.deserialize(default)

        return instance._attributes.get(self.name)

    def __set__(self, instance: "Document", value, force: bool = False):
        """Called when attribute is set to a given value.

        Values will be deserialized to the type defined in the attribute.
        Additionally, the attribute will be marked as modified.

        Parameters
        ----------
        force : bool, False
            When force is set, the value is assumed to be from the server.
            In this case, `mutable` and `readonly` are ignored and `sticky` is respected.
        """
        if force and self.sticky:
            return

        if not force:
            self._raise_immutable("set", instance)

        if self.type and value is not None:
            value = self.deserialize(value)

        # Only update the value if it has changed
        if (self.name not in instance._attributes and value is None) or (
            instance._attributes.get(self.name) == value
        ):
            return

        # It is being set by the server, it is no longer modified
        if force:
            instance._modified.discard(self.name)
        else:
            instance._modified.add(self.name)

        instance._attributes[self.name] = value

    def __delete__(self, instance: "Document", force: bool = False):
        """Called when an attribute is deleted."""
        if not force:
            self._raise_immutable("delete", instance)

        instance._attributes.pop(self.name, None)

    def __neg__(self):
        return self._to_sort(ascending=False)

    def _to_sort(self, ascending: bool = True):
        if not self.sortable:
            raise ValueError(f"Cannot sort on property: {self.name}")

        return Sort(self.name, ascending)

    def _raise_immutable(self, operation: str, instance: "Document"):
        """Raises an error when an attribute cannot be modified."""
        if self.readonly:
            raise ValueError(
                "Unable to {} readonly attribute '{}'".format(operation, self.name)
            )

        if not self.mutable and (
            instance is None or instance._attributes.get(self.name, None)
        ):
            raise ValueError(
                "Unable to {} immutable attribute '{}'".format(operation, self.name)
            )

    def deserialize(self, value) -> T:
        """Deserializes a value to the type in the attribute."""
        if value is None or isinstance(value, self.type):
            return value

        try:
            return self.type(value)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Unable to assign {type(value)} to type {self.type}: {e}")

    def serialize(self, value):
        """Serializes a value to a JSON encodable type."""
        return value

    def __repr__(self):
        return (
            "<Attribute name={} filterable={} mutable={} readonly={} sticky={}>".format(
                repr(self.name),
                self.filterable,
                self.mutable,
                self.readonly,
                self.sticky,
            )
        )


class DatetimeAttribute(Attribute):
    """Represents a datetime attribute on a document."""

    def __init__(
        self,
        timezone=None,
        remote_timezone=timezone.utc,
        default: Union[T, Callable] = None,
        mutable: bool = True,
        readonly: bool = False,
        sticky: bool = False,
        **extra,
    ):
        """Defines a datetime attribute.

        Parameters
        ----------
        timezone : timezone, None
            The timezone the client would like dates to be in.
            By default, this will used the timezone defined by the user's machine.
        remote_timezone : timezone, timezone.utc
            The timezone the server will return dates in.
            By default, this is assumed to be UTC.
        default : Any, Callable, None
            The default value for the attribute when no value is defined.
            If a callable is provided, it will be called once when the attribute is first
            fetched.
        mutable : bool, True
            If not set, the attribute will be immutable and can only be set once.
        readonly : bool, False
            If set, the attribute cannot be modified by the user.
            This is designed for attributes set and managed exclusively by the server.
        sticky : bool, False
            If set, the attribute exists on the client only.
            This attribute will be ignored when set by the server.
        """
        self.timezone = timezone
        self.remote_timezone = remote_timezone

        super().__init__(
            type=datetime,
            default=default,
            mutable=mutable,
            readonly=readonly,
            sticky=sticky,
            **extra,
        )

    def deserialize(self, value: str) -> T:
        """Deserialize a server datetime."""
        if value is None:
            return None

        if isinstance(value, (int, float)):
            value = datetime.fromtimestamp(value, tz=self.remote_timezone)

        if isinstance(value, str):
            if value.endswith("Z"):
                value = value[:-1] + "+00:00"

            value = datetime.fromisoformat(value)

        if isinstance(value, datetime):
            if not value.tzinfo:
                value.replace(tzinfo=self.remote_timezone)

            return value.astimezone(tz=self.timezone)
        else:
            raise ValueError("Expected iso formatted date or unix timestamp")

    def serialize(self, value: datetime):
        """Serialize a datetime in local time to server time in iso format."""
        if value is None:
            return value

        return value.astimezone(tz=self.remote_timezone).isoformat()
