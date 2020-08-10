import numbers
import re

from collections.abc import Iterable, Mapping, MutableMapping, MutableSequence
from datetime import datetime
from enum import Enum
from pytz import utc

from descarteslabs.common.shapely_support import (
    geometry_like_to_shapely,
    shapely_to_geojson,
)


def parse_iso_datetime(date_str):
    try:
        # Metadata timestamps allow nanoseconds, but python only allows up to
        # microseconds...  Not rounding; just truncating (for better or worse)
        if len(date_str) > 27:  # len(YYYY-MM-DDTHH:MM:SS.mmmmmmZ) == 27
            date_str = date_str[0:26] + date_str[-1]
        date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S.%fZ")
        return date.replace(tzinfo=utc)
    except ValueError:
        # it's possible that a utc formatted time string from the server won't have
        # a fractional seconds component
        date = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%SZ")
        return date.replace(tzinfo=utc)


def serialize_datetime(value):
    """Serialize a value to a json-serializable type.

    See :meth:`Attribute.serialize`.
    """
    return datetime.isoformat(value) if isinstance(value, datetime) else value


class AttributeValidationError(ValueError):
    """There was a problem validating the corresponding attribute.

    This exception indicates that the attribute value may have been required, may be
    incorrect, or cannot be serialized.
    """

    pass


class DocumentState(str, Enum):
    """The state of the catalog object.

    Attributes
    ----------
    UNSAVED : enum
        The catalog object was never synchronized with the Descartes Labs catalog.
        All values are considered modified and saving the catalog object will create
        the corresponding object in the Descartes Labs catalog.
    MODIFIED : enum
        The catalog object was synchronized with the Descartes Labs catalog (using
        :py:meth:`~descarteslabs.catalog.Product.get` or
        :py:meth:`~descarteslabs.catalog.Product.save`), but at least one
        attribute value has since been changed.  You can
        :py:meth:`~descarteslabs.catalog.Product.save` a modified catalog object
        to update the object in the Descartes Labs catalog.

        Note that assigning an identical value does not change the state.
    SAVED : enum
        The catalog object has been fully synchronized with the Descartes Labs catalog
        (using :py:meth:`~descarteslabs.catalog.Product.get` or
        :py:meth:`~descarteslabs.catalog.Product.save`).
    DELETED : enum
        The catalog object has been deleted from the Descartes Labs catalog.  Many
        operations cannot be performed on ``DELETED`` objects.

    Note
    ----
    A ``SAVED`` catalog object can still be out-of-date with respect to the Descartes
    Labs catalog if there was an update from another client since the last
    sycnronization.  To re-synchronize a ``SAVED`` catalog object you can use
    :py:meth:`~descarteslabs.catalog.Product.reload`.
    """

    SAVED = "saved"
    MODIFIED = "modified"
    UNSAVED = "unsaved"
    DELETED = "deleted"


class Attribute(object):
    """A description of an attribute as received from the Descartes Labs catalog or
    set by the end-user.

    Changing the value of an attribute will set the corresponding CatalogObject to
    modified.

    Parameters
    ----------
    mutable : bool
        Whether this attribute can be changed.  Set to ``True`` by default.  If set
        to ``False``, the attribute can be set once and after that can only be set
        with the same value.  If set with a different value, an
        `AttributeValidationError` will be raised.
    serializable : bool
        Whether this attribute will be included during serialization.  Set to ``True``
        by default.  If set to ``False``, the attribute will be skipped during
        serialization.
    sticky : bool
        Whether this attribute will be cleared when new attribute values are loaded
        from the Descartes Labs catalog.  Set to ``False`` by default.  This is used
        specifically for attributes that are only deserialised on the Descartes Labs
        catalog (`load_only`).  These attributes will never appear in the data from
        the Descartes Labs catalog, and to allow them to persist you can set the _sticky
        parameter to True.
    readonly : bool
        Whether this attribute can be set.  Set to ``False`` by default.  If set to
        ``True``, the attribute can never be set and will raise an
        `AttributeValidationError` it set.
    """

    _PARAM_MUTABLE = "mutable"
    _PARAM_SERIALIZABLE = "serializable"
    _PARAM_STICKY = "sticky"
    _PARAM_READONLY = "readonly"
    _PARAM_DOC = "doc"

    def __init__(
        self, mutable=True, serializable=True, sticky=False, readonly=False, doc=None
    ):
        self._mutable = mutable
        self._serializable = serializable
        self._sticky = sticky
        self._readonly = readonly

        if doc is not None:
            self.__doc__ = doc

    def __get__(self, obj, objtype):
        """Gets the value for this attribute on the given object."""
        # Attributes cannot be used as class properties
        if obj is None:
            raise AttributeError(
                "type object '{}' has no attribute '{}'".format(
                    objtype, self._attribute_name
                )
            )

        return obj._attributes.get(self._attribute_name)

    def __set__(self, obj, value, validate=True):
        """Sets the value for this attribute on the given object.

        Sets a value for this attribute on the given model object at the given attribute
        name, deserializing it if necessary.  Optionally indicates whether the data
        should be validated.

        Parameters
        ----------
        obj : object
            The `CatalogObject` on which to set the value.
        value : object
            The value to set on the given `CatalogObject`.  The value will be deserialized before being set.
        validate : bool
            Whether or not to check whether the value is allowed to be set and to
            validate the value itself.  ``True`` by default.

        Raises
        ------
        AttributeValidationError
            When `validate` is ``True``, and the attribute cannot be assigned to
            (readonly or immutable) or the value is invalid.
        """
        if validate:
            self._raise_if_immutable_or_readonly("set", obj)

        value = self.deserialize(value, validate)
        changed = not (
            self._attribute_name in obj._attributes
            and obj._attributes[self._attribute_name] == value
        )

        # `_set_modified()` will raise exception if change is not allowed
        obj._set_modified(self._attribute_name, changed, validate)
        obj._attributes[self._attribute_name] = value

    def __delete__(self, obj, validate=True):
        if validate:
            self._raise_if_immutable_or_readonly("delete", obj)

        obj._attributes.pop(self._attribute_name, None)

    def _get_attr_params(self, **extra_params):
        # We don't need _PARAM_DOC
        params = {
            self._PARAM_MUTABLE: self._mutable,
            self._PARAM_SERIALIZABLE: self._serializable,
            self._PARAM_STICKY: self._sticky,
            self._PARAM_READONLY: self._readonly,
        }
        if extra_params is not None:
            params.update(extra_params)
        return params

    def _raise_if_immutable_or_readonly(self, operation, obj=None):
        if self._readonly:
            raise AttributeValidationError(
                "Can't {} '{}' item because it is a readonly attribute".format(
                    operation, self._attribute_name
                )
            )
        if not self._mutable and (
            obj is None or self._attribute_name in obj._attributes
        ):
            raise AttributeValidationError(
                "Can't {} '{}' item because it is an immutable attribute".format(
                    operation, self._attribute_name
                )
            )

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        Serializes a value for this attribute to a value that can be serialized to a
        JSONAPI representation fit to send to the Descartes Labs catalog.

        Parameters
        ----------
        value : object
            Any Python object.
        jsonapi_format : bool
            Whether or not to prepend the attributes with a JSONAPI block.  ``False``
            by default.  This is only relevant for top-level catalog objects which may
            be embedded as attributes.

        Returns
        -------
        object
            Any Python object.
        """
        return value

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        Deserializes a value for this attribute from a plain python type, possibly
        generated through JSONAPI deserialization as it comes from the Descartes Labs
        catalog.  Optionally indicates whether the data should be validated.

        Parameters
        ----------
        value : object
            Any Python object
        validate : bool
            Whether or not the value should be validated.  This value is ``True`` be
            default, and this method can raise an `AttributeValidationError` in that
            case.

        Returns
        -------
        object
            Any Python object.

        Raises
        ------
        AttributeValidationError
            When `validate` is ``True`` and a validation error was encountered.
        """
        return value


class TypedAttribute(Attribute):
    """The value of the attribute is checked against the given type.

    Parameters
    ----------
    attribute_type : type
        The type of the attribute.
    coerce : bool, optional
        Whether a non-conforming value should be coerced to that type.  ``False`` by
        default.
    **kwargs : optional
        See `Attribute`.
    """

    def __init__(self, attribute_type, coerce=False, **kwargs):
        super(TypedAttribute, self).__init__(**kwargs)

        self.attribute_type = attribute_type
        self.coerce = coerce

    def __set__(self, obj, value, validate=True):
        """Assign the given value to the attribute.

        Raises
        ------
        AttributeValidationError
            If a coercion failed or if the value is not of the given type.
        """
        if self.attribute_type and value is not None:
            if self.coerce:
                try:
                    value = self.attribute_type(value)
                except ValueError as e:
                    raise AttributeValidationError(e)
            elif not isinstance(value, self.attribute_type):
                raise AttributeValidationError(
                    "The attribute type is {} for {}".format(
                        self.attribute_type, self._attribute_name
                    )
                )

        super(TypedAttribute, self).__set__(obj, value, validate)


class CatalogObjectReference(Attribute):
    """A reference to another CatalogObject.

    An attribute that holds another CatalogObject, referenced by id through another
    attribute that by convention should be the name of this attribute plus the suffix
    "_id".

    Parameters
    ----------
    reference_class : CatalogObject
        The class for the CatalogObject instance that this attribute will hold a
        reference to.
    require_unsaved : bool, optional
        Whether the reference is allowed even if the CatalogObject instance is not in
        the `SAVED` state.  ``False`` by default.
    **kwargs : optional
        See `Attribute`.
    """

    def __init__(self, reference_class, require_unsaved=False, **kwargs):
        # Serializable defaults to `False` for reference objects
        kwargs[self._PARAM_SERIALIZABLE] = kwargs.pop(self._PARAM_SERIALIZABLE, False)
        super(CatalogObjectReference, self).__init__(**kwargs)

        self.reference_class = reference_class
        self._require_unsaved = require_unsaved

    def __get__(self, obj, objtype):
        """Gets the value for this attribute on the given object.

        Access the referenced object by looking it up in related objects or else on
        the Descartes Labs catalog.  Values are cached until this attribute or the
        corresponding id field are modified.
        """
        if obj is None:
            return super(CatalogObjectReference, self).__get__(self, obj, objtype)

        cached_value = obj._attributes.get(self._attribute_name)
        reference_id = getattr(obj, self.id_field)
        if cached_value and cached_value.id == reference_id:
            return cached_value

        if reference_id:
            new_value = self.reference_class.get(reference_id, client=obj._client)
        else:
            new_value = None

        obj._attributes[self._attribute_name] = new_value
        return new_value

    def __set__(self, obj, value, validate=True):
        """Sets the value for this attribute on the given object.

        See :meth:`Attribute.__set__`.

        Sets a new referenced object.  Must be a saved object of the correct type.

        Raises
        ------
        AttributeValidationError
            If the given reference is not a CatalogObject reference, or the referred-to
            instance is `DocumentState.UNSAVED` and `require_unsaved` is ``False``,
            or the referred-to instance not in `DocumentState.UNSAVED` and
            `require_unsaved` is ``True``
        """
        if validate:
            self._raise_if_immutable_or_readonly("set", obj)

        if value is not None:
            if not isinstance(value, self.reference_class):
                raise AttributeValidationError(
                    "Expected {} instance for attribute '{}' but got '{}'".format(
                        self.reference_class.__name__, self._attribute_name, value
                    )
                )
            if not self._require_unsaved and value.state == DocumentState.UNSAVED:
                raise AttributeValidationError(
                    "Can't assign unsaved related object to '{}'. Save it first.".format(
                        self._attribute_name
                    )
                )
            elif self._require_unsaved and value.state != DocumentState.UNSAVED:
                raise AttributeValidationError(
                    "Can't assign saved related object to '{}'. Use a new unsaved object.".format(
                        self._attribute_name
                    )
                )

        changed = not (
            self._attribute_name in obj._attributes
            and obj._attributes[self._attribute_name] == value
        )

        # `_set_modified()` will raise exception if change is not allowed
        obj._set_modified(self._attribute_name, changed, validate)
        obj._attributes[self._attribute_name] = value
        # Jam in the `id`
        obj._set_modified(self.id_field, changed, validate=False)
        obj._attributes[self.id_field] = None if value is None else value.id

    @property
    def id_field(self):
        return "{}_id".format(self._attribute_name)

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        return value.serialize(modified_only=False, jsonapi_format=jsonapi_format)


class Timestamp(Attribute):
    """A datetime backed timestamp.  No validation is done."""

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        return serialize_datetime(value)

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Returns
        -------
        datetime or str
            Any Python object if `validate` is ``True``, otherwise a `datetime` instance
            representing the timestamp, typically in UTC.
        """
        if value is None or validate:
            # In this case `validate` is a misnomer because we do not want to validate
            # or deserialize datetimes set by the user on the client.
            # Validation and timestamp parsing happens on the server.
            return value
        elif isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=utc)
            else:
                return value
        else:
            try:
                return parse_iso_datetime(value)
            except ValueError:
                # Not sure what's going on, but since this came from the service,
                # don't raise an exception...
                return value


class EnumAttribute(Attribute):
    """An attribute backed by an enumeration.

    Parameters
    ----------
    enum : enum
        The enumeration that the value must confirm to.
    **kwargs : optional
        See `Attribute`.
    """

    def __init__(self, enum, **kwargs):
        super(EnumAttribute, self).__init__(**kwargs)

        if not (issubclass(enum, str) and issubclass(enum, Enum)):
            raise TypeError("EnumAttribute expects an 'Enum' with 'str' as mixin")
        self._enum_cls = enum

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        if type(value) is self._enum_cls:
            return value.value
        else:
            return value

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Returns
        -------
        str
            A string representing the enum value.

        Raises
        ------
        AttributeValidationError
            When a non-enum value is given.
        """
        if validate:
            # Validate that the value is allowed, but don't return the Enum instance
            try:
                return self._enum_cls(value).value
            except ValueError as e:
                raise AttributeValidationError(e)
        else:
            # No validation; allow values outside the enum range
            return value


class GeometryAttribute(Attribute):
    """An attribute that holds a geometry.

    Accepts geometry in a geojson-like format and always represents them as a shapely
    shape.
    """

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        return shapely_to_geojson(value)

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Returns
        -------
        shapely.geometry.base.BaseGeometry
            A shapely instance.

        Raises
        ------
        AttributeValidationError
            When the given value cannot be coerced into a geometry.
        """
        if value is None:
            return value
        else:
            try:
                return geometry_like_to_shapely(value)
            except (ValueError, TypeError) as ex:
                raise AttributeValidationError(ex)


class BooleanAttribute(Attribute):
    """An attribute with a boolean value.  Exactly like the Python bool."""

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        return bool(value)

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Returns
        -------
        bool
            The boolean value.  Note that any non-empty string, include "False" will
            return ``True``.
        """
        return bool(value)


class AttributeEqualityMixin(object):
    """Tests for equality and inequality.

    A mixin that defines equality for classes that have an Attribute dictionary property
    at `_attribute_types` and the values dictionary at `_attributes`.  Equality is
    defined as equality of all serializable attributes in serialized form.
    """

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False

        for name, attribute_type in self._attribute_types.items():
            if not attribute_type._serializable:
                continue
            if attribute_type.serialize(
                self._attributes.get(name)
            ) != attribute_type.serialize(other._attributes.get(name)):
                return False

        return True


class ModelAttribute(Attribute):
    """A class that allows for models to be registered and updated.

    Parameters
    ----------
    **kwargs : optional
        See `Attribute`.
    """

    def __init__(self, **kwargs):
        self._model_objects = {}
        super(ModelAttribute, self).__init__(**kwargs)

    def __set__(self, obj, value, validate=True):
        """Sets the value for this attribute on the given object.

        See :meth:`Attribute.__set__`.

        This will also register the model with the given `ModelAttribute` instance
        value and deregister the model from the old `ModelAttribute` instance value.

        Parameters
        ----------
        value : ModelAttribute or object
            The value will be deserialized to a `ModelAttribute`.
        """
        if validate:
            self._raise_if_immutable_or_readonly("set", obj)

        value = self.deserialize(value, validate=validate)
        previous_value = obj._attributes.get(self._attribute_name, None)

        changed = not (
            self._attribute_name in obj._attributes and previous_value == value
        )

        # `_set_modified()` will raise exception if change is not allowed
        obj._set_modified(self._attribute_name, changed, validate)

        # deregister the previous value and register the new one
        if previous_value is not None:
            previous_value._remove_model_object(obj)
        if value is not None:
            value._add_model_object(obj, self._attribute_name)

        obj._attributes[self._attribute_name] = value

    def __delete__(self, obj, validate=True):
        """Delete the value for this attribute on the given object.

        It will remove the reference to the old value.
        """
        if validate:
            self._raise_if_immutable_or_readonly("delete", obj)

        previous_value = obj._attributes.pop(self._attribute_name, None)
        if previous_value is not None:
            previous_value._remove_model_object(obj)

    def _add_model_object(self, model, attr_name=None):
        """Register a model and attribute name.

        Since we can reuse one ModelAttribute object across different
        model object types, each with potentially different attribute names,
        we register the name of the attribute on the specific model object,
        to avoid propagating bad changes.

        Parameters
        ----------
        model : CatalogObject
            The model to add to the registered models for this value instance.
        attr_name : str
            The name of the attribute in this model that this value belongs to.
        """
        id_ = id(model)
        self._model_objects[id_] = (model, attr_name)

    def _remove_model_object(self, model):
        """Deregister a model.

        Parameters
        ----------
        model : CatalogObject
            The model to remove from the registered models for this instance.
        """
        id_ = id(model)
        self._model_objects.pop(id_, None)

    def _set_modified(self, attr_name=None, changed=True, validate=True):
        """Verify change on all the referenced model objects and trigger modification.

        The model can reject this change by raising an `AttributeValidationError` if
        validate is ``True``.  If the new value is identical to the old value, the
        modification is **not** triggered (and changed will be ``False``).

        Parameters
        ----------
        attr_name : str
            The name of the attribute.  ``None`` by default.  Note that the attr_name
            argument is ignored because of the chaining.
        changed : bool
            Whether or not the actual value changed.  ``True`` by default.
        validate : bool
            Whether or not to verify that the value can be assigned.  ``True`` by
            default.

        Raises
        ------
        AttributeValidationError
            When `validate` is ``True`` and the attribute cannot be assigned to.
        """
        for model_object, attr_name in self._model_objects.values():
            model_object._set_modified(attr_name, changed, validate)


class AttributeMeta(type):
    """Apply the class attribute instances to the instance."""

    _KEY_ATTR_TYPES = "_attribute_types"
    _KEY_REF_ATTR_TYPES = "_reference_attribute_types"

    def __new__(cls, name, bases, attrs):
        types = {}
        references = {}

        # Register all declared attributes
        for attr_name, attr_type in attrs.items():
            if isinstance(attr_type, Attribute):
                types[attr_name] = attr_type
                if isinstance(attr_type, CatalogObjectReference):
                    references[attr_name] = attr_type

                # Register this attribute's name with the instance
                attr_type._attribute_name = attr_name

        # inherit attributes from base classes
        for b in bases:
            if hasattr(b, AttributeMeta._KEY_ATTR_TYPES):
                for attr_name, attr_type in b._attribute_types.items():
                    # Don't overwrite existing attrs
                    if attr_name not in types:
                        types[attr_name] = attr_type
                        if "_no_inherit" not in attrs or not attrs["_no_inherit"]:
                            # Add base attributes for documentation
                            # (sphinx doesn't inherit attrs)
                            attrs[attr_name] = attr_type
            if hasattr(b, AttributeMeta._KEY_REF_ATTR_TYPES):
                for attr_name, attr_type in b._reference_attribute_types.items():
                    # Don't overwrite existing reference attrs
                    if attr_name not in references:
                        references[attr_name] = attr_type

        attrs["ATTRIBUTES"] = tuple(types.keys())
        attrs[AttributeMeta._KEY_ATTR_TYPES] = types
        attrs[AttributeMeta._KEY_REF_ATTR_TYPES] = references

        return super(AttributeMeta, cls).__new__(cls, name, bases, attrs)


class MappingAttribute(ModelAttribute, AttributeEqualityMixin, metaclass=AttributeMeta):
    """Base class for attributes that are mapping types.


    Can be set using a mapping, or an instance of a MappingAttribute derived type.

    MappingAttributes differ from other Attribute subclasses in a few key respects:

    - MappingAttribute shouldn't ever be instantiated directly, but subclassed and the
      subclass should be instantiated
    - MappingAttribute subclasses have two "modes": they are instantiated on classes
      directly, just like the other Attribute types they're also instantiated directly
      and used in value assignments.
    - MappingAttribute subclasses keep track of their own state, rather than delegating
      this to the model object they're attached to.  This allows these objects to be
      instantiated directly without being attached to a model object, and it allows a
      single instance to be attached to multiple model objects.  Since they track their
      own state, the model objects they're attached to retain references to instances
      in their _attributes, like with other type (e.g.  datetime).

    Parameters
    ----------
    **kwargs : optional
        See `Attribute`.

    Examples
    --------
    The first way MappingAttributes are used is just like other attributes,
    they are instantiated as part of a class definition, and the instance
    is cached on the class.

    The other way mapping attributes are used is but instantiating a new instance and
    assigning that instance to a model object.
    >>> from descarteslabs.catalog.attributes import (
    ...     MappingAttribute,
    ...     Attribute,
    ... )
    >>> from descarteslabs.catalog import CatalogObject
    >>> class MyMapping(MappingAttribute):
    ...     foo = Attribute()
    >>> class ExampleCatalogObject(CatalogObject):
    ...     map_attr = MyMapping()
    >>> my_map = MyMapping(foo="bar")
    >>> obj1 = ExampleCatalogObject(map_attr=my_map)
    >>> obj2 = ExampleCatalogObject(map_attr=my_map)
    >>> assert obj1.map_attr is obj2.map_attr is my_map
    >>> my_map.foo = "baz"
    >>> assert obj1.is_modified
    >>> assert obj2.is_modified
    """

    # this value is ONLY used for for instances of the attribute that
    # are attached to class definitions. It's confusing to put this
    # instantiation into __init__, because the value is only ever set
    # from AttributeMeta.__new__, after it's already been instantiated
    _attribute_name = None

    def __init__(self, **kwargs):
        self._attributes = {}

        attr_params = {
            self._PARAM_MUTABLE: kwargs.pop(self._PARAM_MUTABLE, True),
            self._PARAM_SERIALIZABLE: kwargs.pop(self._PARAM_SERIALIZABLE, True),
            self._PARAM_STICKY: kwargs.pop(self._PARAM_STICKY, False),
            self._PARAM_READONLY: kwargs.pop(self._PARAM_READONLY, False),
            self._PARAM_DOC: kwargs.pop(self._PARAM_DOC, None),
        }
        super(MappingAttribute, self).__init__(**attr_params)

        validate = kwargs.pop("validate", True)
        for attr_name, value in kwargs.items():
            attr = (
                self.get_attribute_type(attr_name)
                if validate
                else self._attribute_types.get(attr_name)
            )
            if attr is not None:
                attr.__set__(self, value, validate=validate)

    def __repr__(self):
        """A string representation for the instance.

        The representation is broken up over multiple lines for readability.
        """
        sections = ["{}:".format(self.__class__.__name__)]
        for key, val in sorted(self._attributes.items()):
            val_sections = ["  " + v for v in repr(val).split("\n")]
            val = "\n".join(val_sections).strip() if len(val_sections) > 1 else val
            sections.append("  {}: {}".format(key, val))
        return "\n".join(sections)

    def __setattr__(self, name, value):
        """Set the value on the given attribute.

        Check that the attribute exists (unless it's a private attribute starting with
        ``_``) before setting the value.
        """
        if not name.startswith("_"):
            # Make sure it's a proper attribute
            self.get_attribute_type(name)
        super(MappingAttribute, self).__setattr__(name, value)

    def get_attribute_type(self, name):
        """Get the type definition for an attribute by name."""
        try:
            return self._attribute_types[name]
        except KeyError:
            raise AttributeError(
                "{} has no attribute {}".format(self.__class__.__name__, name)
            )

    def serialize(self, attrs, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        if attrs is None:
            return None

        if isinstance(attrs, MappingAttribute):
            # mapping attribute objects hold their own state
            data = attrs._attributes
        else:
            data = attrs

        serialized = {}
        for name, value in data.items():
            attribute_type = self.get_attribute_type(name)
            if attribute_type._serializable:
                serialized[name] = attribute_type.serialize(value)

        return serialized

    def deserialize(self, values, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Parameters
        ----------
        values : dict or MappingAttribute
            The values to use to initialize a new MappingAttribute.

        Returns
        -------
        MappingAttribute
            A `MappingAttribute` instance with the given values.

        Raises
        ------
            AttributeValidationError
                If the given value is not a `Mapping`, or the value does not
                conform to the attribute type.
        """
        if values is None:
            return None

        if isinstance(values, MappingAttribute):
            return values

        if not isinstance(values, Mapping):
            raise AttributeValidationError(
                "Expected a mapping or {} for attribute {}".format(
                    self.__class__.__name__, self._attribute_name
                )
            )
        type_ = type(self)
        return type_(validate=validate, **self._get_attr_params(**values))


class ResolutionUnit(str, Enum):
    """Valid units of measure for Resolution.

    Attributes
    ----------
    METERS :  enum
        The resolution in meters.
    DEGREES : enum
        The resolution in degrees.
    """

    METERS = "meters"
    DEGREES = "degrees"


class Resolution(MappingAttribute):
    """A spatial pixel resolution with a unit.

    For example, ``Resolution(value=60, unit=ResolutionUnit.METERS)`` represents a
    resolution of 60 meters per pixel.  You can also use a string with a value and
    unit, for example ``60m`` or ``1.2 deg.``.  The available unit designations are:

    * m, meter, meters, metre, metres
    * °, deg, degree, degrees

    Spaces between the value and unit are optional, as is a trailing period.

    Objects with resolution values can be filtered by a unitless number in which
    case the value is always in meters. For example, retrieving all bands with
    a resolution of 60 meters per pixel:

    >>> Band.search().filter(p.resolution == 60) # doctest: +SKIP

    Parameters
    ----------
    values : Mapping or str
        A mapping that either contains the `value` and `unit` key/value pairs,
        or is a string that can be parsed to a `value` and a `unit`.
    **kwargs : optional
        See `Attribute`.

    Attributes
    ----------
    value : float
        The value of the resolution.
    unit : str or ResolutionUnit
        The unit the resolution is measured in.
    """

    _pattern = re.compile(r"([-0-9.]+)\s*([a-zA-Z.°]+)")
    _unit_mapping = {
        "m": ResolutionUnit.METERS,
        "meter": ResolutionUnit.METERS,
        "metre": ResolutionUnit.METERS,
        "meters": ResolutionUnit.METERS,
        "metres": ResolutionUnit.METERS,
        "°": ResolutionUnit.DEGREES,
        "deg": ResolutionUnit.DEGREES,
        "degree": ResolutionUnit.DEGREES,
        "degrees": ResolutionUnit.DEGREES,
    }

    value = Attribute()
    unit = EnumAttribute(ResolutionUnit)

    def __init__(self, values=None, **kwargs):
        super(Resolution, self).__init__(**kwargs)

        if values is not None:
            r = self.deserialize(values)
            self.value = r.value
            self.unit = r.unit

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        # Serialize a single number as is - this supports filtering resolution
        # attributes by meters.
        if isinstance(value, numbers.Number):
            return value
        else:
            return super(Resolution, self).serialize(
                value, jsonapi_format=jsonapi_format
            )

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Parameters
        ----------
        values : dict or MappingAttribute
            The values to use to initialize a new MappingAttribute.  The two keys that
            can be used as ``value`` and ``unit``.

        Returns
        -------
        Resolution
            A `Resolution` instance with the given values.

        Raises
        ------
        AttributeValidationError
            If the value is not a `Resolution` or a mapping with a `value` and `unit`
            key, or cannot be parsed into a compatible `value` and `unit`.
        """
        if isinstance(value, str):
            match = self._pattern.match(value)
            unit = match and match.group(2).lower().rstrip(".")

            if not unit or unit not in self._unit_mapping:
                raise AttributeValidationError(
                    "The given resolution string cannot be parsed: {}".format(value)
                )

            value = {"value": float(match.group(1)), "unit": self._unit_mapping[unit]}

        return super(Resolution, self).deserialize(value, validate)


class File(MappingAttribute):
    """File definition for an Image.

    Attributes
    ----------
    href : str
        If the :py:class:`~descarteslabs.catalog.StorageState` is
        :py:attr:`~descarteslabs.catalog.StorageState.AVAILABLE`, this field is required
        and it must be a valid reference to either a JP2 or a GeoTiff file using the
        ``gs`` scheme.  If the :py:class:`~descarteslabs.catalog.StorageState` is
        :py:attr:`~descarteslabs.catalog.StorageState.REMOTE`, this field is optional
        and you can use one of the schemes ``gs``, ``http``, ``https``, ``ftp``, or
        ``ftps``; if the scheme is ``gs``, it must be a valid reference
        but can be any format.
    size_bytes : int
        Size of the file in bytes.  Required when the
        :py:class:`~descarteslabs.catalog.StorageState` is
        :py:attr:`~descarteslabs.catalog.StorageState.AVAILABLE`.
    hash : str
        The md5 hash for the given file.  Required when the
        :py:class:`~descarteslabs.catalog.StorageState` is
        :py:attr:`~descarteslabs.catalog.StorageState.AVAILABLE`.
    provider_id : str
        Optional ID for the external provider when the
        :py:class:`~descarteslabs.catalog.StorageState` is
        :py:attr:`~descarteslabs.catalog.StorageState.REMOTE`.
    provider_href : str
        A URI to describe the remote image in more detail.  Either the `provider_href`
        or the `href` must be specified when the
        :py:class:`~descarteslabs.catalog.StorageState` is
        :py:attr:`~descarteslabs.catalog.StorageState.REMOTE`.
    """

    href = Attribute()
    size_bytes = Attribute()
    hash = Attribute()
    provider_id = Attribute()
    provider_href = Attribute()


class ListAttribute(ModelAttribute, MutableSequence):
    """Base class for attributes that are lists.

    Can be set using an iterable of items.  The type is the same for all list items,
    and created automatically to hold a given deserialized value if it's not already
    that type.  The type can reject the value with a `AttributeValidationError`.

    ListAttributes behave similarly to `MappingAttributes` but provide additional
    operations that allow list-like interactions (slicing, appending, etc.)

    One major difference between ListAttributes and `MappingAttributes` is that
    ListAttributes shouldn't be subclassed or instantiated directly - it's much easier
    for users to construct and assign a list or iterable, and allow __set__ to handle
    the coercing of the values to the correct type.

    Parameters
    ----------
    attribute_type : Attribute
        All items in the ListAttribute must be of the same Attribute type.  The actual
        values must be able to be deserialized by that Attribute type.
    items : Iterable
        An iterable of items from which to construct the initial content.
    validate : bool
        Whether or not to verify whether the values are valid for the given Attribute
        type.  ``True`` be default.

    Raises
    ------
    AttributeValidationError
        If any of the items cannot be successfully deserialized to the given attribute
        type and `validate` is ``True``.

    Example
    -------
    This is the recommended way to instantiate a ListAttribute, you don't maintain a
    reference to the original list but the semantics are much cleaner.

    >>> from descarteslabs.catalog import CatalogObject, File
    >>> from descarteslabs.catalog.attributes import ListAttribute
    >>> class ExampleCatalogObject(CatalogObject):
    ...     files = ListAttribute(File)
    >>> files = [
    ...     File(href="https://foo.com/1"),
    ...     File(href="https://foo.com/2"),
    ... ]
    >>> obj = ExampleCatalogObject(files=files)
    >>> assert obj.files is not files
    """

    # this value is ONLY used for for instances of the attribute that
    # are attached to class definitions. It's confusing to put this
    # instantiation into __init__, because the value is only ever set
    # from AttributeMeta.__new__, after it's already been instantiated
    _attribute_name = None

    def __init__(self, attribute_type, validate=True, items=None, **kwargs):
        if isinstance(attribute_type, Attribute):
            self._attribute_type = attribute_type
        elif issubclass(attribute_type, Attribute):
            self._attribute_type = attribute_type(**kwargs)
        else:
            raise AttributeValidationError(
                "First argument for {} must be an Attribute type".format(
                    self.__class__.__name__
                )
            )
        self._items = []

        super(ListAttribute, self).__init__(**kwargs)

        if items is not None:
            self._items = [
                self._instantiate_item(item, validate=validate) for item in items
            ]

    def __repr__(self):
        """A string representation for this instance.

        The representation is broken up over multiple lines for readability.
        """
        sections = []
        for item in self._items:
            sections.append(repr(item))
        return "[" + ", ".join(sections) + "]"

    def _instantiate_item(self, item, validate=True, add_model=True):
        """Handles coercing the provided value to the correct type.

        Handles coercing the provided value to the correct type, optionally registers
        this instance of the ListAttribute as the model object for ModelAttribute
        item types.
        """
        item = self._attribute_type.deserialize(item, validate=validate)

        if add_model and isinstance(item, ModelAttribute):
            item._add_model_object(self)

        return item

    def serialize(self, values, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        if values is None:
            return None

        return [
            self._attribute_type.serialize(v, jsonapi_format=jsonapi_format)
            for v in values
        ]

    def deserialize(self, values, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Parameters
        ----------
        values : Iterable
            An iterator used to initialize a `ListAttribute` instance.

        Returns
        -------
        ListAttribute
            A `ListAttribute` with the given items.

        Raises
        ------
        AttributeValidationError
            If the value is not an iterable or if the value cannot be successfully
            deserialized to the given attribute type and `validate` is ``True``.
        """
        if values is None:
            return None

        if isinstance(values, ListAttribute):
            return values

        if not isinstance(values, Iterable) or isinstance(values, (str, bytes)):
            raise AttributeValidationError(
                "{} expects a non-string/bytes iterable for attribute {}, not {}".format(
                    self.__class__.__name__,
                    self._attribute_name,
                    values.__class__.__name__,
                )
            )

        # ensures subclasses are handled correctly
        type_ = type(self)
        return type_(
            self._attribute_type,
            validate=validate,
            items=values,
            **self._get_attr_params()
        )

    # MutableSequence methods

    def __getitem__(self, n):
        return self._items[n]

    def __setitem__(self, n, item):
        self._raise_if_immutable_or_readonly("set")
        previous_value = self._items[n]

        # handling slice assignment
        if isinstance(n, slice):
            try:
                iter(item)
            except TypeError:
                # mimic the error you get from the builtin
                raise TypeError("Can only assign an iterable")

            new_item = list(self._instantiate_item(o) for o in item)
        else:
            new_item = self._instantiate_item(item)

        # `_set_modified()` will raise exception if change is not allowed
        self._set_modified(changed=(previous_value != new_item))
        # will throw IndexError which is what we want if previous value isn't set
        self._items[n] = new_item

        # slicing returns a list of items
        if not isinstance(n, slice):
            previous_value = [previous_value]

        for val in previous_value:
            if isinstance(val, MappingAttribute):
                val._remove_model_object(self)

    def __delitem__(self, n):
        self._raise_if_immutable_or_readonly("delete")
        previous_value = self._items[n]

        # slicing returns a list of items
        if not isinstance(n, slice):
            previous_value = [previous_value]

        for val in previous_value:
            if isinstance(val, MappingAttribute):
                val._remove_model_object(self)

        new_items = list(self._items)
        # will throw IndexError which is what we want if previous value isn't set
        del new_items[n]

        # `_set_modified()` will raise exception if change is not allowed
        self._set_modified(changed=(self._items != new_items))
        self._items = new_items

    def __len__(self):
        return len(self._items)

    def insert(self, index, value):
        self._raise_if_immutable_or_readonly("insert")
        new_value = self._instantiate_item(value)

        # `_set_modified()` will raise exception if change is not allowed
        self._set_modified()
        self._items.insert(index, new_value)

    # Remaining Sequence methods

    def __add__(self, other):
        # emulating how concatenation works for lists
        if not isinstance(other, Iterable) or isinstance(other, (str, bytes)):
            raise TypeError(
                "{} can only concatenate non-string/bytes iterables"
                "for attribute {}, not {}".format(
                    self.__class__.__name__,
                    self._attribute_name,
                    other.__class__.__name__,
                )
            )

        # this is a shallow copy operations, so we don't attach the new item to this
        # model object
        new_other = [self._instantiate_item(o, add_model=False) for o in other]
        return self._items + new_other

    def __mul__(self, other):
        return self._items * other

    def __imul__(self, other):
        # `_set_modified()` will raise exception if change is not allowed
        self._set_modified(changed=(self._items and other != 1))
        self._items *= other
        return self

    def __rmul__(self, other):
        return self._items * other

    def copy(self):
        """Return a shallow copy of the list."""
        return self._items.copy()

    def sort(self, key=None, reverse=False):
        self._raise_if_immutable_or_readonly("sort")

        """Stable sort *IN PLACE*."""
        new_items = list(self._items)
        new_items.sort(key=key, reverse=reverse)

        # `_set_modified()` will raise exception if change is not allowed
        self._set_modified(changed=(self._items != new_items))
        self._items = new_items

    # Comparison methods

    def __eq__(self, other):
        if self is other:
            return True

        if not isinstance(other, (self.__class__, Iterable)):
            return False

        if len(self) != len(other):
            return False

        for (i1, i2) in zip(self, other):
            if i1 != i2:
                return False

        return True

    def __ge__(self, other):
        if isinstance(other, self.__class__):
            other = other._items

        # allow list __ge__ to raise/return
        return self._items >= other

    def __gt__(self, other):
        if isinstance(other, self.__class__):
            other = other._items

        # allow list __gt__ to raise/return
        return self._items > other

    def __le__(self, other):
        if isinstance(other, self.__class__):
            other = other._items

        # allow list __le__ to raise/return
        return self._items <= other

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            other = other._items

        # allow list __lt__ to raise/return
        return self._items < other


class ExtraPropertiesAttribute(ModelAttribute, MutableMapping):
    """An attribute that contains properties (key/value pairs).

    Can be set using a dictionary of items or any `Mapping`, or an instance of this
    attribute.  All keys must be string and values can be string or numbers.
    ExtraPropertiesAttribute behaves similar to dictionaries.

    Example
    -------
    This is the recommended way to instantiate a ExtraPropertiesAttribute, you don't
    maintain a reference to the original list but the semantics are much cleaner.

    >>> from descarteslabs.catalog import CatalogObject
    >>> from descarteslabs.catalog.attributes import ExtraPropertiesAttribute
    >>> class ExampleCatalogObject(CatalogObject):
    ...     extra_properties = ExtraPropertiesAttribute()
    >>> properties = {
    ...     "prop1": "value1",
    ...     "prop2": "value2",
    ... }
    >>> obj = ExampleCatalogObject(extra_properties=properties)
    >>> assert obj.extra_properties is not properties
    >>> obj.extra_properties["prop3"] = "value3"
    """

    # this value is ONLY used for for instances of the attribute that
    # are attached to class definitions. It's confusing to put this
    # instantiation into __init__, because the value is only ever set
    # from AttributeMeta.__new__, after it's already been instantiated
    _attribute_name = None

    def __init__(self, value=None, validate=True, **kwargs):
        self._items = {}

        super(ExtraPropertiesAttribute, self).__init__(**kwargs)

        if value is not None:
            if validate:
                for key, val in value.items():
                    self.validate_key_and_value(key, val)

            self._items.update(value)

    def __repr__(self):
        return "{}{}{}".format(
            "{",
            ", ".join(
                [
                    "{}: {}".format(repr(key), repr(value))
                    for key, value in self._items.items()
                ]
            ),
            "}",
        )

    def validate_key_and_value(self, key, value):
        """Validate the key and value.

        The key must be a string, and the value either a string or a number.
        """
        if not isinstance(key, str):
            raise AttributeValidationError(
                "Keys for property {} must be strings: {}".format(
                    self._attribute_name, key
                )
            )
        elif not isinstance(value, (str, int, float)):
            raise AttributeValidationError(
                "The value for property {} with key {} must be a string or a number: {}".format(
                    self._attribute_name, key, value
                )
            )

    def serialize(self, value, jsonapi_format=False):
        """Serialize a value to a json-serializable type.

        See :meth:`Attribute.serialize`.
        """
        if value is None:
            return None

        # Shallow copy
        return dict(value._items)

    def deserialize(self, value, validate=True):
        """Deserialize a value to a native type.

        See :meth:`Attribute.deserialize`.

        Parameters
        ----------
        value : dict or ExtraPropertiesAttribute
            A set of values to use to initialize a new ExtraPropertiesAttribute
            instance.  All keys must be strings, and values can be strings or numbers.

        Returns
        -------
        ExtraPropertiesAttribute
            A `ExtraPropertiesAttribute` with the given items.

        Raises
        ------
        AttributeValidationError
            If the value is not a mapping or any of the keys are not strings, or any
            of the values are not strings or numbers.
        """
        if value is None:
            return None

        if isinstance(value, ExtraPropertiesAttribute):
            return value

        if validate:
            if not isinstance(value, Mapping):
                raise AttributeValidationError(
                    "A ExtraPropertiesAttribute expects a mapping: {}".format(
                        self._attribute_name
                    )
                )

            for key, val in value.items():
                self.validate_key_and_value(key, val)

        return ExtraPropertiesAttribute(
            value, validate=validate, **self._get_attr_params()
        )

    # Mapping methods

    def __getitem__(self, key):
        return self._items[key]

    def __setitem__(self, key, value):
        self._raise_if_immutable_or_readonly("set")
        self.validate_key_and_value(key, value)

        old_value = self._items.get(key, None)
        changed = key not in self._items or old_value != value
        self._set_modified(changed=changed)
        self._items[key] = value

    def __delitem__(self, key):
        self._raise_if_immutable_or_readonly("delete")
        if key in self._items:
            self._set_modified(changed=True)
        del self._items[key]

    def __iter__(self):
        return iter(self._items)

    def __len__(self):
        return len(self._items)


class TupleAttribute(Attribute):
    """An attribute that represents a tuple.

    The minimum and maximum size of the tuple can be specified.  If the minimum and
    maximum size are identical, the tuple must be exactly that size.

    Parameters
    ----------
    attribute_type : type, optional
        Each item in the tuple must be of the given type.
    coerce : bool, optional
        If an `attribute_type` is given, whether a non-conforming value should be
        coerced to that type.  ``False`` by default.
    min : int, optional
        The minimum number of items the tuple must contain.  If not set, there is no
        minimum.
    max : int, optional
        The maximum number of items the table can contain.  If not set, there is no
        maximum.
    **kwargs : optional
        See `Attribute`.

    Raises
    ------
    ValueError
        If a coercion failed.
    AttributeValidationError
        If the value doesn't confirm to the tuple specification.
    """

    def __init__(
        self,
        attribute_type=None,
        coerce=False,
        min_length=None,
        max_length=None,
        **kwargs
    ):
        super(TupleAttribute, self).__init__(**kwargs)

        self.attribute_type = attribute_type
        self.coerce = coerce
        self.min_length = None if min_length is None else int(min_length)
        self.max_length = None if max_length is None else int(max_length)

    def __set__(self, obj, value, validate=True):
        """Sets the value for this attribute on the given object.

        See :meth:`Attribute.__set__`.

        Raises
        ------
        AttributeValidationError
            If the value cannot be coerced into a tuple, or if any of the tuple items
            cannot be coerced into, or match the given attribute type, or if the tuple
            length does not confirm to the given min_length and max_length.
        """
        # Make sure it's a tuple
        if isinstance(value, Iterable) and not isinstance(value, (str, bytes, tuple)):
            value = tuple(value)

        if validate:
            value = self.validate_value(value)

        super(TupleAttribute, self).__set__(obj, value, validate)

    def validate_value(self, value):
        # Validate the value, optionally coercing it, and return the value
        if not isinstance(value, tuple):
            raise AttributeValidationError(
                "You must specify a tuple for {}".format(self._attribute_name)
            )

        if self.attribute_type:
            if self.coerce:
                items = []
            for item in value:
                if self.coerce:
                    try:
                        item = self.attribute_type(item)
                    except ValueError as e:
                        raise AttributeValidationError(e)
                    items.append(item)
                elif not isinstance(item, self.attribute_type):
                    raise AttributeValidationError(
                        "Not all items are of type {} for {}".format(
                            self.attribute_type, self._attribute_name
                        )
                    )
            if self.coerce:
                value = tuple(items)

        if (
            self.min_length is not None
            and self.min_length == self.max_length
            and len(value) != self.min_length
        ):
            raise AttributeValidationError(
                "Tuple must contain exactly {} items for {}".format(
                    self.min_length, self._attribute_name
                )
            )

        if self.min_length is not None and len(value) < self.min_length:
            raise AttributeValidationError(
                "Tuple must contain at least {} items for {}".format(
                    self.min_length, self._attribute_name
                )
            )

        if self.max_length is not None and len(value) < self.max_length:
            raise AttributeValidationError(
                "Tuple can contain up to {} items for {}".format(
                    self.max_length, self._attribute_name
                )
            )

        return value
