from enum import Enum
from datetime import datetime
from pytz import utc
import numbers

from six import add_metaclass, PY2, PY3, iteritems, itervalues
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
        :py:meth:`~descarteslabs.catalog.CatalogObject.get` or
        :py:meth:`~descarteslabs.catalog.CatalogObject.save`), but at least one
        attribute value has since been changed.  You can
        :py:meth:`~descarteslabs.catalog.CatalogObject.save` a modified catalog object
        to update the object in the Descartes Labs catalog.
    SAVED : enum
        The catalog object has been fully synchronized with the Descartes Labs catalog
        (using :py:meth:`~descarteslabs.catalog.CatalogObject.get` or
        :py:meth:`~descarteslabs.catalog.CatalogObject.save`).
    DELETED : enum
        The catalog object has been deleted from the Descartes Labs catalog.  Many
        operations cannot be performed on ``DELETED`` objects.

    Note
    ----
    A ``SAVED`` catalog object can still be out-of-date with respect to the Descartes
    Labs catalog if there was an update from another client since the last
    sycnronization.  To re-synchronize a ``SAVED`` catalog object you can use
    :py:meth:`~descarteslabs.catalog.CatalogObject.reload`.
    """

    SAVED = "saved"
    MODIFIED = "modified"
    UNSAVED = "unsaved"
    DELETED = "deleted"


class Attribute(object):
    """A description of an attribute as received from the Descartes Labs catalog or
    set by the end-user.

    Parameters
    ----------
    _mutable : bool
        Whether this attribute can be changed.
        Set to True by default.
        If set to False, the attribute can be set once and after that can only be
        set with the same value. If set with a different value, an
        `AttributeValidationError` will be thrown.
    _serializable : bool
        Whether this attribute will be included during serialization.
        Set to True by default.
        If set to False, the attribute will be skipped during serialized and will
        throw an AttributeValidationError if serialized explicitly using
        `serialize_attribute_for_filter`.
    _sticky : bool
        Whether this attribute will be cleared when new attribute values are loaded
        from the Descartes Labs catalog.  Set fo False by default.  This is used
        specifically for attributes that are only deserialised on the Descartes Labs
        catalog (`load_only`).  These attributes will never appear in the data from
        the Descartes Labs catalog, and to allow them to persist you can set the _sticky
        parameter to True.
    """

    def __init__(self, _mutable=True, _serializable=True, _sticky=False):
        self._mutable = _mutable
        self._serializable = _serializable
        self._sticky = _sticky

    def serialize(self, value, jsonapi_format=False):
        """
        Serializes a value for this attribute to a JSONAPI representation fit to
        send to the Descartes Labs catalog.
        """
        return value

    def deserialize(self, value, validate=True):
        """
        Deserializes a value for this attribute from a JSONAPI representation as it
        comes from the Descartes Labs catalog.  Optionally indicates whether the data
        should be validated.
        """
        return value

    def __get__(self, obj, objtype):
        """
        Gets the value for this attribute on the given object.
        """
        # Attributes cannot be used as class properties
        if obj is None:
            raise AttributeError(
                "type object '{}' has no attribute '{}'".format(
                    objtype, self._attribute_name
                )
            )

        return obj._attributes.get(self._attribute_name)

    def __set__(self, obj, value, validate=True):
        """
        Sets a value for this attribute on the given model object at the
        given attribute name, deserializing it if necessary.  Optionally
        indicates whether the data should be validated.
        """
        if (
            not self._mutable
            and validate
            and self._attribute_name in obj._attributes
            and obj._attributes[self._attribute_name] != value
        ):
            raise AttributeValidationError(
                "Can't set '{}' to {} because it is an immutable attribute".format(
                    self._attribute_name, value
                )
            )

        obj._attributes[self._attribute_name] = self.deserialize(value, validate)
        obj._set_modified(self._attribute_name)


class CatalogObjectReference(Attribute):
    """
    An attribute that holds another CatalogObject, referenced by id through
    another attribute that by convention should be the name of this attribute
    plus the suffix "_id".
    """

    def __init__(
        self,
        reference_class,
        _mutable=True,
        _serializable=False,
        _allow_unsaved=False,
        _sticky=False,
    ):
        super(CatalogObjectReference, self).__init__(
            _mutable=_mutable, _serializable=_serializable, _sticky=_sticky
        )

        self.reference_class = reference_class
        self._allow_unsaved = _allow_unsaved

    @property
    def id_field(self):
        return "{}_id".format(self._attribute_name)

    def serialize(self, value, jsonapi_format=False):
        """
        Serializes a value for this attribute to a JSONAPI representation fit to
        send to the Descartes Labs catalog.
        """
        return value.serialize(modified_only=False, jsonapi_format=jsonapi_format)

    def __get__(self, model_object, objtype):
        """
        Access the referenced object by looking it up in related objects or else on
        the Descartes Labs catalog.  Values are cached until this attribute or the
        corresponding id field are modified.
        """
        if model_object is None:
            return super(CatalogObjectReference, self).__get__(
                self, model_object, objtype
            )

        cached_value = model_object._attributes.get(self._attribute_name)
        reference_id = getattr(model_object, self.id_field)
        if cached_value and cached_value.id == reference_id:
            return cached_value

        if reference_id:
            new_value = self.reference_class.get(
                reference_id, client=model_object._client
            )
        else:
            new_value = None

        model_object._attributes[self._attribute_name] = new_value
        return new_value

    def __set__(self, model_object, value, validate=True):
        """
        Sets a new referenced object. Must be a saved object of the correct
        type.
        """
        if value is not None:
            if not isinstance(value, self.reference_class):
                raise AttributeValidationError(
                    "Expected {} instance for attribute '{}' but got '{}'".format(
                        self.reference_class.__name__, self._attribute_name, value
                    )
                )
            if not self._allow_unsaved and value.state == DocumentState.UNSAVED:
                raise AttributeValidationError(
                    "Can't assign unsaved related object to '{}'. Save it first.".format(
                        self._attribute_name
                    )
                )

        model_object._attributes[self._attribute_name] = value
        setattr(model_object, self.id_field, None if value is None else value.id)

        model_object._set_modified(self._attribute_name)


class Timestamp(Attribute):
    def deserialize(self, value, validate=True):
        if value is None or validate:
            # in this case `validate` is a misnomer because we do not want to validate or
            # deserialize datetimes set by the user on the client.
            # validation and timestamp parsing happens on the server.
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
                raise AttributeValidationError(
                    "{} is not a valid value for a DateTime attribute."
                    " Value must match format '%Y-%m-%dT%H:%M:%S.%fZ'".format(value)
                )

    def serialize(self, value, jsonapi_format=False):
        return serialize_datetime(value)


class ImmutableTimestamp(Timestamp):
    def __init__(self):
        super(ImmutableTimestamp, self).__init__(_mutable=False)


class EnumAttribute(Attribute):
    def __init__(self, enum):
        super(EnumAttribute, self).__init__()

        if not (issubclass(enum, str) and issubclass(enum, Enum)):
            raise TypeError("EnumAttribute expects an 'Enum' with 'str' as mixin")
        self._enum_cls = enum

    def serialize(self, value, jsonapi_format=False):
        if type(value) is self._enum_cls:
            return value.value
        else:
            return value

    def deserialize(self, value, validate=True):
        if validate:
            # Validate that the value is allowed, but don't return the Enum instance
            return self._enum_cls(value).value
        else:
            # No validation; allow values outside the enum range
            return value


class GeometryAttribute(Attribute):
    """
    Accepts geometry in a geojson-like format and always represents them as a shapely
    shape.
    """

    def deserialize(self, value, validate=True):
        if value is None:
            return value
        else:
            try:
                return geometry_like_to_shapely(value)
            except (ValueError, TypeError) as ex:
                raise AttributeValidationError(ex)

    def serialize(self, value, jsonapi_format=False):
        return shapely_to_geojson(value)


class BooleanAttribute(Attribute):
    def deserialize(self, value, validate=True):
        return bool(value)

    def serialize(self, value, jsonapi_format=False):
        return bool(value)


class AttributeEqualityMixin(object):
    """
    A mixin that defines equality for classes that have an Attribute dictionary
    property at `_attribute_types` and the values dictionary at `_attributes`.
    Equality is defined as equality of all serializable attributes in serialized
    form.
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

    def __ne__(self, other):
        return not self.__eq__(other)

    if PY2:

        def __hash__(self):
            raise TypeError("unhashable type: '{}'".format(self.__class__.__name__))


class AttributeMeta(type):
    def __new__(cls, name, bases, attrs):
        attrs["_attribute_types"] = {}
        attrs["_reference_attribute_types"] = {}

        # register all declared attributes
        for attr_name, value in attrs.items():
            if isinstance(value, Attribute):
                attrs["_attribute_types"][attr_name] = value
                if isinstance(value, CatalogObjectReference):
                    attrs["_reference_attribute_types"][attr_name] = value

                # register this attribute's name with the instance
                value._attribute_name = attr_name

        # inherit attributes from base classes
        for b in bases:
            if hasattr(b, "_attribute_types"):
                attrs["_attribute_types"].update(b._attribute_types)
            if hasattr(b, "_reference_attribute_types"):
                attrs["_reference_attribute_types"].update(b._reference_attribute_types)

        attrs["ATTRIBUTES"] = tuple(attrs["_attribute_types"].keys())
        return super(AttributeMeta, cls).__new__(cls, name, bases, attrs)


@add_metaclass(AttributeMeta)
class MappingAttribute(Attribute, AttributeEqualityMixin):
    """
    Base class for attributes that are mapping types. Can be set using
    a dict, or an instance of a MappingAttribute derived type.

    MappingAttributes differ from other Attribute subclasses in a few key respects:
    - MappingAttribute shouldn't ever be instantiated directly, but subclassed
      and the subclass should be instantiated
    - MappingAttribute subclasses have two "modes": they are instantiated on classes directly,
      just like the other Attribute types they're also instantiated directly and used in
      value assignments.
    - MappingAttribute subclasses keep track of their own state, rather than delegating
      this to the model object they're attached to. This allows these objects to be instantiated
      directly without being attached to a model object, and it allows a single instance to be
      attached to multiple model objects Since, they track their own state, the model
      objects they're attached to retain references to instances in their _attributes,
      like with other type (e.g. datetime).

    Examples
    --------
    The first way MappingAttributes are used is just like other attributes,
    they are instantiated as part of a class definition, and the instance
    is cached on the class.

    >>> class MyMapping(MappingAttribute):
    ...     foo = Attribute()

    >>> class FakeCatalogObject(CatalogObject):
    ...     map_attr = MyMapping()

    The other way mapping attributes are used is but instantiating a new
    instance and assigning that instance to a model object.

    >>> my_map = MyMapping(foo="bar")
    >>> obj1 = FakeCatalogObject(map_attr=my_map)
    >>> obj2 = FakeCatalogObject(map_attr=my_map)
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
        _sticky = kwargs.pop("_sticky", False)
        _mutable = kwargs.pop("_mutable", True)
        super(MappingAttribute, self).__init__(_sticky=_sticky, _mutable=_mutable)

        self._model_objects = {}

        validate = kwargs.pop("validate", True)
        self._attributes = {}
        for attr_name, value in iteritems(kwargs):
            attr = (
                self.get_attribute_type(attr_name)
                if validate
                else self._attribute_types.get(attr_name)
            )
            if attr is not None:
                attr.__set__(self, value, validate=validate)

    def __repr__(self):
        sections = ["{}:".format(self.__class__.__name__)]
        for key, val in sorted(self._attributes.items()):
            val_sections = ["  " + v for v in repr(val).split("\n")]
            val = "\n".join(val_sections).strip() if len(val_sections) > 1 else val
            sections.append("  {}: {}".format(key, val))
        return "\n".join(sections)

    def __setattr__(self, name, value):
        if not name.startswith("_"):
            # Make sure it's a proper attribute
            self.get_attribute_type(name)
        super(MappingAttribute, self).__setattr__(name, value)

    def get_attribute_type(self, name):
        """
        Get the type definition for an attribute by name.
        """
        try:
            return self._attribute_types[name]
        except KeyError:
            raise AttributeError(
                "{} has no attribute {}".format(self.__class__.__name__, name)
            )

    def _add_model_object(self, model_object, attr_name):
        """
        Register a model object and attribute name.

        Since we can reuse one MappingAttribute object across different
        model object types, each with potentially different attribute names,
        we register the name of the attribute on the specific model object,
        to avoid propagating bad changes.
        """
        id_ = id(model_object)
        self._model_objects[id_] = (model_object, attr_name)

    def _remove_model_object(self, model_object):
        """
        Deregister a model object.
        """
        id_ = id(model_object)
        self._model_objects.pop(id_, None)

    def _set_modified(self, attr_name=None):
        """
        Trigger modifications on all the referenced model objects.

        Attributes expect to provide an argument to this function, but we ignore
        it in the case of List/MappingAttribute types because they retain
        references to the names of attributes on objects they're attached to.
        """
        for model_object, attr_name in itervalues(self._model_objects):
            model_object._set_modified(attr_name)

    def __get__(self, model_object, objtype):
        """
        Get the value from the model object.
        """
        return model_object._attributes.get(self._attribute_name)

    def __set__(self, model_object, value, validate=True):
        """
        Set the value for the model object.

        Allows setting using either a dict or from an already instantiated
        MappingAttribute.
        """
        if not self._mutable and validate:
            raise AttributeValidationError("Cannot set {} because it is immutable.")

        value = self.deserialize(value, validate=validate)

        # deregister the previous value we're replacing, if it exists
        previous_value = model_object._attributes.pop(self._attribute_name, None)
        if previous_value is not None:
            previous_value._remove_model_object(model_object)

        # register the new value with bidirectional references
        if isinstance(value, MappingAttribute):
            # value could be None
            value._add_model_object(model_object, self._attribute_name)
        model_object._attributes[self._attribute_name] = value
        model_object._set_modified(self._attribute_name)

    def serialize(self, attrs, jsonapi_format=False):
        """
        Serialize the provided values to a JSON-serializable dict based on the class
        definition for this MappingAttribute.
        """
        if attrs is None:
            return None

        if isinstance(attrs, MappingAttribute):
            # mapping attribute objects hold their own state
            data = attrs._attributes
        else:
            data = attrs

        serialized = {}
        for name, value in iteritems(data):
            attribute_type = self.get_attribute_type(name)
            if attribute_type._serializable:
                serialized[name] = attribute_type.serialize(value)

        return serialized

    def deserialize(self, values, validate=True):
        """
        Deserialize the provided JSON-deserialized dict to a MappingAttribute
        based on the class definition.

        If the provided values are already a MappingAttribute, that value is
        returned.
        """
        if values is None:
            return None

        if isinstance(values, MappingAttribute):
            return values

        if not isinstance(values, dict):
            raise AttributeValidationError(
                "Expected a dict or {} for attribute {}".format(
                    self.__class__.__name__, self._attribute_name
                )
            )
        type_ = type(self)
        return type_(validate=validate, _mutable=self._mutable, **values)


class ResolutionUnit(str, Enum):
    """
    Valid units of measure for Resolution.

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
    """
    A spatial pixel resolution with a unit. For example,
    ``Resolution(value=60, unit=ResolutionUnit.METERS)`` represents a resolution
    of 60 meters per pixel.

    Objects with resolution values can be filtered by a unitless number in which
    case the value is always in meters. For example, retrieving all bands with
    a resolution of 60 meters per pixel:

    >>> Band.search().filter(p.resolution == 60)

    Attributes
    ----------
    value : float
        Required: The value of the resolution.
    unit : str
        Required: The unit the resolution is measured in.
    """

    value = Attribute()
    unit = EnumAttribute(ResolutionUnit)

    def serialize(self, value, jsonapi_format=False):
        # Serialize a single number as is - this supports filtering resolution
        # attributes by meters.
        if isinstance(value, numbers.Number):
            return value
        else:
            return super(Resolution, self).serialize(
                value, jsonapi_format=jsonapi_format
            )


class File(MappingAttribute):
    """
    File definition for an Image.

    Attributes
    ----------
    href : str
        A valid reference to a file object using one of the schemas ``gs``, ``http``,
        ``https``, ``ftp``, or ``ftps``.  Required when the
        :py:class:`~descarteslabs.catalog.StorageState` is
        :py:attr:`~descarteslabs.catalog.StorageState.AVAILABLE`.  Optional otherwise.
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


class ListAttribute(Attribute):
    """
    Base class for attributes that are lists. Can be set using
    a list of items, or an instance of a List derived type.

    ListAttributes behave similarly to MappingAttributes but provide additional operations
    that allow list-like interactions (slicing, appending, etc.)

    One major difference between ListAttributes and MappingAttributes is that ListAttributes
    shouldn't be subclassed or instantiated directly - it's much easier for users to construct
    and assign a list, and allow __set__ handle the coercing the values to the correct type.

    Example
    -------
    This is the recommended way to instantiate a ListAttribute, you don't maintain a
    reference to the original list but the semantics are much cleaner.
    >>> class FakeCatalogObject(CatalogObject):
    ...     files = ListAttribute(File)
    >>> files = [
    ...     File(href="https://foo.com/1"),
    ...     File(href="https://foo.com/2"),
    ... ]
    >>> obj = FakeCatalogObject(files=files)
    >>> assert obj.files is not files
    """

    # this value is ONLY used for for instances of the attribute that
    # are attached to class definitions. It's confusing to put this
    # instantiation into __init__, because the value is only ever set
    # from AttributeMeta.__new__, after it's already been instantiated
    _attribute_name = None

    def __init__(self, item_cls, validate=True, items=None, _mutable=True):
        super(ListAttribute, self).__init__(_mutable=_mutable)

        self._model_objects = {}

        # ensure we can deserilize data correctly
        if not issubclass(item_cls, Attribute):
            raise AttributeValidationError("expected an Attribute type")

        self._item_cls = item_cls
        self._item_type = item_cls(_mutable=_mutable)

        if items is None:
            items = []
        self._items = [
            self._instantiate_item(item, validate=validate) for item in items
        ]

    def _raise_if_immutable(self, operation="set"):
        if not self._mutable:
            raise AttributeValidationError(
                "Can't {} '{}' item because it is an immutable attribute".format(
                    operation, self._attribute_name
                )
            )

    def _instantiate_item(self, item, validate=True, add_model=True):
        """
        Handles coercing the provided value to the correct type, optionally
        registers this instance of the ListAttribute as the model object for
        MappingAttribute item types.
        """
        if isinstance(self._item_type, MappingAttribute):
            # create a new instance
            if not isinstance(item, MappingAttribute):
                item = self._item_cls(validate=validate, **item)

            if add_model:
                # no attribute name is provided because the object are
                # accessed by index on this ListAttribute's _items
                item._add_model_object(self, None)

        return item

    def _add_model_object(self, model_object, attr_name):
        """
        Register a model object and attribute name.

        Since we can reuse one MappingAttribute object across different
        model object types, each with potentially different attribute names,
        we register the name of the attribute on the specific model object,
        to avoid propagating bad changes.
        """
        id_ = id(model_object)
        self._model_objects[id_] = (model_object, attr_name)

    def _remove_model_object(self, model_object):
        id_ = id(model_object)
        try:
            del self._model_objects[id_]
        except KeyError:
            pass

    def _set_modified(self, attr_name=None):
        """
        Trigger modifications on all the referenced model objects.

        Attributes expect to provide an argument to this function, but we ignore
        it in the case of List/MappingAttribute types because they retain
        references to the names of attributes on objects they're attached to.
        """
        for model_object, attr_name in itervalues(self._model_objects):
            model_object._set_modified(attr_name)

    def __set__(self, model_object, value, validate=True):
        if validate:
            self._raise_if_immutable()

        value = self.deserialize(value, validate=validate)

        # deregister the model object from the attribute we're replacing
        # if it exists
        previous_value = model_object._attributes.pop(self._attribute_name, None)
        if previous_value is not None:
            previous_value._remove_model_object(model_object)

        # register the new attribute with the correct name
        if isinstance(value, ListAttribute):
            value._add_model_object(model_object, self._attribute_name)
        model_object._attributes[self._attribute_name] = value
        model_object._set_modified(self._attribute_name)

    def deserialize(self, values, validate=True):
        """
        Deserialize the provided JSON-deserialized dict to a ListAttribute
        based on the class definition.

        If the provided values are already a ListAttribute, that value is
        returned.
        """
        if values is None:
            return None

        if isinstance(values, ListAttribute):
            return values

        if not isinstance(values, list):
            raise AttributeValidationError(
                "Expected a list or {} for attribute {}".format(
                    self.__class__.__name__, self._attribute_name
                )
            )

        # ensures subclasses are handled correctly
        type_ = type(self)
        return type_(
            self._item_cls, validate=validate, items=values, _mutable=self._mutable
        )

    def serialize(self, values, jsonapi_format=False):
        """
        Serialize the provided values to a JSON-serializable list based on the class definition for this
        ListAttribute.
        """
        if values is None:
            return None

        return [
            self._item_type.serialize(v, jsonapi_format=jsonapi_format) for v in values
        ]

    # list methods

    def append(self, item):
        """ Append object to the end of the list. """
        self._raise_if_immutable(operation="append")

        value = self._instantiate_item(item)

        self._items.append(value)

        self._set_modified()

    # these two methods only appear on py3 lists
    if PY3:

        def clear(self):
            """Remove all items from list."""
            self._raise_if_immutable(operation="clear")

            del self[:]

            self._set_modified()

        def copy(self):
            """Return a shallow copy of the list."""
            return self._items.copy()

    def count(self, value):
        """Return number of occurrences of value."""
        new_value = self._instantiate_item(value, add_model=False)
        return self._items.count(new_value)

    def extend(self, other):
        """Extend list by appending elements from the iterable."""
        self._raise_if_immutable(operation="extend")

        new_others = (self._instantiate_item(o) for o in other)
        self._items.extend(new_others)

        self._set_modified()

    def index(self, value, *args):
        """Return first index of value.

        Raises ValueError if the value is not present.
        """
        # I don't like using varargs here because help(list.index) lists the signature as
        # index(...) on python 2 and
        # index(value, start=0, stop=9223372036854775807, /) on python 3
        # in all cases, all parameters are positional only, and varars seems like the cleanest
        # way to continue that in the absence of "/" notation
        new_value = self._instantiate_item(value)
        return self._items.index(new_value, *args)

    def insert(self, index, value):
        """Insert object before index."""
        self._raise_if_immutable(operation="insert")

        new_value = self._instantiate_item(value)
        self._items.insert(index, new_value)

        self._set_modified()

    def pop(self, index=-1):
        """
        Remove and return item at index (default last).

        Raises IndexError if list is empty or index is out of range.
        """
        self._raise_if_immutable(operation="pop")
        popped = self._items.pop(index)

        if isinstance(popped, MappingAttribute):
            popped._remove_model_object(self)

        self._set_modified()

        return popped

    def remove(self, value):
        """Remove first occurrence of value.

        Raises ValueError if the value is not present.
        """
        self._raise_if_immutable(operation="remove")
        i = self.index(value)
        self.pop(index=i)  # will set_modified

    def reverse(self):
        """Reverse *IN PLACE*."""
        self._raise_if_immutable(operation="reverse")

        self._items.reverse()

        self._set_modified()

    # sort changed signatures between py2 and py3
    if PY2:

        def sort(self, cmp=None, key=None, reverse=False):
            """Stable sort *IN PLACE*."""
            self._raise_if_immutable(operation="sort")

            self._items.sort(cmp=cmp, key=key, reverse=reverse)

            self._set_modified()

    else:

        def sort(self, key=None, reverse=False):
            """Stable sort *IN PLACE*."""
            self._raise_if_immutable(operation="sort")

            self._items.sort(key=key, reverse=reverse)

            self._set_modified()

    def __getitem__(self, n):
        return self._items[n]

    def __setitem__(self, n, item):
        # will throw IndexError which is what we want
        # if previous value isn't set
        self._raise_if_immutable(operation="__setitem__")

        previous_value = self._items[n]

        self._raise_if_immutable

        # handling slice assignment
        if isinstance(n, slice):
            try:
                iter(item)
            except TypeError:
                # mimic the error you get from the builtin
                raise TypeError("can only assign an iterable")

            new_item = (self._instantiate_item(o) for o in item)
        else:
            new_item = self._instantiate_item(item)

        self._items[n] = new_item

        # slicing returns a list of items
        if not isinstance(n, slice):
            previous_value = [previous_value]

        for val in previous_value:
            if isinstance(val, MappingAttribute):
                val._remove_model_object(self)

        self._set_modified()

    def __delitem__(self, n):
        # will throw IndexError which is what we want
        # if previous value isn't set
        self._raise_if_immutable(operation="__delitem__")
        previous_value = self._items[n]

        # slicing returns a list of items
        if not isinstance(n, slice):
            previous_value = [previous_value]

        for val in previous_value:
            if isinstance(val, MappingAttribute):
                val._remove_model_object(self)

        del self._items[n]

        self._set_modified()

    def __len__(self):
        return len(self._items)

    def __add__(self, other):
        # emulating how concatenation works for lists
        if not isinstance(other, (list, ListAttribute)):
            raise TypeError(
                'can only concatenate list (not "{}") to list'.format(
                    other.__class__.__name__
                )
            )

        # this is a shallow copy operations, so we don't attach the new item to this
        # model object
        new_other = [self._instantiate_item(o, add_model=False) for o in other]
        return self._items + new_other

    def __contains__(self, value):
        new_value = self._instantiate_item(value)
        return new_value in self._items

    def __iadd__(self, other):
        # this raises a TypeError which mimics normal list behavior when given
        # a non-iterable "other"
        self._raise_if_immutable(operation="__iadd__")

        new_other = (self._instantiate_item(o) for o in other)
        self._items += new_other

        self._set_modified()
        return self

    def __imul__(self, other):
        self._raise_if_immutable(operation="__imul__")

        self._items *= other

        self._set_modified()

        return self

    def __iter__(self):
        return iter(self._items)

    def __mul__(self, other):
        return self._items * other

    def __repr__(self):
        sections = []
        for item in self._items:
            sections.append(repr(item))
        return "[" + ",\n".join(sections) + "\n]\n"

    def __reversed__(self):
        return reversed(self._items)

    def __rmul__(self, other):
        return self._items * other

    # comparison magicmethods
    def __eq__(self, other):
        if self is other:
            return True

        if not isinstance(other, (self.__class__, list)):
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

        # allow list __ge__ to raise/return depending on python version
        return self._items >= other

    def __gt__(self, other):
        if isinstance(other, self.__class__):
            other = other._items

        # allow list __gt__ to raise/return depending on python version
        return self._items > other

    def __le__(self, other):
        if isinstance(other, self.__class__):
            other = other._items

        # allow list __le__ to raise/return depending on python version
        return self._items <= other

    def __lt__(self, other):
        if isinstance(other, self.__class__):
            other = other._items

        # allow list __lt__ to raise/return depending on python version
        return self._items < other
