import os
from enum import Enum
from six import add_metaclass, iteritems, ensure_str, wraps
from types import MethodType
import json

from descarteslabs.client.auth import Auth
from descarteslabs.client.exceptions import ClientError, NotFoundError
from descarteslabs.client.services.service.service import Service, WrappedSession
from .attributes import (
    Attribute,
    AttributeMeta,
    AttributeValidationError,
    AttributeEqualityMixin,
    DocumentState,
    Timestamp,
    ListAttribute,
)


class DeletedObjectError(Exception):
    """Indicates that an action cannot be performed.

    Raised when some action cannot be performed because the catalog object
    has been deleted from the Descartes Labs catalog using the delete method
    (e.g. :py:meth:`Product.delete`).
    """

    pass


class UnsavedObjectError(Exception):
    """Indicate that an action cannot be performed.

    Raised when trying to delete an object that hasn't been saved.
    """

    pass


def check_deleted(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self.state == DocumentState.DELETED:
            raise DeletedObjectError(
                "catalog object is deleted, cannot perform operation"
            )
        return f(self, *args, **kwargs)

    return wrapper


def check_derived(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if self._url is None:
            raise TypeError(
                "This method is only available for a derived class of 'CatalogObject'"
            )
        return f(self, *args, **kwargs)

    return wrapper


def _new_abstract_class(cls, abstract_cls):
    if cls is abstract_cls:
        raise TypeError(
            "You can only instantiate a derived class of '{}'".format(
                abstract_cls.__name__
            )
        )

    return super(abstract_cls, cls).__new__(cls)


class _RewriteErrorSession(WrappedSession):
    """Rewrite JSON ClientErrors that are returned to make them easier to read"""

    def request(self, *args, **kwargs):
        try:
            return super(_RewriteErrorSession, self).request(*args, **kwargs)
        except ClientError as client_error:
            self._rewrite_error(client_error)
            raise

    def _rewrite_error(self, client_error):
        KEY_ERRORS = "errors"
        KEY_TITLE = "title"
        KEY_STATUS = "status"
        KEY_DETAIL = "detail"
        KEY_SOURCE = "source"
        KEY_POINTER = "pointer"
        message = ""

        for arg in client_error.args:
            try:
                errors = json.loads(arg)[KEY_ERRORS]

                for error in errors:
                    line = ""
                    seperator = ""

                    if KEY_TITLE in error:
                        line += error[KEY_TITLE]
                        seperator = ": "
                    elif KEY_STATUS in error:
                        line += error[KEY_STATUS]
                        seperator = ": "

                    if KEY_DETAIL in error:
                        line += seperator + error[KEY_DETAIL].strip(".")
                        seperator = ": "

                    if KEY_SOURCE in error:
                        source = error[KEY_SOURCE]
                        if KEY_POINTER in source:
                            source = source[KEY_POINTER].split("/")[-1]
                        line += seperator + source

                    if line:
                        message += "\n    " + line
            except Exception:
                return

        if message:
            client_error.args = (message,)


class CatalogClient(Service):
    """
    The CatalogClient handles the HTTP communication with the Descartes Labs catalog.
    It is almost sufficient to use the default client that is automatically retrieved
    using `get_default_client`.  However, if you want to adjust e.g.  the retries, you
    can create your own.

    Parameters
    ----------
    url : str, optional
        The URL to use when connecting to the Descartes Labs catalog.  Only change
        this if you are being asked to use a non-default Descartes Labs catalog.  If
        not set, the logic will first look for the environment variable
        ``DESCARTESLABS_CATALOG_V2_URL`` and then use the default Descartes Labs
        catalog.
    auth : Auth, optional
        The authentication object used when connecting to the Descartes Labs catalog.
        This is typically the default `Auth` object that uses the cached authentication
        token retrieved with the shell command "$ descarteslabs auth login".
    retries : int, optional
        The number of retries when there is a problem with the connection.  Set this to
        zero to disable retries.  The default is 3 retries.
    """

    _instance = None

    def __init__(self, url=None, auth=None, retries=None):
        if auth is None:
            auth = Auth()

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_CATALOG_V2_URL",
                "https://platform.descarteslabs.com/metadata/v1/catalog/v2",
            )

        super(CatalogClient, self).__init__(
            url, auth=auth, retries=retries, session_class=_RewriteErrorSession
        )

    @staticmethod
    def get_default_client():
        """Retrieve the default client.

        This client is used whenever you don't explicitly set the client.
        """
        if CatalogClient._instance is None:
            CatalogClient._instance = CatalogClient()

        return CatalogClient._instance

    @staticmethod
    def set_default_client(client):
        """Change the default client to the given client.

        This is the client that will be used whenever you don't explicitly set the
        client
        """
        CatalogClient._instance = client


class CatalogObjectMeta(AttributeMeta):
    def __new__(cls, name, bases, attrs):
        new_cls = super(CatalogObjectMeta, cls).__new__(cls, name, bases, attrs)

        if new_cls._doc_type:
            new_cls._model_classes_by_type_and_derived_type[
                (new_cls._doc_type, new_cls._derived_type)
            ] = new_cls
        return new_cls


@add_metaclass(CatalogObjectMeta)
class CatalogObject(AttributeEqualityMixin):
    """A base class for all representations of objects in the Descartes Labs catalog."""

    # The following can be overridden by subclasses to customize behavior:

    # JSONAPI type for this model (required)
    _doc_type = None

    # Path added to the base URL for a list request of this model (required)
    _url = None

    # List of related objects to include in read requests
    _default_includes = []

    # The derived type of this class
    _derived_type = None

    # Attribute to use to determine the derived type of an instance
    _derived_type_switch = None

    _model_classes_by_type_and_derived_type = {}

    class _RequestMethod(str, Enum):
        POST = "post"
        PATCH = "patch"
        PUT = "put"
        GET = "get"

    id = Attribute(
        mutable=False,
        serializable=False,
        doc="""str, immutable: A unique identifier for this object.

        Note that if you pass a string that does not begin with your Descartes Labs
        user organization ID, it will be prepended to your `id` with a ``:`` as
        separator.  If you are not part of an organization, your user ID is used.  Once
        set, it cannot be changed.
        """,
    )
    created = Timestamp(
        readonly=True,
        doc="""datetime, readonly: The point in time this object was created.

        *Filterable, sortable*.
        """,
    )
    modified = Timestamp(
        readonly=True,
        doc="""datetime, readonly: The point in time this object was last modified.

        *Filterable, sortable*.
        """,
    )
    owners = ListAttribute(
        Attribute,
        doc="""list(str), optional: User, group, or organization IDs that own this object.

        Defaults to [``user:current_user``, ``org:current_org``].  The owner can edit,
        delete, and change access to this object.  :ref:`See this note <product_note>`.

        *Filterable*.
        """,
    )
    readers = ListAttribute(
        Attribute,
        doc="""list(str), optional: User, group, or organization IDs that can read this object.

        Will be empty by default.  This attribute is only available to the `owners`
        of a catalog object.  :ref:`See this note <product_note>`.
        """,
    )
    writers = ListAttribute(
        Attribute,
        doc="""list(str), optional: User, group, or organization IDs that can edit this object.

        Writers will also have read permission.  Writers will be empty by default.
        See note below.  This attribute is only available to the `owners` of a catalog
        object.  :ref:`See this note <product_note>`.
        """,
    )
    extra_properties = Attribute(
        doc="""dict, optional: A dictionary of up to 50 key/value pairs.

        The keys of this dictonary must be strings, and the values of this dictionary
        can be strings or numbers.  This allows for more structured custom metadata
        to be associated with objects.
        """
    )
    tags = ListAttribute(
        Attribute,
        doc="""list, optional: A list of up to 20 tags.

        The tags may support the classification and custom filtering of objects.

        *Filterable*.
        """,
    )

    def __new__(cls, *args, **kwargs):
        return _new_abstract_class(cls, CatalogObject)

    def __init__(self, **kwargs):
        self.delete = self._instance_delete
        self._client = kwargs.pop("client", None) or CatalogClient.get_default_client()

        self._attributes = {}
        self._modified = set()

        self._initialize(
            id=kwargs.pop("id", None),
            saved=kwargs.pop("_saved", False),
            relationships=kwargs.pop("_relationships", None),
            related_objects=kwargs.pop("_related_objects", None),
            **kwargs
        )

    def __del__(self):
        for attr_type in self._attribute_types.values():
            attr_type.__delete__(self, validate=False)

    def _clear_attributes(self):
        self._mapping_attribute_instances = {}
        self._clear_modified_attributes()

        # This only applies to top-level attributes
        sticky_attributes = {}
        for name, value in self._attributes.items():
            attribute_type = self._attribute_types.get(name)
            if attribute_type._sticky:
                sticky_attributes[name] = value
        self._attributes = sticky_attributes

    def _initialize(
        self,
        id=None,
        saved=False,
        relationships=None,
        related_objects=None,
        deleted=False,
        **kwargs
    ):
        self._clear_attributes()
        self._saved = saved
        self._deleted = deleted

        # This is an immutable attribute; can only be set once
        if id:
            self.id = id

        for (name, val) in iteritems(kwargs):
            # Only silently ignore unknown attributes if data came from service
            attribute_definition = (
                self._attribute_types.get(name)
                if saved
                else self._get_attribute_type(name)
            )
            if attribute_definition is not None:
                attribute_definition.__set__(self, val, validate=not saved)

        for name, t in iteritems(self._reference_attribute_types):
            id_value = kwargs.get(t.id_field)
            if id_value is not None:
                object_value = kwargs.get(name)
                if object_value and object_value.id != id_value:
                    message = (
                        "Conflicting related object reference: '{}' was '{}' "
                        "but '{}' was '{}'"
                    ).format(t.id_field, id_value, name, object_value.id)
                    raise AttributeValidationError(message)

                if related_objects:
                    related_object = related_objects.get(
                        (t.reference_class._doc_type, id_value)
                    )
                    if related_object is not None:
                        setattr(self, name, related_object)

        if saved:
            self._clear_modified_attributes()

    def __repr__(self):

        name = ensure_str(self.name) if getattr(self, "name", None) is not None else ""

        sections = [
            # Document type and ID
            "{}: {}\n  id: {}".format(self.__class__.__name__, name, self.id)
        ]
        # related objects and their ids
        for name in sorted(self._reference_attribute_types):
            t = self._reference_attribute_types[name]
            # as a temporary hack for image upload, handle missing image_id field
            sections.append("  {}: {}".format(name, getattr(self, t.id_field, None)))

        if self.created:
            sections.append("  created: {:%c}".format(self.created))

        if self.state == DocumentState.DELETED:
            sections.append("* Deleted from the Descartes Labs catalog.")
        elif self.state != DocumentState.SAVED:
            sections.append(
                "* Not up-to-date in the Descartes Labs catalog. Call `.save()` to save or update this record."
            )

        return "\n".join(sections)

    def __eq__(self, other):
        if (
            not isinstance(other, self.__class__)
            or self.id != other.id
            or self.state != other.state
        ):
            return False

        return super(CatalogObject, self).__eq__(other)

    def __setattr__(self, name, value):
        if not (name.startswith("_") or isinstance(value, MethodType)):
            # Make sure it's a proper attribute
            self._get_attribute_type(name)
        super(CatalogObject, self).__setattr__(name, value)

    @property
    def is_modified(self):
        """bool: Whether any attributes were changed (see `state`).

        ``True`` if any of the attribute values changed since the last time this
        catalog object was retrieved or saved.  ``False`` otherwise.

        Note that assigning an identical value does not affect the state.
        """
        return bool(self._modified)

    @classmethod
    def _get_attribute_type(cls, name):
        try:
            return cls._attribute_types[name]
        except KeyError:
            raise AttributeError("{} has no attribute {}".format(cls.__name__, name))

    @classmethod
    def _get_model_class(cls, serialized_object):
        class_type = serialized_object["type"]
        klass = cls._model_classes_by_type_and_derived_type.get((class_type, None))

        if klass._derived_type_switch:
            derived_type = serialized_object["attributes"][klass._derived_type_switch]
            klass = cls._model_classes_by_type_and_derived_type.get(
                (class_type, derived_type)
            )

        return klass

    @classmethod
    def _serialize_filter_attribute(cls, name, value):
        """Serialize a single value for a filter.

        Allow the given value to be serialized using the serialization logic
        of the given attribute.  This method should only be used to serialize
        a filter value.

        Parameters
        ----------
        name : str
            The name of the attribute used for serialization logic.
        value : object
            The value to be serialized.

        Raises
        ------
        AttributeValidationError
            If the attribute is not serializable.
        """
        attribute_type = cls._get_attribute_type(name)
        if isinstance(attribute_type, ListAttribute):
            attribute_type = attribute_type._item_type
        return attribute_type.serialize(value)

    def _set_modified(self, attr_name, changed=True, validate=True):
        # Verify it is allowed to to be set
        attr = self._get_attribute_type(attr_name)
        if validate:
            if attr._readonly:
                raise AttributeValidationError(
                    "Can't set '{}' because it is a readonly attribute".format(
                        attr_name
                    )
                )
            if not attr._mutable and attr_name in self._attributes:
                raise AttributeValidationError(
                    "Can't set '{}' because it is an immutable attribute".format(
                        attr_name
                    )
                )

        if changed:
            self._modified.add(attr_name)

    def _serialize(self, attrs, jsonapi_format=False):
        serialized = {}
        for name in attrs:
            value = self._attributes[name]
            attribute_type = self._get_attribute_type(name)
            if attribute_type._serializable:
                serialized[name] = attribute_type.serialize(
                    value, jsonapi_format=jsonapi_format
                )

        return serialized

    @check_deleted
    def update(self, ignore_errors=False, **kwargs):
        """Update multiple attributes at once using the given keyword arguments.

        Parameters
        ----------
        ignore_errors : bool, optional
            ``False`` by default.  When set to ``True``, it will suppress
            `AttributeValidationError` and `AttributeError`.  Any given attribute that
            causes one of these two exceptions will be ignored, all other attributes
            will be set to the given values.

        Raises
        ------
        AttributeValidationError
            If one or more of the attributes being updated are immutable.
        AttributeError
            If one or more of the attributes are not part of this catalog object.
        DeletedObjectError
            If this catalog object was deleted.
        """
        original_values = dict(self._attributes)
        original_modified = set(self._modified)

        for (name, val) in iteritems(kwargs):
            try:
                # A non-existent attribute will raise an AttributeError
                attribute_definition = self._get_attribute_type(name)

                # A bad value will raise an AttributeValidationError
                attribute_definition.__set__(self, val)
            except (AttributeError, AttributeValidationError):
                if ignore_errors:
                    pass
                else:
                    self._attributes = original_values
                    self._modified = original_modified
                    raise

    def serialize(self, modified_only=False, jsonapi_format=False):
        """Serialize the catalog object into json.

        Parameters
        ----------
        modified_only : bool, optional
            Whether only modified attributes should be serialized.  ``False`` by
            default. If set to ``True``, only those attributes that were modified since
            the last time the catalog object was retrieved or saved will be included.
        jsonapi_format : bool, optional
            Whether to use the ``data`` element for catalog objects.  ``False`` by
            default.  When set to ``False``, the serialized data will directly contain
            the attributes of the catalog object.  If set to ``True``, the serialized
            data will follow the exact JSONAPI with a top-level ``data`` element which
            contains ``id``, ``type``, and ``attributes``.  The latter will contain
            the attributes of the catalog object.
        """
        keys = self._modified if modified_only else self._attributes.keys()
        attributes = self._serialize(keys, jsonapi_format=jsonapi_format)

        if jsonapi_format:
            return dict(
                data=dict(id=self.id, type=self._doc_type, attributes=attributes)
            )
        else:
            return attributes

    def _clear_modified_attributes(self):
        self._modified = set()

    @property
    def state(self):
        """DocumentState: The state of this catalog object."""
        if self._deleted:
            return DocumentState.DELETED

        if self._saved is False:
            return DocumentState.UNSAVED
        elif self.is_modified:
            return DocumentState.MODIFIED
        else:
            return DocumentState.SAVED

    @classmethod
    def get(cls, id, client=None):
        """Get an existing object from the Descartes Labs catalog.

        If the Descartes Labs catalog object is found, it will be returned in the
        `~descarteslabs.catalog.DocumentState.SAVED` state.  Subsequent changes will
        put the instance in the `~descarteslabs.catalog.DocumentState.MODIFIED` state,
        and you can use :py:meth:`save` to commit those changes and update the Descartes
        Labs catalog object.  Also see the example for :py:meth:`save`.

        Parameters
        ----------
        id : str
            The id of the object you are requesting.
        client : CatalogClient, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.  The
            :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
            be used if not set.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.CatalogObject` or None
            The object you requested, or ``None`` if an object with the given `id`
            does not exist in the Descartes Labs catalog.

        """
        try:
            data, related_objects = cls._send_data(
                method=cls._RequestMethod.GET, id=id, client=client
            )
        except NotFoundError:
            return None

        model_class = cls._get_model_class(data)
        return model_class(
            id=data["id"],
            client=client,
            _saved=True,
            _relationships=data.get("relationships"),
            _related_objects=related_objects,
            **data["attributes"]
        )

    @classmethod
    def get_or_create(cls, id, client=None, **kwargs):
        """Get an existing object from the Descartes Labs catalog or create a new object.

        If the Descartes Labs catalog object is found, and the remainder of the
        arguments do not differ from the values in the retrieved instance, it will be
        returned in the `~descarteslabs.catalog.DocumentState.SAVED` state.

        If the Descartes Labs catalog object is found, and the remainder of the
        arguments update one or more values in the instance, it will be returned in
        the `~descarteslabs.catalog.DocumentState.MODIFIED` state.

        If the Descartes Labs catalog object is not found, it will be created and the
        state will be `~descarteslabs.catalog.DocumentState.UNSAVED`.  Also see the
        example for :py:meth:`save`.

        Parameters
        ----------
        id : str
            The id of the object you are requesting.
        client : CatalogClient, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.  The
            :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
            be used if not set.
        kwargs : dict, optional
            With the exception of readonly attributes (`created`, `modified`), any
            attribute of a catalog object can be set as a keyword argument (Also see
            `ATTRIBUTES`).

        Returns
        -------
        :py:class:`~descarteslabs.catalog.CatalogObject`
            The requested catalog object that was retrieved or created.

        """
        obj = cls.get(id, client=client)

        if obj is None:
            obj = cls(id=id, client=client, **kwargs)
        else:
            obj.update(**kwargs)

        return obj

    @classmethod
    def get_many(cls, ids, ignore_missing=False, client=None):
        """Get existing objects from the Descartes Labs catalog.

        All returned Descartes Labs catalog objects will be in the
        `~descarteslabs.catalog.DocumentState.SAVED` state.  Also see :py:meth:`get`.

        Parameters
        ----------
        ids : list(str)
            A list of identifiers for the objects you are requesting.
        ignore_missing : bool, optional
            Whether to raise a `~descarteslabs.client.exceptions.NotFoundError`
            exception if any of the requested objects are not found in the Descartes
            Labs catalog.  ``False`` by default which raises the exception.
        client : CatalogClient, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.  The
            :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
            be used if not set.

        Raises
        ------
        NotFoundError
            If any of the requested objects do not exist in the Descartes Labs catalog
            and `ignore_missing` is ``False``.

        Returns
        -------
        list(:py:class:`~descarteslabs.catalog.CatalogObject`)
            List of the objects you requested in the same order.

        """

        if not isinstance(ids, list) or any(not isinstance(id_, str) for id_ in ids):
            raise TypeError("ids must be a list of strings")

        id_filter = {"name": "id", "op": "eq", "val": ids}

        raw_objects, related_objects = cls._send_data(
            method=cls._RequestMethod.PUT,
            client=client,
            json={"filter": json.dumps([id_filter], separators=(",", ":"))},
        )

        if not ignore_missing:
            received_ids = set(obj["id"] for obj in raw_objects)
            missing_ids = set(ids) - received_ids

            if len(missing_ids) > 0:
                raise NotFoundError(
                    "Objects not found for ids: {}".format(", ".join(missing_ids))
                )

        objects = [
            cls._get_model_class(obj)(
                id=obj["id"],
                client=client,
                _saved=True,
                _relationships=obj.get("relationships"),
                _related_objects=related_objects,
                **obj["attributes"]
            )
            for obj in raw_objects
        ]

        return objects

    @classmethod
    @check_derived
    def exists(cls, id, client=None):
        """Checks if an object exists in the Descartes Labs catalog.

        Parameters
        ----------
        id : str
            The id of the object.
        client : CatalogClient, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.  The
            :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
            be used if not set.


        Returns
        -------
        bool
            Returns ``True`` if the given ``id`` represents an existing object in
            the Descartes Labs catalog and ``False`` if not.
        """
        client = client or CatalogClient.get_default_client()
        r = None
        try:
            r = client.session.head(cls._url + "/" + id)
        except NotFoundError:
            return False

        return r and r.ok

    @classmethod
    @check_derived
    def search(cls, client=None):
        """A search query for all object of the type this class represents.

        Parameters
        ----------
        client : CatalogClient, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.  The
            :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
            be used if not set.

        Returns
        -------
        Search
            An instance of the :py:class:`~descarteslabs.catalog.Search`
            class.

        Example
        -------
        >>> search = Product.search().limit(10)
        >>> for result in search:
                print(result.name)

        """
        from .search import Search

        return Search(cls, client=client)

    @check_deleted
    def save(self, extra_attributes=None):
        """Saves this object to the Descartes Labs catalog.

        If this instance was created using the constructor, it will be in the
        `~descarteslabs.catalog.DocumentState.UNSAVED` state and is considered a new
        Descartes Labs catalog object that must be created.  If the catalog object
        already exists in this case, this method will raise a
        `~descarteslabs.client.exceptions.BadRequestError`.

        If this instance was retrieved using :py:meth:`get`, :py:meth:`get_or_create`
        or any other way (for example as part of a :py:meth:`search`), and any of its
        values were changed, it will be in the
        `~descarteslabs.catalog.DocumentState.MODIFIED` state and the existing catalog
        object will be updated.

        If this instance was retrieved using :py:meth:`get`, :py:meth:`get_or_create`
        or any other way (for example as part of a :py:meth:`search`), and none of its
        values were changed, it will be in the
        `~descarteslabs.catalog.DocumentState.SAVED` state, and if no `extra_attributes`
        parameter is given, nothing will happen.

        Parameters
        ----------
        extra_attributes : dict, optional
            A dictionary of attributes that should be sent to the catalog along with
            attributes already set on this object.  Empty by default.  If not empty,
            and the object is in the `~descarteslabs.catalog.DocumentState.SAVED`
            state, it is updated in the Descartes Labs catalog even though no attributes
            were modified.

        Raises
        ------
        BadRequestError
            If the given ``id`` already exists in the Descartes Labs catalog or
            if attribute values are invalid.
        DeletedObjectError
            If this catalog object was deleted.

        Example
        -------
        >>> new_product = Product(
        ...     id="my-product",
        ...     name="My Product",
        ...     description="This is a test product"
        ... )
        >>> new_product.state
        <DocumentState.UNSAVED: 'unsaved'>
        >>> new_product.save()
        >>> # ids will be automatically prefixed by the Descartes Labs catalog
        >>> # with your organization id
        >>> new_product.id
        my_org_id:my-product
        >>> # Now you can retrieve the product and update it
        >>> existing_product = Product.get(new_product.id)
        >>> existing_product.state
        <DocumentState.SAVED: 'saved'>
        >>> existing_product.name = "My Updated Product"
        >>> existing_product.state
        <DocumentState.MODIFIED: 'modified'>
        >>> existing_product.save()
        >>> existing_product.state
        <DocumentState.SAVED: 'saved'>
        >>> # After you delete it...
        >>> existing_product.delete()
        True
        >>> product.state
        <DocumentState.DELETED: 'deleted'>

        """
        if self.state == DocumentState.SAVED and not extra_attributes:
            # Noop, already saved in the catalog
            return

        if self.state == DocumentState.UNSAVED:
            method = self._RequestMethod.POST
            json = self.serialize(modified_only=False, jsonapi_format=True)
        else:
            method = self._RequestMethod.PATCH
            json = self.serialize(modified_only=True, jsonapi_format=True)

        if extra_attributes:
            json["data"]["attributes"].update(extra_attributes)

        data, related_objects = self._send_data(
            method=method, id=self.id, json=json, client=self._client
        )

        self._initialize(
            id=data["id"],
            saved=True,
            relationships=data.get("relationships"),
            related_objects=related_objects,
            **data["attributes"]
        )

    @check_deleted
    def reload(self):
        """Reload all attributes from the Descartes Labs catalog.

        Refresh the state of this catalog object from the object in the Descartes Labs
        catalog.  This may be necessary if there are concurrent updates and the object
        in the Descartes Labs catalog was updated from another client.  The instance
        state must be in the `~descarteslabs.catalog.DocumentState.SAVED` state.

        If you want to revert a modified object to its original one, you should use
        :py:meth:`get` on the object class with the object's `id`.

        Raises
        ------
        NotFoundError
            If the object no longer exists.
        ValueError
            If the catalog object is not in the ``SAVED`` state.
        DeletedObjectError
            If this catalog object was deleted.

        Example
        -------
        >>> p = Product("my_org_id:my_product_id")
        >>> # Some time elapses and a concurrent change was made
        >>> p.state
        <DocumentState.SAVED: 'saved'>
        >>> p.reload()
        >>> # But once you make changes, you cannot use this method any more
        >>> p.name = "My name has changed"
        >>> p.reload()
        Traceback (most recent call last):
        File "<stdin>", line 1, in <module>
        File "/Users/jaap-mbp/Work/monorepo/descarteslabs/catalog/catalog_base.py", line 47, in wrapper
            return f(self, *args, **kwargs)
        File "/Users/jaap-mbp/Work/monorepo/descarteslabs/catalog/catalog_base.py", line 879, in reload
            \"""Reload all attributes from the Descartes Labs catalog.
        ValueError: Product instance with id descarteslabs:jaaptest has not been saved
        >>> # But you can revert
        >>> p = Product.get(p.id)
        >>> p.state
        <DocumentState.SAVED: 'saved'>

        """

        if self.state != DocumentState.SAVED:
            raise ValueError(
                "{} instance with id {} has not been saved".format(
                    self.__class__.__name__, self.id
                )
            )

        try:
            data, related_objects = self._send_data(
                method=self._RequestMethod.GET, id=self.id, client=self._client
            )
        except NotFoundError:
            # once delete protocol is defined, do the same here instead of error
            raise ValueError(
                "{} instance with id {} has been deleted".format(
                    self.__class__.__name__, self.id
                )
            )

        # this will effectively wipe all current state & caching
        self._initialize(
            id=data["id"],
            saved=True,
            relationships=data.get("relationships"),
            related_objects=related_objects,
            **data["attributes"]
        )

    @classmethod
    def delete(cls, id, client=None, ignore_missing=False):
        """Delete the catalog object with the given `id`.

        Parameters
        ----------
        id : str
            The id of the object to be deleted.
        client : CatalogClient, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.  The
            :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
            be used if not set.
        ignore_missing : bool, optional
            Whether to ignore (not raise) the
            `~descarteslabs.client.exceptions.NotFoundError` exception if the object
            to be deleted is not found in the Descartes Labs catalog.  ``False`` by
            default which raises the exception.

        Returns
        -------
        bool
            ``True`` if this object was successfully deleted. ``False`` if the
            object was not found, and `ignore_missing` = ``True``.

        Raises
        ------
        ConflictError
            If the object has related objects (bands, images) that exist.
        NotFoundError
            If the object does not exist in the Descartes Labs catalog and
            `ignore_missing` is ``False``.

        Example
        -------
        >>> Image.delete('my-image-id')
        """
        # only invoked if called on the class. on instance initialization, delete is
        # bound to the _delete instance method
        return cls._delete_impl(id, ignore_missing, client=client)

    @check_deleted
    def _instance_delete(self, ignore_missing=False):
        if self.state == DocumentState.UNSAVED:
            raise UnsavedObjectError("You cannot delete an unsaved object.")

        deleted = self._delete_impl(self.id, ignore_missing, client=self._client)
        self._deleted = deleted
        return deleted

    @classmethod
    @check_derived
    def _delete_impl(cls, id, ignore_missing, client=None):
        if client is None:
            client = CatalogClient.get_default_client()
        try:
            r = client.session.delete(cls._url + "/" + id)
            return r.status_code == 200
        except NotFoundError:
            if not ignore_missing:
                raise
            return False

    @classmethod
    @check_derived
    def _send_data(cls, method, id=None, json=None, client=None):
        client = client or CatalogClient.get_default_client()
        session_method = getattr(client.session, method)
        url = cls._url

        if method not in (cls._RequestMethod.POST, cls._RequestMethod.PUT):
            url += "/" + id

        if cls._default_includes:
            url += "?include=" + ",".join(cls._default_includes)

        r = session_method(url, json=json).json()
        data = r["data"]
        related_objects = cls._load_related_objects(r, client)

        return data, related_objects

    @classmethod
    def _load_related_objects(cls, response, client):
        related_objects = {}
        related_objects_serialized = response.get("included")
        if related_objects_serialized:
            for serialized in related_objects_serialized:
                model_class = cls._get_model_class(serialized)
                if model_class:
                    related = model_class(
                        id=serialized["id"],
                        client=client,
                        _saved=True,
                        **serialized["attributes"]
                    )
                    related_objects[(serialized["type"], serialized["id"])] = related

        return related_objects
