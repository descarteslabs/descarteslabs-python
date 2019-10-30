import time
from enum import Enum
from concurrent.futures import TimeoutError

from descarteslabs.common.property_filtering import GenericProperties
from .catalog_base import CatalogObject, CatalogClient, check_deleted
from .attributes import Attribute, Resolution, Timestamp, BooleanAttribute

properties = GenericProperties()


class Product(CatalogObject):
    """A raster product that connects band information to imagery.

    Parameters
    ----------
    kwargs : dict
        With the exception of readonly attributes
        (:py:attr:`~descarteslabs.catalog.CatalogObject.created`,
        :py:attr:`~descarteslabs.catalog.CatalogObject.modified`), any
        (inherited) attribute listed below can also be used as a keyword argument.

    Inheritance
    -----------
    For inherited parameters, methods, attributes, and properties, please refer to the
    base class:

    * :py:class:`descarteslabs.catalog.CatalogObject`

    |

    Attributes
    ----------
    name : str
        Required: The name of this product.
        *Sortable*.
    description : str
        A description with further details on this product.
    start_datetime : str, datetime-like
        The beginning of the mission for this product.
        *Filterable, sortable*.
    end_datetime : str, datetime-like
        The end of the mission for this product.
        *Filterable, sortable*.
    is_core : bool
        Whether this is a Descartes Labs catalog core product, which means that
        the product is fully supported by Descartes Labs.
        *Filterable, sortable*.
    revisit_period_minutes_min : float
        Minimum length of the time interval between observations of any given area in
        minutes.
        *Filterable, sortable*.
    revisit_period_minutes_max : float
        Maxiumum length of the time interval between observations of any given area in
        minutes.
        *Filterable, sortable*.
    resolution_min : Resolution
        Minimum resolution of the bands for this product. If applying a filter with a
        plain unitless number the value is assumed to be in meters.
        *Filterable, sortable*.
    resolution_max : Resolution
        Maximum resolution of the bands for this product. If applying a filter with a
        plain unitless number the value is assumed to be in meters.
        *Filterable, sortable*.
    """

    _doc_type = "product"
    _url = "/products"

    # Product Attributes
    name = Attribute()
    description = Attribute()
    is_core = BooleanAttribute()
    revisit_period_minutes_min = Attribute()
    revisit_period_minutes_max = Attribute()
    start_datetime = Timestamp()
    end_datetime = Timestamp()
    resolution_min = Resolution()
    resolution_max = Resolution()

    @check_deleted
    def delete_related_objects(self):
        """Delete all related bands and images for this product.

        Starts an asynchronous operation that deletes all bands and images associated
        with this product. If the product has a large number of associated images, this
        operation could take several minutes, or even hours.

        Returns
        -------
        DeletionTaskStatus
            Returns :py:class`DeletionTaskStatus` if deletion task was successfully started and ``None``
            if there were no related objects to delete.

        Raises
        ------
        ConflictError
            If a deletion process is already in progress.

        """
        r = self._client.session.post(
            "/products/{}/delete_related_objects".format(self.id),
            json={"data": {"type": "product_delete_task"}},
        )
        if r.status_code == 201:
            response = r.json()
            return DeletionTaskStatus(
                id=self.id, _client=self._client, **response["data"]["attributes"]
            )

    @check_deleted
    def get_delete_status(self):
        """Fetches the status of a deletion task.

        Fetches the status of a deletion task started using
        :py:meth:`delete_related_objects`.

        Returns
        -------
        DeletionTaskStatus

        """
        r = self._client.session.get(
            "/products/{}/delete_related_objects".format(self.id)
        )
        response = r.json()
        return DeletionTaskStatus(id=self.id, **response["data"]["attributes"])

    @check_deleted
    def update_related_objects_permissions(
        self, owners=None, readers=None, writers=None, inherit=False
    ):
        """Update the owners, readers, and/or writers for all related bands and images.

        Starts an asynchronous operation that updates the owners, readers, and/or
        writers of all bands and images associated with this product. If the product
        has a large number of associated images, this operation could take several
        minutes, or even hours.

        Parameters
        ----------
        owners : list(str)
        readers : list(str)
        writers : list(str)
        inherit: Whether to inherit the values from the product for owners, readers,
            and/or writers that have not been set in this request.  By default, this
            value is ``False`` and if an ACL is not set, it is not changed.
            When set to ``True``, and the ACL is not set, it is inherited from the
            product.

        Returns
        -------
        UpdatePermissionsTaskStatus
            Returns :py:class`UpdatePermissionsTaskStatus` if update task was
            successfully started and ``None`` if there were no related objects
            to update.

        Raises
        ------
        ConflictError
            If an update task is already in progress.

        """

        r = self._client.session.post(
            "/products/{}/update_related_objects_acls".format(self.id),
            json={
                "data": {
                    "type": "product_update_acls",
                    "attributes": {
                        "owners": owners,
                        "readers": readers,
                        "writers": writers,
                        "inherit": inherit,
                    },
                }
            },
        )
        if r.status_code == 201:
            response = r.json()
            return UpdatePermissionsTaskStatus(
                id=self.id, _client=self._client, **response["data"]["attributes"]
            )

    @check_deleted
    def get_update_permissions_status(self):
        """Fetches the status of an update task.

        Fetches the status of an update task started using
        :py:meth:`update_related_objects_permissions`.

        Returns
        -------
        UpdatePermissionsTaskStatus

        Example
        -------
        >>> product = Product.get('product-id')
        >>> product.update_related_objects_permissions()
        >>> product.get_update_permissions_status()

        """
        r = self._client.session.get(
            "/products/{}/update_related_objects_acls".format(self.id)
        )
        response = r.json()
        return UpdatePermissionsTaskStatus(
            id=self.id, _client=self._client, **response["data"]["attributes"]
        )

    @check_deleted
    def bands(self):
        """A search query for all bands for this product, sorted by default band
        ``sort_order``.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.search.Search`
            A :py:class:`~descarteslabs.catalog.search.Search` instance configured to
            find all bands for this product.
        """
        from .band import Band

        return (
            Band.search(client=self._client)
            .filter(properties.product_id == self.id)
            .sort("sort_order")
        )

    @check_deleted
    def derived_bands(self):
        """A search query for all derived bands associated with this product.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.search.Search`
            A :py:class:`~descarteslabs.catalog.search.Search` instance configured to
            find all derived bands for this product.
        """
        from .search import Search
        from .band import DerivedBand

        return Search(
            DerivedBand,
            url="{}/{}/relationships/{}".format(self._url, self.id, "derived_bands"),
            client=self._client,
            includes=False,
        )

    @check_deleted
    def images(self):
        """A search query for all images in this product.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.search.Search`
            A :py:class:`~descarteslabs.catalog.search.Search` instance configured to
            find all images in this product.
        """
        from .image import Image

        return Image.search(client=self._client).filter(
            properties.product_id == self.id
        )

    @check_deleted
    def image_uploads(self):
        """A search query for all uploads in this product created by this user.

        Returns
        -------
        :py:class:`~descarteslabs.catalog.search.Search`
            A :py:class:`~descarteslabs.catalog.search.Search` instance configured to
            find all uploads in this product.
        """
        from .image_upload import ImageUpload

        return ImageUpload.search(client=self._client).filter(
            properties.product_id == self.id
        )

    @classmethod
    def namespace_id(cls, id_, client=None):
        """Generate a fully namespaced id.

        Parameters
        ----------
        id_ : str
            The unprefixed part of the id that you want prefixed.
        client : CatalogClient, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.  The
            :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
            be used if not set.

        Returns
        -------
        str
            The fully namespaced id.

        Example
        -------
        >>> product_id = Product.namespace_id("my-product")
        """
        if client is None:
            client = CatalogClient.get_default_client()
        org = client.auth.payload.get("org")
        if org is None:
            org = client.auth.namespace  # defaults to the user namespace

        prefix = "{}:".format(org)
        if id_.startswith(prefix):
            return id_

        return "{}{}".format(prefix, id_)


class TaskState(Enum):
    """
    Attributes
    ----------
    NEVERRAN : enum
        The operation was never invoked.
    RUNNING : enum
        The operation is in progress.
    SUCCEEDED : enum
        The operation was successfully completed.
    FAILED : enum
        The operation resulted in a failure and may not have been completed.
    """

    NEVERRAN = "NONE"  # The operation was never started
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCESS"
    FAILED = "FAILURE"


class TaskStatus(object):
    """
    A base class for the status of asynchronous jobs.
    """

    task_name = "task"
    _TERMINAL_STATES = [TaskState.SUCCEEDED, TaskState.FAILED]
    _POLLING_INTERVAL = 60

    def __init__(
        self,
        id=None,
        status=None,
        start_datetime=None,
        duration_in_seconds=None,
        errors=None,
        _client=None,
        **kwargs
    ):
        self.product_id = id
        self.start_datetime = start_datetime
        self.duration_in_seconds = duration_in_seconds
        self.errors = errors
        self._client = _client or CatalogClient.get_default_client()

        try:
            self.status = TaskState(status)
        except ValueError:
            pass

    def __repr__(self):
        status = self.status.value if self.status else "UNKNOWN"
        text = ["{} {} status: {}".format(self.product_id, self.task_name, status)]
        if self.start_datetime:
            text.append("  - started: {}".format(self.start_datetime))

        if self.duration_in_seconds:
            text.append("  - took {:,.4f} seconds".format(self.duration_in_seconds))

        if self.errors:
            text.append("  - {} errors reported:".format(len(self.errors)))
            for e in self.errors:
                text.append("    - {}".format(e))
        return "\n".join(text)

    def reload(self):
        r = self._client.session.get(
            "/products/{}/update_related_objects_acls".format(self.product_id)
        )
        response = r.json()
        new_values = response["data"]["attributes"]

        self.status = TaskState(new_values.pop("status"))
        for (key, value) in new_values.items():
            setattr(self, key, value)

    def wait_for_completion(self, timeout=None):
        """Wait for the task to complete.

        Parameters
        ----------
        timeout : int, optional
            If specified, will wait up to specified number of seconds and will raise
            a :py:exc:`concurrent.futures.TimeoutError` if the task has not completed.

        Raises
        ------
        :py:exc:`concurrent.futures.TimeoutError`
            If the specified timeout elapses and the task has not completed
        """
        if self.status in self._TERMINAL_STATES:
            return

        if timeout:
            timeout = time.time() + timeout
        while True:
            self.reload()
            if self.status in self._TERMINAL_STATES:
                return
            if timeout:
                t = timeout - time.time()
                if t <= 0:
                    raise TimeoutError()
                t = min(t, self._POLLING_INTERVAL)
            else:
                t = self._POLLING_INTERVAL
            time.sleep(t)


class DeletionTaskStatus(TaskStatus):
    """The asynchronous deletion task's status

    Inheritance
    -----------
    For inherited parameters, methods, attributes, and properties, please refer to the
    base class:

    * :py:class:`TaskStatus`

    |

    Attributes
    ----------
    product_id : str
        The id of the product for which this task is running.
    status : TaskState
        The state of the task as explained in `TaskState`.
    start_datetime : datetime
        The date and time at which the task started running.
    duration_in_seconds : float
        The duration of the task.
    objects_deleted : int
        The number of object (a combination of bands or images) that were deleted.
    errors: list
        In case the status is ``FAILED`` this will contain a list of errors
        that were encountered.  In all other states this will not be set.
    """

    task_name = "deletion task"

    def __init__(self, objects_deleted=None, **kwargs):
        super(DeletionTaskStatus, self).__init__(**kwargs)
        self.objects_deleted = objects_deleted

    def __repr__(self):
        text = super(DeletionTaskStatus, self).__repr__()

        if self.objects_deleted:
            text += "\n  - {:,} objects deleted".format(self.objects_deleted)

        return text


class UpdatePermissionsTaskStatus(TaskStatus):
    """The asynchronous task status for updating related objects' access control permissions

    Inheritance
    -----------
    For inherited parameters, methods, attributes, and properties, please refer to the
    base class:

    * :py:class:`TaskStatus`

    |

    Attributes
    ----------
    product_id : str
        The id of the product for which this task is running.
    status : TaskState
        The state of the task as explained in `TaskState`.
    start_datetime : datetime
        The date and time at which the task started running.
    duration_in_seconds : float
        The duration of the task.
    objects_updated : int
        The number of object (a combination of bands or images) that were updated.
    errors: list
        In case the status is ``FAILED`` this will contain a list of errors
        that were encountered.  In all other states this will not be set.
    """

    task_name = "update permissions task"

    def __init__(self, objects_updated=None, **kwargs):
        super(UpdatePermissionsTaskStatus, self).__init__(**kwargs)
        self.objects_updated = objects_updated

    def __repr__(self):
        text = super(UpdatePermissionsTaskStatus, self).__repr__()
        if self.objects_updated:
            text += "\n  - {:,} objects updated".format(self.objects_updated)
        return text
