# Copyright 2018-2024 Descartes Labs.
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

from ..client.services.service import ThirdPartyService
from ..common.collection import Collection
from .attributes import (
    BooleanAttribute,
    Timestamp,
    TypedAttribute,
)
from .catalog_base import (
    AuthCatalogObject,
    CatalogClient,
)
from .search import Search


class EventScheduleSearch(Search):
    """A search request that iterates over its search results for event schedules.

    The `EventScheduleSearch` is identical to `GeoSearch`.
    """

    pass


class EventSchedule(AuthCatalogObject):
    """A Scheduled Event.


    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict
        With the exception of readonly attributes (`created`, `modified`, and `owner`)
        and with the exception of properties (`ATTRIBUTES`, `is_modified`, and `state`),
        any attribute listed below can also be used as a keyword argument.  Also see
        `~EventSchedule.ATTRIBUTES`.
    """

    _doc_type = "event_schedule"
    _url = "/event_schedules"
    # _collection_type set below due to circular problems
    _url_client = ThirdPartyService()

    # EventSchedule Attributes
    namespace = TypedAttribute(
        str,
        doc="""str: The namespace of this event schedule.

        All event schedules are stored and indexed under a namespace.
        Namespaces are allowed a restricted alphabet (``a-zA-Z0-9:._-``),
        and must begin with the user's org name, or their unique user hash if
        the user has no org. The required prefix is seperated from the rest of
        the namespace name (if any) by a ``:``. If not provided, the namespace
        will default to the users org (if any) and the unique user hash.

        *Searchable, sortable*.
        """,
    )
    name = TypedAttribute(
        str,
        doc="""str: The name of this event schedule.

        All event_schedules are stored and indexed by name. Names are allowed
        a restricted alphabet (``a-zA-Z0-9_-``).

        *Searchable, sortable*.
        """,
    )
    description = TypedAttribute(
        str,
        doc="""str, optional: A description with further details on this event schedule.

        The description can be up to 80,000 characters and is used by
        :py:meth:`Search.find_text`.

        *Searchable*
        """,
    )
    arn = TypedAttribute(
        str,
        doc="""str: The Amazon Resource Name (ARN) for this event schedule.

        The ARN is a unique identifier for this event schedule in the AWS ecosystem.

        *Searchable, sortable*.
        """,
    )
    schedule = TypedAttribute(
        str,
        doc="""str: The schedule expression for this event schedule.

        The schedule expression can be one of three forms. For a single event,
        use the `at()` form. For an event which is triggered on a fixed interval,
        use the `rate()` form. For a cron-type event which recurs, use the `cron()`
        form. See the AWS EventBridge Scheduler documentation at
        https://docs.aws.amazon.com/scheduler/latest/UserGuide/schedule-types.html
        for the complete syntax of these expressions.

        *Searchable, sortable*.
        """,
    )
    schedule_timezone = TypedAttribute(
        str,
        doc="""str: The timezone for the schedule expression. Must be a valid timezone string
        as defined by the IANA ZoneInfoiana database.

        *Searchable, sortable*.
        """,
    )
    start_datetime = Timestamp(
        doc="""str or datetime, optional: Timestamp when the schedule should begin.

        *Filterable, sortable*.
        """
    )
    end_datetime = Timestamp(
        doc="""str or datetime, optional: Timestamp when the schedule should be expired and deleted.

        *Filterable, sortable*.
        """
    )
    flexible_time_window = TypedAttribute(
        int,
        doc="""int, optional: The maximum amount of time in seconds that the event schedule
        can be delayed. The event will be generated at a random time within this window,
        beginning with the nominal scheduled time.

        *Filterable, sortable*.
        """,
    )
    enabled = BooleanAttribute(
        doc="""bool, optional: True if the schedule is enabled. Non-enabled schedules are ignored
        during the matching of events.

        *Filterable, sortable*.
        """,
    )
    expires = Timestamp(
        doc="""str or datetime, readonly. Timestamp when the schedule will be expired and deleted.
        Set automatically when the schedule is created or updated.

        *Filterable, sortable*.
        """,
    )

    @classmethod
    def namespace_id(cls, namespace_id, client=None):
        """Generate a fully namespaced id.

        Parameters
        ----------
        namespace_id : str or None
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
        >>> namespace = EventSchedule.namespace_id("myproject") # doctest: +SKIP
        'myorg:myproject' # doctest: +SKIP
        """
        if client is None:
            client = CatalogClient.get_default_client()
        org = client.auth.payload.get("org")
        namespace = client.auth.namespace

        if not namespace_id:
            if org:
                return f"{org}:{namespace}"
            else:
                return namespace
        elif org:
            if namespace_id == org or namespace_id.startswith(org + ":"):
                return namespace_id
            else:
                return f"{org}:{namespace_id}"
        elif namespace_id == namespace or namespace_id.startswith(namespace + ":"):
            return namespace_id
        else:
            return f"{namespace}:{namespace_id}"

    @classmethod
    def get(
        cls,
        id=None,
        namespace=None,
        name=None,
        client=None,
        request_params=None,
        headers=None,
    ):
        """Get an existing EventSchedule from the Descartes Labs catalog.

        If the EventSchedule is found, it will be returned in the
        `~descarteslabs.catalog.DocumentState.SAVED` state.  Subsequent changes will
        put the instance in the `~descarteslabs.catalog.DocumentState.MODIFIED` state,
        and you can use :py:meth:`save` to commit those changes and update the Descartes
        Labs catalog object.  Also see the example for :py:meth:`save`.

        Exactly one of the ``id`` and ``name`` parameters must be specified. If ``name``
        is specified, it is used together with the ``namespace``
        parameters to form the corresponding ``id``.

        Parameters
        ----------
        id : str, optional
            The id of the object you are requesting. Required unless ``name`` is supplied.
            May not be specified if ``name`` is specified.
        namespace : str, optional
            The namespace of the EventSchedule you wish to retrieve. Defaults to the user's org name
            (if any) plus the unique user hash. Ignored unless ``name`` is specified.
        name : str, optional
            The name of the EventSchedule you wish to retrieve. Required if ``id`` is not specified.
            May not be specified if ``id`` is specified.
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

        Raises
        ------
        ~descarteslabs.exceptions.ClientError or ~descarteslabs.exceptions.ServerError
            :ref:`Spurious exception <network_exceptions>` that can occur during a
            network request.
        """
        if (not id and not name) or (id and name):
            raise TypeError("Must specify exactly one of id or name parameters")
        if not id:
            id = f"{cls.namespace_id(namespace)}:{name}"
        return super(cls, EventSchedule).get(
            id, client=client, request_params=request_params, headers=headers
        )

    @classmethod
    def get_or_create(
        cls,
        id=None,
        namespace=None,
        name=None,
        client=None,
        **kwargs,
    ):
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
        id : str, optional
            The id of the object you are requesting. Required unless ``name`` is supplied.
            May not be specified if ``name`` is specified.
        namespace : str, optional
            The namespace of the EventSchedule you wish to retrieve. Defaults to the user's org name
            (if any) plus the unique user hash. Ignored unless ``name`` is specified.
        name : str, optional
            The name of the EventSchedule you wish to retrieve. Required if ``id`` is not specified.
            May not be specified if ``id`` is specified.
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
        if (not id and not name) or (id and name):
            raise TypeError("Must specify exactly one of id or name parameters")
        if not id:
            namespace = cls.namespace_id(namespace)
            id = f"{namespace}:{name}"
            kwargs["namespace"] = namespace
            kwargs["name"] = name

        return super(cls, EventSchedule).get_or_create(id, client=client, **kwargs)

    @classmethod
    def search(cls, client=None, request_params=None, headers=None):
        """A search query for all event schedules.

        Return an `~descarteslabs.catalog.EventScheduleSearch` instance for searching
        event schedules in the Descartes Labs catalog.

        Parameters
        ----------
        client : :class:`CatalogClient`, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.

        Returns
        -------
        :class:`~descarteslabs.catalog.EventScheduleSearch`
            An instance of the `~descarteslabs.catalog.EventScheduleSearch` class

        Example
        -------
        >>> from descarteslabs.catalog import EventSchedule
        >>> search = EventSchedule.search().limit(10)
        >>> for result in search: # doctest: +SKIP
        ...     print(result.name) # doctest: +SKIP

        """
        return EventScheduleSearch(
            cls, client=client, request_params=request_params, headers=headers
        )


class EventScheduleCollection(Collection):
    _item_type = EventSchedule


# handle circular references
EventSchedule._collection_type = EventScheduleCollection
