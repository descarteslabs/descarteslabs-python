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

import json
import functools
from typing import Dict, List

from strenum import StrEnum

from ..client.services.service import ThirdPartyService
from ..common.collection import Collection
from .attributes import (
    BooleanAttribute,
    EnumAttribute,
    ExpressionAttribute,
    GeometryAttribute,
    ListAttribute,
    MappingAttribute,
    Timestamp,
    TypedAttribute,
)
from .catalog_base import (
    CatalogClient,
    CatalogObject,
)
from .search import GeoSearch


class EventType(StrEnum):
    """The event type for a subscription.

    Also refered to as the detail type for an event.

    Attributes
    ----------
    NEW_IMAGE : enum
        A new image that has been uploaded to the Catalog.
    UPDATE_IMAGE : enum
        An existing image in the Catalog has been updated.
    NEW_STORAGE : enum
        A new storage blob that has been uploaded to the Catalog.
    UPDATE_STORAGE : enum
        An existing blob in the Catalog has been updated.
    NEW_VECTOR : enum
        A new vector feature that has been uploaded to the Vector service.
    UPDATE_VECTOR : enum
        An existing vector feature in the Vector service has been updated.
    SCHEDULED : enum
        A scheduled event.
    """

    NEW_IMAGE = "new-image"
    UPDATE_IMAGE = "update-image"
    NEW_STORAGE = "new-storage"
    UPDATE_STORAGE = "update-storage"
    NEW_VECTOR = "new-vector"
    UPDATE_VECTOR = "update-vector"
    SCHEDULED = "scheduled"


class EventSubscriptionTarget(MappingAttribute):
    """Target for an EventSubscription.

    Attributes
    ----------
    rule_id : str
        The id of the EventRule for the target.
    detail_template : str, optional
        A Jinja2 template for the detail JSON for the target event.
        If not provided, the detail will be the same as the event
        which the subscription matches. The context for rendering
        the template will include the event object and the subscription
        to which this target belongs. Note that this template must
        render to a valid JSON string: no trailing commas anywhere.
    """

    rule_id = TypedAttribute(
        str,
        doc="""str: The id of the EventRule for the target.""",
    )
    detail_template = TypedAttribute(
        str,
        doc="""str, optional: A Jinja2 template for the detail JSON for the target event.""",
    )


class Placeholder:
    """Placeholder class for EventSubscriptionComputeTarget.

    Can be used in an EventSubscriptionComputeTarget for any value element,
    as a mechanism to pass Jinja2 template substitutions through to the resulting
    detail template.
    """

    def __init__(self, text: str):
        """Create a Placeholder object.

        Parameters
        ----------
        text : str
            The text to be rendered into the resulting JSON detail template. Typically
            a Jinja2 template substitution string.
        """
        self.text = text

    @classmethod
    def json_serialize(cls, obj, placeholders=None):
        if not isinstance(obj, cls):
            raise TypeError(
                f"Object of type {obj.__class__.__name__} is not JSON serializable"
            )
        placeholders.append(obj.text)
        return "__placeholder__"

    @classmethod
    def substitute_placeholders(cls, text: str, placeholders):
        for placeholder in placeholders:
            text = text.replace('"__placeholder__"', placeholder, 1)
        return text


class EventSubscriptionComputeTarget(EventSubscriptionTarget):
    """An EventSubscriptionTarget tailored for a compute function.

    Supports the use of placeholders in the detail template to be substituted
    from the matching event and subscription.
    """

    def __init__(self, _: str, *args, **kwargs):
        """Create an EventSubscriptionTarget tailored for a compute function.

        Placeholder values can be used for any parameter value, which allows
        for passing through Jinja2 template substitutions into the resulting
        detail template which are otherwise not themselves JSON serializable.

        Parameters
        ----------
        _ : str
            The compute function id to be invoked.
        args : Any, optional
            Positional arguments to pass to the compute function.
        kwargs : Any, optional
            Keyword arguments to pass to the compute function. This includes
            the special parameters `tags` and `environment` as used by the
            compute function.
        """
        super().__init__()
        self.rule_id = "descarteslabs:compute-job-create"
        self.detail_template = self._make_detail_template(_, *args, **kwargs)

    def _make_detail_template(
        self,
        _: str,
        *args,
        tags: List[str] = None,
        environment: Dict[str, str] = None,
        **kwargs,
    ):
        """Generate a template of a job invocation for use with a Catalog
        EventSubscription to send events to the compute function.

        This call will return a JSON template string (with placeholders for
        Jinja2 templating) that can be used to submit a job to the function
        via an EventSubscription.

        Returns
        -------
        str
            The the detail template to use for the EventSubscription target.

        Parameters
        ----------
        _ : str
            The compute function id to be invoked.
        args : Any, optional
            Positional arguments to pass to the function.
        tags : List[str], optional
            A list of tags to apply to the Job.
        environment : Dict[str, str], optional
            Environment variables to be set in the environment of the running Job.
            Will be merged with environment variables set on the Function, with
            the Job environment variables taking precedence.
        kwargs : Any, optional
            Keyword arguments to pass to the function.
        """
        body = {
            "function_id": _,
            "args": args or None,
            "kwargs": kwargs or None,
            "environment": environment or None,
            "tags": tags or None,
        }
        placeholders = []
        return Placeholder.substitute_placeholders(
            json.dumps(
                {"body": {k: v for k, v in body.items() if v is not None}},
                default=functools.partial(
                    Placeholder.json_serialize, placeholders=placeholders
                ),
            ),
            placeholders,
        )


class EventSubscriptionSearch(GeoSearch):
    """A search request that iterates over its search results for event subscriptions.

    The `EventSubscriptionSearch` is identical to `GeoSearch`.
    """

    pass


class EventSubscription(CatalogObject):
    """An EventSubscription.


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
        `~EventSubscription.ATTRIBUTES`.


    .. _event_subscription_note:

    Note
    ----
    The ``reader`` and ``writer`` IDs must be prefixed with ``email:``, ``user:``,
    ``group:`` or ``org:``.  The ``owner`` ID only accepts ``org:`` and ``user:``.
    Using ``org:`` as an ``owner`` will assign those privileges only to administrators
    for that organization; using ``org:`` as a ``reader`` or ``writer`` assigns those
    privileges to everyone in that organization.  The `readers` and `writers` attributes
    are only visible in full to the `owners`. If you are a `reader` or a `writer` those
    attributes will only display the element of those lists by which you are gaining
    read or write access.

    Any user with ``owner`` privileges is able to read the event subscription attributes or data,
    modify the event subscription attributes, or delete the event subscription, including reading
    and modifying the ``owners``, ``writers``, and ``readers`` attributes.

    Any user with ``writer`` privileges is able to read the event subscription attributes or data,
    or modify the event subscription attributes, but not delete the event subscription. A ``writer``
    can read the ``owners`` and can only read the entry in the ``writers`` and/or ``readers``
    by which they gain access to the event subscription.

    Any user with ``reader`` privileges is able to read the event subscription attributes or data.
    A ``reader`` can read the ``owners`` and can only read the entry in the ``writers`` and/or
    ``readers`` by which they gain access to the event subscription.

    Also see :doc:`Sharing Resources </guides/sharing>`.
    """

    _doc_type = "event_subscription"
    _url = "/event_subscriptions"
    # _collection_type set below due to circular problems
    _url_client = ThirdPartyService()

    # EventSubscription Attributes
    namespace = TypedAttribute(
        str,
        doc="""str: The namespace of this event subscription.

        All event subscriptions are stored and indexed under a namespace.
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
        doc="""str: The name of this event subscription.

        All event_subscriptions are stored and indexed by name. Names are allowed
        a restricted alphabet (``a-zA-Z0-9_-``).

        *Searchable, sortable*.
        """,
    )
    description = TypedAttribute(
        str,
        doc="""str, optional: A description with further details on this event subscription.

        The description can be up to 80,000 characters and is used by
        :py:meth:`Search.find_text`.

        *Searchable*
        """,
    )
    geometry = GeometryAttribute(
        doc="""str or shapely.geometry.base.BaseGeometry, optional: Geometry representing the AOI
        for the subscription.

        *Filterable*

        (use :py:meth:`EventSubscriptionSearch.intersects
        <descarteslabs.catalog.EventSubscriptionSearch.intersects>` to search based on geometry)
        """
    )
    expires = Timestamp(
        doc="""str or datetime, optional: Timestamp when the subscription should be expired and deleted.

        *Filterable, sortable*.
        """
    )
    owner = TypedAttribute(
        str,
        doc="""str, optional: The user who created the subscription, and for whom any subsequent actions
        will be credentialed. The form is ``user:<user hash>``.

        This attribute may not be set by the end user.

        *Filterable, sortable*.
        """,
    )
    event_type = ListAttribute(
        EnumAttribute(EventType),
        doc="""list(str): Event detail types which this subscription will match. At least one event
        detail type must be specified.

        *Filterable*.
        """,
    )
    event_source = ListAttribute(
        TypedAttribute(str),
        doc="""list(str), optional: Event sources which this subscription will match.

        *Filterable*.
        """,
    )
    event_namespace = ListAttribute(
        TypedAttribute(str),
        doc="""list(str): Event object namespaces which this subscription will match.
        At least one event namespace must be specified. For image events, this is the product_id.
        For storage events, this is the namespace of the blob.

        *Filterable*.
        """,
    )
    event_filters = ListAttribute(
        ExpressionAttribute,
        doc="""list(Expression), optional: A list of property filter expressions against the appropriate
        event object type that must all be true for an event to be matched.
        """,
    )
    targets = ListAttribute(
        EventSubscriptionTarget,
        doc="""list(EventSubscriptionTarget): A list of targets to be invoked when the subscription matches
        an event.
        """,
    )
    enabled = BooleanAttribute(
        doc="""bool, optional: True if the subscription is enabled. Non-enabled subscriptions are ignored
        during the matching of events.

        *Filterable, sortable*.
        """,
    )
    owners = ListAttribute(
        TypedAttribute(str),
        doc="""list(str), optional: User, group, or organization IDs that own this event subscription.

        Defaults to [``user:current_user``, ``org:current_org``].  The owner can edit,
        delete, and change access to this event subscription.  :ref:`See this note <event_subscription_note>`.

        *Filterable*.
        """,
    )
    readers = ListAttribute(
        TypedAttribute(str),
        doc="""list(str), optional: User, email, group, or organization IDs that can read this event subscription.

        Will be empty by default.  This attribute is only available in full to the `owners`
        of the event subscription.  :ref:`See this note <event_subscription_note>`.
        """,
    )
    writers = ListAttribute(
        TypedAttribute(str),
        doc="""list(str), optional: User, group, or organization IDs that can edit this event subscription.

        Writers will also have read permission.  Writers will be empty by default.
        See note below.  This attribute is only available in full to the `owners` of the event subscription.
        :ref:`See this note <event_subscription_note>`.
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
        >>> namespace = EventSubscription.namespace_id("myproject") # doctest: +SKIP
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
        """Get an existing EventSubscription from the Descartes Labs catalog.

        If the EventSubscription is found, it will be returned in the
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
            The namespace of the EventSubscription you wish to retrieve. Defaults to the user's org name
            (if any) plus the unique user hash. Ignored unless ``name`` is specified.
        name : str, optional
            The name of the EventSubscription you wish to retrieve. Required if ``id`` is not specified.
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
        return super(cls, EventSubscription).get(
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
            The namespace of the EventSubscription you wish to retrieve. Defaults to the user's org name
            (if any) plus the unique user hash. Ignored unless ``name`` is specified.
        name : str, optional
            The name of the EventSubscription you wish to retrieve. Required if ``id`` is not specified.
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
            id = f"{cls.namespace_id(namespace)}:{name}"

        return super(cls, EventSubscription).get_or_create(id, client=client, **kwargs)

    @classmethod
    def search(cls, client=None, request_params=None, headers=None):
        """A search query for all event subscriptions.

        Return an `~descarteslabs.catalog.EventSubscriptionSearch` instance for searching
        event subscriptions in the Descartes Labs catalog.

        Parameters
        ----------
        client : :class:`CatalogClient`, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.

        Returns
        -------
        :class:`~descarteslabs.catalog.EventSubscriptionSearch`
            An instance of the `~descarteslabs.catalog.EventSubscriptionSearch` class

        Example
        -------
        >>> from descarteslabs.catalog import EventSubscription
        >>> search = EventSubscription.search().limit(10)
        >>> for result in search: # doctest: +SKIP
        ...     print(result.name) # doctest: +SKIP

        """
        return EventSubscriptionSearch(
            cls, client=client, request_params=request_params, headers=headers
        )


class EventSubscriptionCollection(Collection):
    _item_type = EventSubscription


# handle circular references
EventSubscription._collection_type = EventSubscriptionCollection


class ScheduledEventSubscription(EventSubscription):
    """A convenience class for creating an EventSubscription for a scheduled event."""

    _derived_type = "scheduled_event_subscription"

    def __init__(self, *event_schedule_ids, **kwargs):
        """Create an EventSubscription for a scheduled event.

        Parameters
        ----------
        event_schedule_ids : str
            The ids of one or more scheduled event to subscribe to (as separate positional arguments).
        Plus any additional keyword arguments to pass to the EventSubscription constructor.
        """
        if not event_schedule_ids:
            raise TypeError(
                "At least one EventSchedule id must be provided as a positional argument"
            )
        if any(not isinstance(id, str) for id in event_schedule_ids):
            raise TypeError("All EventSchedule ids must be strings")

        kwargs["event_source"] = ["scheduler"]
        kwargs["event_type"] = ["scheduled"]
        kwargs["event_namespace"] = event_schedule_ids
        super().__init__(**kwargs)
