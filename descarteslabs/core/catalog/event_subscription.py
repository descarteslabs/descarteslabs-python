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

from collections.abc import Mapping
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
    AuthCatalogObject,
    CatalogClient,
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
    COMPUTE_FUNCTION_COMPLETED : enum
        A compute function has completed all jobs.
    """

    NEW_IMAGE = "new-image"
    UPDATE_IMAGE = "update-image"
    NEW_STORAGE = "new-storage"
    UPDATE_STORAGE = "update-storage"
    NEW_VECTOR = "new-vector"
    UPDATE_VECTOR = "update-vector"
    SCHEDULED = "scheduled"
    COMPUTE_FUNCTION_COMPLETED = "compute-function-completed"


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

    def __init__(self, text: str, unquoted=False, raw=False):
        """Create a Placeholder object.

        By default when unquoted and raw are both False, the text will be rendered as a
        string value with the text substituted from the event context.
        For example, ``Placeholder("event.detail.id")`` will render as ``"some-id"``.

        If unquoted is True, the text will be rendered without enclosing quotes
        (typically for a numeric value, JSON object or array). For example,
        ``Placeholder("event.detail.geometry", unquoted=True)`` will render as
        ``{"type": "Polygon", "coordinates": [[[0, 0,], [1, 0], [1, 1], [0, 1], [0, 0]]]}``.

        If raw is True, then the text will be rendered by Jinja2 as is, without
        introducing any additional quotes or substitutions. Generally this is used
        when you must explicitly pass through a Jinja2 template expression with
        substitutions.

        In all cases, the final result after all substitions must be a fragment of
        a valid JSON string.

        Parameters
        ----------
        text : str
            The text to be rendered into the resulting JSON detail template. How it is
            handled depends on the `unquoted` and `raw` parameters.
        unquoted: bool, optional
            If False, the text will be rendered as a string value. If False,
            then the text will be rendered without enclosing quotes. Defaults to False.
            Ignored if `raw` is True.
        raw : bool, optional
            If True, the text will be rendered as is, without wrapping as a substitution
            or a string. Defaults to False.
        """
        if raw:
            self.text = text
        elif unquoted:
            self.text = f"{{{{ {text} }}}}"
        else:
            self.text = f'"{{{{ {text} }}}}"'

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
        self.rule_id = "internal:compute-job-create"
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


class EventSubscriptionSqsTarget(EventSubscriptionTarget):
    """An EventSubscriptionTarget tailored for an SQS queue.

    Supports the use of placeholders in the detail template to be substituted
    from the matching event and subscription.
    """

    def __init__(self, _: str, *args, **kwargs):
        """Create an EventSubscriptionTarget tailored for an SQS queue.

        Placeholder values can be used for any parameter value, which allows
        for passing through Jinja2 template substitutions into the resulting
        detail template which are otherwise not themselves JSON serializable.

        If no positional or keyword arguments are provided, then the message
        defaults to being the event detail.

        Parameters
        ----------
        _ : str
            The SQS queue URL.
        args : Placeholder or mapping type, optional
            At most one positional argument which is either a Placeholder object
            which will be rendered as a JSON object, or a mapping type which will
            yield the same. If a value is provided, then no kwargs are permitted.
        kwargs : Any, optional
            Keyword parameters to pass in the message to the SQS queue. They
            may include placeholders for Jinja2 templating.
        """
        super().__init__()
        self.rule_id = "internal:sqs-forwarder"
        if len(args) > 1:
            raise TypeError("At most one positional argument is allowed")
        if args:
            if kwargs:
                raise TypeError(
                    "No keyword arguments allowed with a positional argument"
                )
            if isinstance(args[0], Placeholder):
                message = args[0]
            elif isinstance(args[0], Mapping):
                message = {**args[0]}
            else:
                raise ValueError(
                    "Positional argument must be a Placeholder or a mapping type"
                )
        elif kwargs:
            message = kwargs
        else:
            message = Placeholder("event.detail", unquoted=True)

        self.detail_template = self._make_detail_template(_, message)

    def _make_detail_template(
        self,
        _,
        message,
    ):
        """Generate a template of an SQS queue message for use with a Catalog
        EventSubscription to send events to the SQS queue.

        This call will return a JSON template string (with placeholders for
        Jinja2 templating) that can be used to send a message to the SQS queue
        via an EventSubscription.

        Returns
        -------
        str
            The the detail template to use for the EventSubscription target.

        Parameters
        ----------
        _ : str
            The SQS queue URL.
        kwargs : Any, optional
            Keyword parameters to compose into the message.
        """
        placeholders = []
        return Placeholder.substitute_placeholders(
            json.dumps(
                {"message": message, "sqs_queue_url": _},
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


class EventSubscription(AuthCatalogObject):
    """A Subscription to receive event notifications.


    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict
        With the exception of readonly attributes (`created`, `modified`, `owner`, and
        `owner_role_arn`), and with the exception of properties (`ATTRIBUTES`,
        `is_modified`, and `state`), any attribute listed below can also be used as a
        keyword argument.  Also see `~EventSubscription.ATTRIBUTES`.
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
    owner_role_arn = TypedAttribute(
        str,
        doc="""str, readonlyl: The AWS IAM role associated with the owner for use in target invocation.

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
            namespace = cls.namespace_id(namespace)
            id = f"{namespace}:{name}"
            kwargs["namespace"] = namespace
            kwargs["name"] = name

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


class NewImageEventSubscription(EventSubscription):
    """A convenience class for creating an EventSubscription for a new image event.

    Creates an EventSubscription for a new image event. Based on the one or more
    Product ids provided to the constructer, the subscription is configured
    with the correct ``event_source``, ``event_type``, and ``event_namespace``
    attributes, so that they need not be provided explicitly (indeed if they are
    explicitly provided, they will be overwritten).
    """

    _derived_type = "new_image_event_subscription"

    def __init__(self, *product_ids, **kwargs):
        """Create an EventSubscription for a new image event.

        Parameters
        ----------
        product_ids : str, as one or more positional arguments
            The ids of one or more products to be subscribed, as separate positional arguments.
        Plus any additional keyword arguments to pass to the EventSubscription constructor.
        """
        if not product_ids:
            raise TypeError(
                "At least one Product id must be provided as a positional argument"
            )
        if any(not isinstance(id, str) for id in product_ids):
            raise TypeError("All Product ids must be strings")

        kwargs["event_source"] = ["catalog"]
        kwargs["event_type"] = [EventType.NEW_IMAGE]
        kwargs["event_namespace"] = product_ids
        super().__init__(**kwargs)


class NewStorageEventSubscription(EventSubscription):
    """A convenience class for creating an EventSubscription for a new storage event.

    Creates an EventSubscription for a new storage event. Based on the one or more
    Blob namespaces provided to the constructer, the subscription is configured
    with the correct ``event_source``, ``event_type``, and ``event_namespace``
    attributes, so that they need not be provided explicitly (indeed if they are
    explicitly provided, they will be overwritten).
    """

    _derived_type = "new_storage_event_subscription"

    def __init__(self, *namespaces, **kwargs):
        """Create an EventSubscription for a new storage event.

        Parameters
        ----------
        namespaces : str, as one or more positional arguments
            One or more storage namespaces to be subscribed, as separate positional arguments.
        Plus any additional keyword arguments to pass to the EventSubscription constructor.
        """
        if not namespaces:
            raise TypeError(
                "At least one storage namespace must be provided as a positional argument"
            )
        if any(not isinstance(id, str) for id in namespaces):
            raise TypeError("All Product ids must be strings")

        kwargs["event_source"] = ["catalog"]
        kwargs["event_type"] = [EventType.NEW_STORAGE]
        kwargs["event_namespace"] = namespaces
        super().__init__(**kwargs)


class NewVectorEventSubscription(EventSubscription):
    """A convenience class for creating an EventSubscription for a new storage event.

    Creates an EventSubscription for a new vector event. Based on the one or more
    Vector product ids provided to the constructer, the subscription is configured
    with the correct ``event_source``, ``event_type``, and ``event_namespace``
    attributes, so that they need not be provided explicitly (indeed if they are
    explicitly provided, they will be overwritten).
    """

    _derived_type = "new_vector_event_subscription"

    def __init__(self, *product_ids, **kwargs):
        """Create an EventSubscription for a new vector event.

        Parameters
        ----------
        product_ids : str, as one or more positional arguments
            The ids of one or more vector products to be subscribed, as separate positional arguments.
        Plus any additional keyword arguments to pass to the EventSubscription constructor.
        """
        if not product_ids:
            raise TypeError(
                "At least one product id must be provided as a positional argument"
            )
        if any(not isinstance(id, str) for id in product_ids):
            raise TypeError("All product ids must be strings")

        kwargs["event_source"] = ["vector"]
        kwargs["event_type"] = [EventType.NEW_VECTOR]
        kwargs["event_namespace"] = product_ids
        super().__init__(**kwargs)


class ComputeFunctionCompletedEventSubscription(EventSubscription):
    """A convenience class for creating an EventSubscription for a compute
    function completion event.

    Creates an EventSubscription for a compute function completion event.
    Based on the one or more Function ids provided to the constructer,
    the subscription is configured with the correct ``event_source``,
    ``event_type``, and ``event_namespace`` attributes, so that they
    need not be provided explicitly (indeed if they are explicitly provided,
    they will be overwritten).
    """

    _derived_type = "compute_function_completed_event_subscription"

    def __init__(self, *function_ids, **kwargs):
        """Create an EventSubscription for a compute function completion event.

        Parameters
        ----------
        function_ids : str, as one or more positional arguments
            One or more Function ids or Function namespaces to be subscribed,
            as separate positional arguments. A Function namespace will match
            all functions in that namespace.
        Plus any additional keyword arguments to pass to the EventSubscription constructor.
        """
        if not function_ids:
            raise TypeError(
                "At least one function id or namespace must be provided as a positional argument"
            )
        if any(not isinstance(id, str) for id in function_ids):
            raise TypeError("All product ids must be strings")

        kwargs["event_source"] = ["compute"]
        kwargs["event_type"] = [EventType.COMPUTE_FUNCTION_COMPLETED]
        kwargs["event_namespace"] = function_ids
        super().__init__(**kwargs)


class ScheduledEventSubscription(EventSubscription):
    """A convenience class for creating an EventSubscription for a scheduled event.

    Creates an EventSubscription for a scheduled event. Based on the one or more
    EventSchedule ids provided to the constructer, the subscription is configured
    with the correct ``event_source``, ``event_type``, and ``event_namespace``
    attributes, so that they need not be provided explicitly (indeed if they are
    explicitly provided, they will be overwritten).
    """

    _derived_type = "scheduled_event_subscription"

    def __init__(self, *event_schedule_ids, **kwargs):
        """Create an EventSubscription for a scheduled event.

        Parameters
        ----------
        event_schedule_ids : str, as one or more positional arguments
            The ids of one or more scheduled event to be subscribed, as separate positional arguments.
        Plus any additional keyword arguments to pass to the EventSubscription constructor.
        """
        if not event_schedule_ids:
            raise TypeError(
                "At least one EventSchedule id must be provided as a positional argument"
            )
        if any(not isinstance(id, str) for id in event_schedule_ids):
            raise TypeError("All EventSchedule ids must be strings")

        kwargs["event_source"] = ["scheduler"]
        kwargs["event_type"] = [EventType.SCHEDULED]
        kwargs["event_namespace"] = event_schedule_ids
        super().__init__(**kwargs)
