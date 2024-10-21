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
    ListAttribute,
    MappingAttribute,
    StringDictAttribute,
    TypedAttribute,
)
from .catalog_base import (
    AuthCatalogObject,
    CatalogClient,
)
from .search import Search


class EventRuleTarget(MappingAttribute):
    """Target for an EventRule.

    Attributes
    ----------
    name: str
        The name of this event target.
    arn: str
        The ARN of the event target. For example, the ARN of the API Destination.
    role_arn: str, optional
        The ARN of the role to assume when invoking the target.
    input: str, optional
        The input to the target.
    input_path: str, optional
        The path into the event to be mapped to the input to the target.
    input_paths_map: dict, optional
        Dictionary mapping named substition variables for the input template
        to their path in the event.
    input_template: str, optional
        The template for the input to the target. Substitutions are defined by the input_paths_map.
        See the AWS EventBridge documentation for more information.
    ttl: int, optional
        The time to live for the target.
    retries: int, optional
        The number of times to retry the target.
    dead_letter_arn: str, optional
        The ARN of the dead letter queue for the target.
    path_parameter_values: list(str), optional
        The path parameter values for the target URL.
    header_parameters: dict, optional
        Dictionary of headers and values to be sent to the target.
    query_string_parameters: dict, optional
        Dictionary of query parameters and values to be sent to the target.
    event_api_destination_id: str
        The id of the EventApiDestination for the target.

    Note that `input`, `input_path`, and `input_template` are mutually exclusive, and `input_paths_map`
    may only be used together with `input_template`.
    """

    name = TypedAttribute(
        str,
        doc="""str: The name of this event target.""",
    )
    arn = TypedAttribute(
        str,
        doc="""str: The ARN of the event target. For example, the ARN of the API Destination.""",
    )
    role_arn = TypedAttribute(
        str,
        doc="""str, optional: The ARN of the role to assume when invoking the target.""",
    )
    input = TypedAttribute(
        str,
        doc="""str, optional: The input to the target.""",
    )
    input_path = TypedAttribute(
        str,
        doc="""str, optional: The path into the event to be mapped to the input to the target.""",
    )
    input_paths_map = StringDictAttribute(
        doc="""dict, optional: Dictionary mapping named substition variables for the input template
        to their path in the event.""",
    )
    input_template = TypedAttribute(
        str,
        doc="""str, optional: The template for the input to the target.
        Substitutions are defined by the input_paths_map.""",
    )
    ttl = TypedAttribute(
        int,
        doc="""int, optional: The time to live for the target.""",
    )
    retries = TypedAttribute(
        int,
        doc="""int, optional: The number of times to retry the target.""",
    )
    dead_letter_arn = TypedAttribute(
        str,
        doc="""str, optional: The ARN of the dead letter queue for the target.""",
    )
    path_parameter_values = ListAttribute(
        TypedAttribute(str),
        doc="""list(str), optional: The path parameter values for the target URL.""",
    )
    header_parameters = StringDictAttribute(
        doc="""dict, optional: Dictionary of HTTP headers to send to the target.""",
    )
    query_string_parameters = StringDictAttribute(
        doc="""dict, optional: Dictionary of HTTP query parameters to send to the target.""",
    )
    event_api_destination_id = TypedAttribute(
        str,
        doc="""str: The id of the EventApiDestination for the target.""",
    )


class EventRuleSearch(Search):
    """A search request that iterates over its search results for event rules.

    The `EventRuleSearch` is identical to `Search`.
    """

    pass


class EventRule(AuthCatalogObject):
    """An EventBridge rule to match event subscription targets.


    Parameters
    ----------
    client : CatalogClient, optional
        A `CatalogClient` instance to use for requests to the Descartes Labs catalog.
        The :py:meth:`~descarteslabs.catalog.CatalogClient.get_default_client` will
        be used if not set.
    kwargs : dict
        With the exception of readonly attributes (`created` and `modified`)
        and with the exception of properties (`ATTRIBUTES`, `is_modified`, and `state`),
        any attribute listed below can also be used as a keyword argument.  Also see
        `~EventRule.ATTRIBUTES`.
    """

    _doc_type = "event_rule"
    _url = "/event_rules"
    # _collection_type set below due to circular problems
    _url_client = ThirdPartyService()

    # EventRule Attributes
    namespace = TypedAttribute(
        str,
        doc="""str: The namespace of this event rule.

        All event rules are stored and indexed under a namespace.
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
        doc="""str: The name of this event rule.

        All event_rules are stored and indexed by name. Names are allowed
        a restricted alphabet (``a-zA-Z0-9_-``).

        *Searchable, sortable*.
        """,
    )
    description = TypedAttribute(
        str,
        doc="""str, optional: A description with further details on this event rule.

        The description can be up to 80,000 characters and is used by
        :py:meth:`Search.find_text`.

        *Searchable*
        """,
    )
    is_core = BooleanAttribute(
        doc="""bool, optional: Whether this is a Descartes Labs catalog core event rule.

        A core event rule is an event rule that is fully managed by Descartes Labs.  By
        default this value is ``False`` and you must have a special permission
        (``descarteslabs:core:create``) to set it to ``True``.

        *Filterable, sortable*.
        """
    )
    event_pattern = TypedAttribute(
        str,
        doc="""str: The event pattern for this rule.

        The event pattern is a JSON object that describes the event that the rule will match.
        """,
    )
    targets = ListAttribute(
        EventRuleTarget,
        doc="""list(EventRuleTarget): A list of targets to be invoked when the rule matches
        an event.

        At least one target is required.
        """,
    )
    enabled = BooleanAttribute(
        doc="""bool, optional: True if the rule is enabled. Non-enabled rules are ignored
        during the matching of events.

        *Filterable, sortable*.
        """,
    )
    event_bus_arn = TypedAttribute(
        str,
        doc="""str: The ARN of the event bus to which this rule belongs.""",
    )
    role_arn = TypedAttribute(
        str,
        doc="""str, optional: The ARN of the role to assume when targeting another Event Bus.

        *Filterable, sortable*.
        """,
    )
    rule_arn = TypedAttribute(
        str,
        doc="""str: The ARN of the rule.""",
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
        >>> namespace = EventRule.namespace_id("myproject") # doctest: +SKIP
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
        """Get an existing EventRule from the Descartes Labs catalog.

        If the EventRule is found, it will be returned in the
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
            The namespace of the EventRule you wish to retrieve. Defaults to the user's org name
            (if any) plus the unique user hash. Ignored unless ``name`` is specified.
        name : str, optional
            The name of the EventRule you wish to retrieve. Required if ``id`` is not specified.
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
        return super(cls, EventRule).get(
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
            The namespace of the EventRule you wish to retrieve. Defaults to the user's org name
            (if any) plus the unique user hash. Ignored unless ``name`` is specified.
        name : str, optional
            The name of the EventRule you wish to retrieve. Required if ``id`` is not specified.
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

        return super(cls, EventRule).get_or_create(id, client=client, **kwargs)

    @classmethod
    def search(cls, client=None, request_params=None, headers=None):
        """A search query for all event rules.

        Return an `~descarteslabs.catalog.EventRuleSearch` instance for searching
        event rules in the Descartes Labs catalog.

        Parameters
        ----------
        client : :class:`CatalogClient`, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.

        Returns
        -------
        :class:`~descarteslabs.catalog.EventRuleSearch`
            An instance of the `~descarteslabs.catalog.EventRuleSearch` class

        Example
        -------
        >>> from descarteslabs.catalog import EventRule
        >>> search = EventRule.search().limit(10)
        >>> for result in search: # doctest: +SKIP
        ...     print(result.name) # doctest: +SKIP

        """
        return EventRuleSearch(
            cls, client=client, request_params=request_params, headers=headers
        )


class EventRuleCollection(Collection):
    _item_type = EventRule


# handle circular references
EventRule._collection_type = EventRuleCollection
