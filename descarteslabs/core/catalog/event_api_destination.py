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
    TypedAttribute,
)
from .catalog_base import (
    CatalogClient,
    CatalogObject,
)
from .search import Search


class EventConnectionParameter(MappingAttribute):
    """Parameter value for a connection.

    Attributes
    ----------
    Key: str
        The key for this parameter.
    Value: str
        The value for this parameter.
    IsValueSecret: bool
        True if the value should be stored as a secret.
    """

    Key = TypedAttribute(
        str,
        doc="""str: The key for this parameter.""",
    )
    Value = TypedAttribute(
        str,
        doc="""str: The value for this parameter.""",
    )
    IsValueSecret = TypedAttribute(
        bool,
        doc="""str: True if the value should be stored as a secret.""",
    )


class EventApiDestinationSearch(Search):
    """A search request that iterates over its search results for event api destinations.

    The `EventApiDestinationSearch` is identical to `Search`.
    """

    pass


class EventApiDestination(CatalogObject):
    """An EventApiDestination.


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
        `~EventApiDestination.ATTRIBUTES`.


    .. _event_api_destination_note:

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

    Any user with ``owner`` privileges is able to read the event api destination
    attributes or data, modify the event api destination attributes, or delete the event
    api destination, including reading and modifying the ``owners``, ``writers``, and
    ``readers`` attributes.

    Any user with ``writer`` privileges is able to read the event api destination attributes
    or data, or modify the event api destination attributes, but not delete the event api
    destination. A ``writer`` can read the ``owners`` and can only read the entry in the
    ``writers`` and/or ``readers`` by which they gain access to the event api destination.

    Any user with ``reader`` privileges is able to read the event api destination
    attributes or data. A ``reader`` can read the ``owners`` and can only read the entry
    in the ``writers`` and/or ``readers`` by which they gain access to the event api
    destination.

    Also see :doc:`Sharing Resources </guides/sharing>`.
    """

    _doc_type = "event_api_destination"
    _url = "/event_api_destinations"
    # _collection_type set below due to circular problems
    _url_client = ThirdPartyService()

    # EventApiDestination Attributes
    namespace = TypedAttribute(
        str,
        doc="""str: The namespace of this event api destination.

        All event api destinations are stored and indexed under a namespace.
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
        doc="""str: The name of this event api destination.

        All event_api_destinations are stored and indexed by name. Names are allowed
        a restricted alphabet (``a-zA-Z0-9_-``).

        *Searchable, sortable*.
        """,
    )
    description = TypedAttribute(
        str,
        doc="""str, optional: A description with further details on this event api destination.

        The description can be up to 80,000 characters and is used by
        :py:meth:`Search.find_text`.

        *Searchable*
        """,
    )
    is_core = BooleanAttribute(
        doc="""bool, optional: Whether this is a Descartes Labs catalog core event api destination.

        A core event api destination is an event api destination that is fully managed by Descartes Labs.  By
        default this value is ``False`` and you must have a special permission
        (``descarteslabs:core:create``) to set it to ``True``.

        *Filterable, sortable*.
        """
    )
    endpoint = TypedAttribute(
        str,
        doc="""str: The endpoint for this api destination. May contain `*` characters to be
        replaced with path parameters from the rule target.""",
    )
    method = TypedAttribute(
        str,
        doc="""str: The HTTP method for this api destination.""",
    )
    invocation_rate = TypedAttribute(
        int,
        doc="""int: The maximum number of invocations per second for this api destination.""",
    )
    arn = TypedAttribute(
        str,
        doc="""str: The ARN of the event api destination.""",
    )
    connection_name = TypedAttribute(
        str,
        doc="""str: The name of the connection for this api destination.""",
    )
    connection_description = TypedAttribute(
        str,
        doc="""str, optional: A description with further details on this event connection.

        The description can be up to 80,000 characters and is used by
        :py:meth:`Search.find_text`.

        *Searchable*
        """,
    )
    connection_header_parameters = ListAttribute(
        EventConnectionParameter,
        doc="""list(EventConnectionParameter): A list of connection parameters for headers to be
        sent on requests on the connection.
        """,
    )
    connection_query_string_parameters = ListAttribute(
        EventConnectionParameter,
        doc="""list(EventConnectionParameter): A list of connection parameters for query strings to be
        sent on requests on the connection.
        """,
    )
    connection_body_parameters = ListAttribute(
        EventConnectionParameter,
        doc="""list(EventConnectionParameter): A list of connection parameters for request bodies to be
        sent on requests on the connection.
        """,
    )
    connection_authorization_type = TypedAttribute(
        str,
        doc="""str: The authorization type for this api destination.""",
    )
    # for connection_authorization_type == "API_KEY"
    connection_api_key_name = TypedAttribute(
        str,
        doc="""str: The API_KEY header name.""",
    )
    connection_api_key_value = TypedAttribute(
        str,
        doc="""str: The API_KEY header value.""",
    )
    # for connection_authorization_type == "BASIC"
    connection_basic_username = TypedAttribute(
        str,
        doc="""str: The BASIC username for this api destination.""",
    )
    connection_basic_password = TypedAttribute(
        str,
        doc="""str: The BASIC password for this api destination.""",
    )
    # for connection_authorization_type == "OAUTH_CLIENT_CREDENTIALS"
    connection_oauth_endpoint = TypedAttribute(
        str,
        doc="""str: The OAUTH authorization endpoint for this connection.""",
    )
    connection_oauth_method = TypedAttribute(
        str,
        doc="""str: The HTTP method for OAuth authorization for this connection.""",
    )
    connection_oauth_client_id = TypedAttribute(
        str,
        doc="""str: The client ID for OAuth authorization for this connection.""",
    )
    connection_oauth_client_secret = TypedAttribute(
        str,
        doc="""str: The client secret for OAuth authorization for this connection.""",
    )
    connection_oauth_header_parameters = ListAttribute(
        EventConnectionParameter,
        doc="""list(EventConnectionParameter): A list of connection parameters for OAuth request
        headers to be sent on OAuth requests on the connection.
        """,
    )
    connection_oauth_query_string_parameters = ListAttribute(
        EventConnectionParameter,
        doc="""list(EventConnectionParameter): A list of connection parameters for OAuth request
        query strings to be sent on OAuth requests on the connection.
        """,
    )
    connection_oauth_body_parameters = ListAttribute(
        EventConnectionParameter,
        doc="""list(EventConnectionParameter): A list of connection parameters for OAuth request
        body values to be sent on OAuth requests on the connection.
        """,
    )
    connection_arn = TypedAttribute(
        str,
        doc="""str: The ARN of the connection.""",
    )
    owners = ListAttribute(
        TypedAttribute(str),
        doc="""list(str), optional: User, group, or organization IDs that own this event api destination.

        Defaults to [``user:current_user``, ``org:current_org``].  The owner can edit,
        delete, and change access to this event api destination.  :ref:`See this note <event_api_destination_note>`.

        *Filterable*.
        """,
    )
    readers = ListAttribute(
        TypedAttribute(str),
        doc="""list(str), optional: User, email, group, or organization IDs that can read this event api destination.

        Will be empty by default.  This attribute is only available in full to the `owners`
        of the event api destination.  :ref:`See this note <event_api_destination_note>`.
        """,
    )
    writers = ListAttribute(
        TypedAttribute(str),
        doc="""list(str), optional: User, group, or organization IDs that can edit this event api destination.

        Writers will also have read permission.  Writers will be empty by default.
        See note below.  This attribute is only available in full to the `owners` of the event api destination.
        :ref:`See this note <event_api_destination_note>`.
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
        >>> namespace = EventApiDestination.namespace_id("myproject") # doctest: +SKIP
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
        """Get an existing EventApiDestination from the Descartes Labs catalog.

        If the EventApiDestination is found, it will be returned in the
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
            The namespace of the EventApiDestination you wish to retrieve. Defaults to the user's org name
            (if any) plus the unique user hash. Ignored unless ``name`` is specified.
        name : str, optional
            The name of the EventApiDestination you wish to retrieve. Required if ``id`` is not specified.
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
        return super(cls, EventApiDestination).get(
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
            The namespace of the EventApiDestination you wish to retrieve. Defaults to the user's org name
            (if any) plus the unique user hash. Ignored unless ``name`` is specified.
        name : str, optional
            The name of the EventApiDestination you wish to retrieve. Required if ``id`` is not specified.
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

        return super(cls, EventApiDestination).get_or_create(
            id, client=client, **kwargs
        )

    @classmethod
    def search(cls, client=None, request_params=None, headers=None):
        """A search query for all event api destinations.

        Return an `~descarteslabs.catalog.EventApiDestinationSearch` instance for searching
        event api destinations in the Descartes Labs catalog.

        Parameters
        ----------
        client : :class:`CatalogClient`, optional
            A `CatalogClient` instance to use for requests to the Descartes Labs
            catalog.

        Returns
        -------
        :class:`~descarteslabs.catalog.EventApiDestinationSearch`
            An instance of the `~descarteslabs.catalog.EventApiDestinationSearch` class

        Example
        -------
        >>> from descarteslabs.catalog import EventApiDestination
        >>> search = EventApiDestination.search().limit(10)
        >>> for result in search: # doctest: +SKIP
        ...     print(result.name) # doctest: +SKIP

        """
        return EventApiDestinationSearch(
            cls, client=client, request_params=request_params, headers=headers
        )


class EventApiDestinationCollection(Collection):
    _item_type = EventApiDestination


# handle circular references
EventApiDestination._collection_type = EventApiDestinationCollection
