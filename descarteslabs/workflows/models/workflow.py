import textwrap

from descarteslabs.common.proto.workflow import workflow_pb2

from descarteslabs.workflows.client import get_global_grpc_client

from .utils import pb_milliseconds_to_datetime
from .versionedgraft import VersionedGraft


class Workflow:
    """
    A collection of versions of a proxy object, with associated metadata.

    Examples
    --------
    >>> from descarteslabs.workflows import Int, Workflow
    >>> workflow = Workflow(
    ...     id="bob@gmail.com:cool_addition_model",
    ...     title="Bob's Super Cool Addition Model",
    ...     description="The result of 1 plus 1",
    ...     public=True,
    ...     labels={"project": "arithmetic"},
    ...     tags=["test", "cool", "deep learning", "AI", "digital twin"])  # doctest: +SKIP
    >>> num = Int(1) + 1  # doctest: +SKIP
    >>> workflow.set_version(num, version="0.0.1")  # doctest: +SKIP
    >>> workflow  # doctest: +SKIP
    Workflow: "Bob's Super Cool Addition Model"
        - id: bob@gmail.com:cool_addition_model
        - public: True
        - labels: {'project': 'arithmetic'}
        - tags: ['test', 'cool', 'deep learning', 'AI', 'digital twin']
        - versions: '0.0.1'
        The result of 1 plus 1
    >>> workflow.get_version("0.0.1").object  # doctest: +SKIP
    <descarteslabs.workflows.types.primitives.number.Int object at 0x...>
    >>> workflow.get_version("0.0.1").object.inspect()  # doctest: +SKIP
    2

    >>> workflow.set_version(num + 2, version="0.0.2", docstring="1 + 2")  # doctest: +SKIP
    >>> workflow.description = "The result of 1 + 2"  # doctest: +SKIP
    >>> workflow.save()  # doctest: +SKIP
    >>> workflow.get_version("0.0.2").object.inspect()  # doctest: +SKIP
    3
    >>> workflow  # doctest: +SKIP
    Workflow: "Bob's Super Cool Addition Model"
        - id: bob@gmail.com:cool_addition_model
        - public: True
        - labels: {'project': 'arithmetic'}
        - tags: ['test', 'cool', 'deep learning', 'AI', 'digital twin']
        - versions: '0.0.1', '0.0.2'
        The result of 1 plus 2
    """

    def __init__(
        self,
        id,
        title="",
        description="",
        public=False,
        labels=None,
        tags=None,
        client=None,
    ):
        """
        Construct a `Workflow` object referencing a new or existing Workflow.

        Parameters
        ----------
        id: str
            ID for the new `Workflow`. This should be of the form
            ``"email:workflow_name"`` and should be globally unique. If this ID is not
            of the proper format, you will not be able to save the `Workflow`.
            Cannot be changed once set.
        title: str, default ""
            User-friendly title for the `Workflow`.
        description: str, default ""
            Long-form description of this `Workflow`. Markdown is supported.
        public: bool, default `False`
            Whether this `Workflow` will be publicly accessible, or only visible to you.
        labels: dict, optional
            Key-value pair labels to add to the `Workflow`.
        tags: list, optional
            A list of strings of tags to add to the `Workflow`.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Returns
        -------
        Workflow

        Example
        -------
        >>> from descarteslabs.workflows import Workflow, Int
        >>> workflow = Workflow(
        ...     id="bob@gmail.com:cool_addition_model",
        ...     title="Bob's Super Cool Addition Model",
        ...     description="The result of 1 plus 1",
        ...     public=True,
        ...     labels={"project": "arithmetic"},
        ...     tags=["test", "cool", "deep learning", "AI", "digital twin"]) # doctest: +SKIP
        >>> workflow # doctest: +SKIP
        Workflow: "Bob's Super Cool Addition Model"
            - id: bob@gmail.com:cool_addition_model
            - public: True
            - labels: {'project': 'arithmetic'}
            - tags: ['test', 'cool', 'deep learning', 'AI', 'digital twin']
            - versions: '0.0.1', '0.0.2'
            The result of 1 plus 1
        """
        if client is None:
            client = get_global_grpc_client()

        message = workflow_pb2.Workflow(
            id=id,
            title=title,
            description=textwrap.dedent(description),
            public=public,
            labels=labels,
            tags=tags,
        )

        self._message = message
        self._client = client

    @classmethod
    def get(cls, id, client=None):
        """
        Get an existing `Workflow` by ID.

        Parameters
        ----------
        id: string
            The ID of the `Workflow` (like ``name@example.com:workflow_id``)
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Returns
        -------
        Workflow

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get('bob@gmail.com:cool_addition_model') # doctest: +SKIP
        >>> workflow # doctest: +SKIP
        Workflow: "Bob's Super Cool Addition Model"
            - id: bob@gmail.com:cool_addition_model
            - public: True
            - labels: {'project': 'arithmetic'}
            - tags: ['test', 'cool', 'deep learning', 'AI', 'digital twin']
            - versions: '0.0.1', '0.0.2'
            The result of 1 plus 1
        """
        if client is None:
            client = get_global_grpc_client()

        message = client.api["GetWorkflow"](
            workflow_pb2.GetWorkflowRequest(id=id), timeout=client.DEFAULT_TIMEOUT,
        )
        return cls._from_proto(message, client)

    @classmethod
    def _from_proto(cls, proto_message, client=None):
        """
        Low-level constructor for a `Workflow` object from a protobuf message.

        Do not use this method directly; use `Workflow.__init__`
        or `Workflow.get` instead.

        Parameters
        ----------
        proto_message: workflow_pb2.Workflow message
            Protobuf message for the Workflow
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.
        """
        obj = cls.__new__(cls)  # bypass __init__

        if client is None:
            client = get_global_grpc_client()

        obj._message = proto_message
        obj._client = client
        return obj

    @classmethod
    def search(cls, email="", name="", tags=None, client=None):
        """
        Iterator over Workflows you have access to.

        All search options are logically ANDed together.

        Parameters
        ----------
        email: str, optional
            Restrict to Workflows belonging to this email address (exact match).
        name: str, optional
            Restrict to Workflow names that start with this string (prefix search).
            (The name follows the colon in a Workflow ID: ``email@example.com:this_is_the_name``.)
        tags: List[str], optional
            Restrict to Workflows that have _any_ of these tags.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Yields
        -----
        workflow: Workflow
            Workflow matching the search criteria

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> for workflow in wf.Workflow.search():  # doctest: +SKIP
        ...     print(workflow.id)  # doctest: +SKIP
        you@example.com:my_workflow
        someone_else@example.com:my_workflow
        someone_else@example.com:my_other_workflow
        """
        if client is None:
            client = get_global_grpc_client()

        stream = client.api["SearchWorkflows"](
            workflow_pb2.SearchWorkflowsRequest(
                email=email, name_prefix=name, tags=tags
            ),
            timeout=client.DEFAULT_TIMEOUT,
        )

        for message in stream:
            yield cls._from_proto(message, client)

    @staticmethod
    def delete_id(id, client=None):
        """
        Delete the `Workflow` that has the provided ID, and every `VersionedGraft` it contains.

        **Warning:** this cannot be undone!

        Parameters
        ----------
        id: str
            The ID of the `Workflow` that we wish to delete.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.
        """
        if client is None:
            client = get_global_grpc_client()

        client.api["DeleteWorkflow"](
            workflow_pb2.DeleteWorkflowRequest(id=id), timeout=client.DEFAULT_TIMEOUT,
        )

    def delete(self):
        """
        Delete this `Workflow` and every `VersionedGraft` it contains.

        After deletion, all fields on ``self`` are reset.

        **Warning:** this cannot be undone!
        """
        self.delete_id(self.id)
        self._message.Clear()

    def save(self, client=None):
        """
        Save the current state of this Workflow and its VersionedGrafts.

        If the Workflow doesn't exist yet, it will be created.

        If it does exist, it'll attempt to update the VersionedGrafts with the ones
        set on this object. Any new versions will be added. Since VersionedGrafts are
        immutable, if a version already exists on the backend that is also set on this
        object, `save` will raise an error if the grafts of those version are different,
        otherwise updaing that version will be a no-op if the grafts are the same.

        If the Workflow does exist, all Workflow-level metadata (title, description, labels, etc)
        will be overwritten with the values set on this object.

        Parameters
        ----------
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Returns
        -------
        self: Workflow
            Same Workflow object, after its state has been updated to match the backend's state
        """
        if client is None:
            client = self._client

        update_request = workflow_pb2.UpsertWorkflowRequest(
            id=self._message.id,
            public=self._message.public,
            title=self._message.title,
            description=self._message.description,
            versioned_grafts=self._message.versioned_grafts,
            labels=self._message.labels,
            tags=self._message.tags,
        )

        new_message = client.api["UpsertWorkflow"](
            update_request, timeout=client.DEFAULT_TIMEOUT
        )

        self._message = new_message
        return self

    def set_version(self, version, obj=None, docstring="", labels=None):
        """
        Register a version to an object. Can also be used as a decorator.

        Note: this doesn't save the new version to the backend. Call `~.save` afterwords to do so.

        Parameters
        ----------
        version: str
            The version to be set, tied to the given `obj`. This should adhere
            to the semantic versioning schema (https://semver.org).
        obj: Proxytype, optional
            The object to store as this version. If not provided, it's assumed
            that `set_version` is being used as a decorator on a function.
        docstring: str, default ""
            The docstring for this version. If not provided and `set_version` is not
            being used as a decorator, then the version's docstring will be set to the
            empty string. If not provided and `set_version` is being used as a decorator,
            then the version's docstring will be set to the docstring of the
            decorated Python function.
        labels: dict, optional
            Key-value pair labels to add to the version.

        Returns
        -------
        version: VersionedGraft or Function
            The version set.
            If used as a decorator, returns the `~.Function` instead.
        """
        if obj is None:
            # decorator format
            def version_decorator(py_func):
                from descarteslabs.workflows.types import Function

                wf_func = Function.from_callable(py_func)
                docstring_ = docstring
                if not docstring_:
                    docstring_ = textwrap.dedent(py_func.__doc__)
                self.set_version(version, wf_func, docstring=docstring_, labels=labels)
                return wf_func  # TODO what should we return here?

            return version_decorator

        new_vg = VersionedGraft(
            version, obj, docstring=docstring, labels=labels
        )

        for v in self._message.versioned_grafts:
            if v.version == version:
                v.CopyFrom(new_vg._message)
                break
        else:
            self._message.versioned_grafts.append(new_vg._message)

        return new_vg

    def update_version(self, version, docstring=None, labels=None):
        """
        Update the docstring and/or labels for a Workflow version.

        Note: this doesn't save the updated version to the backend. Call `~.save` afterwords to do so.

        Parameters
        ----------
        version: str
            The version to be updated.
        docstring: str, optional
            The new docstring for this version. One of docstring and labels must be
            provided.
        labels: dict, optional
            Key-value pair labels to add to the version. These labels will overwrite the
            existing labels for this version. One of docstring and labels must be
            provided.

        Returns
        -------
        version: VersionedGraft
            The updated version.
        """
        if docstring is None and labels is None:
            raise ValueError(
                "When updating a Workflow version, you must specify either a docstring, "
                "labels, or both."
            )

        for v in self._message.versioned_grafts:
            if v.version == version:
                if docstring is not None:
                    v.docstring = docstring
                if labels is not None:
                    v.labels.clear()
                    v.labels.update(labels)
                return VersionedGraft._from_proto(v)
        raise ValueError(
            "The version {!r} does exist in this Workflow's versions: {}".format(
                version, self.version_names
            )
        )

    def get_version(self, version):
        """
        Get the `VersionedGraft` object for an existing version.

        Raises a ``KeyError`` if this `Workflow` object doesn't contain that
        version. Note that that doesn't necessarily mean the version doesn't
        exist---if you constructed this `Workflow` object directly and haven't
        called `~.save` yet, the object won't contain existing versions that are
        defined on the backend, but not in this session.

        However, if you constructed this `Workflow` from `Workflow.get`, then
        an error here means the version actually doesn't exist (unless it was
        just added, after the `~.get` call).

        Note
        ----
        You can also use indexing syntax as shorthand for `get_version`:
        ``workflow["1.0.0"]`` is equivalent to ``workflow.get_version("1.0.0")``.

        Parameters
        ----------
        version: str
            A version of this `Workflow`.

        Returns
        -------
        version: VersionedGraft
            The version requested, represented as a `VersionedGraft`.

        Raises
        ------
        KeyError
            If this object doesn't contain the given version.
        """
        if not isinstance(version, str):
            raise TypeError(
                "Version name to get must be a string, "
                "not {}: {!r}".format(type(version), version)
            )
        for v in self._message.versioned_grafts:
            if v.version == version:
                return VersionedGraft._from_proto(v)
        raise KeyError(
            "The version {!r} does exist in this Workflow's versions: {}".format(
                version, self.version_names
            )
        )

    def __getitem__(self, version):
        return self.get_version(version)

    @property
    def versions(self):
        return tuple(
            VersionedGraft._from_proto(v) for v in self._message.versioned_grafts
        )

    @property
    def version_names(self):
        return tuple(v.version for v in self._message.versioned_grafts)

    def duplicate(self, new_id, include_versions=True, client=None):
        """
        Duplicate this `Workflow` object with a new ID.

        This does not save the new object to the backend; to do so, call `~.save`.

        Parameters
        ----------
        new_id: str
            ID for the copied `Workflow`
        include_versions: bool, optional, default True
            Whether to also copy over all the versions currently set on this `Workflow`

        Returns
        -------
        new_workflow: Workflow
            Copy of ``self`` with a new ID
        """
        if client is None:
            client = self._client

        new_message = workflow_pb2.Workflow()
        new_message.CopyFrom(self._message)
        new_message.id = new_id
        if not include_versions:
            new_message.ClearField("versioned_grafts")
        return self._from_proto(new_message, client)

    @property
    def id(self):
        """
        str: The globally unique identifier for the `Workflow`, of the format
        ``"user@domain.com:workflow-name"``
        """
        return self._message.id

    @property
    def created_timestamp(self):
        """
        datetime.datetime or None: The UTC date this `Workflow` was created,
        or None if it hasn't been saved yet. Cannot be modified.
        """
        return pb_milliseconds_to_datetime(self._message.created_timestamp)

    @property
    def updated_timestamp(self):
        """
        datetime.datetime or None: The UTC date this `Workflow` was most recently modified,
        or None if it hasn't been saved yet. Updated automatically.
        """
        return pb_milliseconds_to_datetime(self._message.updated_timestamp)

    @property
    def name(self):
        """str: The name of this `Workflow` (everything following the first colon in `id`)."""
        if self._message.name == "":
            return None

        return self._message.name

    @property
    def description(self):
        """str: A long-form description of this `Workflow`. Markdown is supported."""
        return self._message.description

    @description.setter
    def description(self, description):
        self._message.description = textwrap.dedent(description)

    @property
    def labels(self):
        """
        dict: The labels associated with this `Workflow`, expressed as key-value pairs.
        """
        return self._message.labels

    @labels.setter
    def labels(self, labels):
        self._message.labels.clear()
        self._message.labels.update(labels)

    @property
    def public(self):
        """bool: Whether this `Workflow` is publicly accessible."""
        return self._message.public

    @public.setter
    def public(self, public):
        self._message.public = public

    @property
    def title(self):
        """str: The user-friendly title of this `Workflow`."""
        return self._message.title

    @title.setter
    def title(self, title):
        self._message.title = title

    @property
    def tags(self):
        """
        list: The tags associated with this `Workflow`, expressed as a list of stirngs.
        """
        return self._message.tags

    @tags.setter
    def tags(self, tags):
        self._message.tags[:] = tags

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._message == other._message

    def __repr__(self):
        return """\
Workflow: "{self.title}"
    - id: {self.id}
    - public: {self.public}
    - labels: {self.labels}
    - tags: {self.tags}
    - versions: {versions}
    {description}
""".format(
            self=self,
            versions=", ".join(map(repr, self.version_names)),
            description=textwrap.indent(self.description, "    "),
        )
