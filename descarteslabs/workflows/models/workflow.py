from typing import Tuple
import textwrap
import re
from urllib.parse import urlencode

from ...common.proto.workflow import workflow_pb2
from ...common.proto.discover import discover_pb2

from ..client import get_global_grpc_client
from ..client.client import (
    ALL_AUTHENTICATED_USERS,
    ROLE_WORKFLOWS_VIEWER,
)

from ...discover.client import (
    _convert_target,
    Organization,
    TargetBase,
    UserEmail,
)

from .utils import pb_milliseconds_to_datetime
from .versionedgraft import VersionedGraft


class AllAuthenticatedUsers(TargetBase):
    entityType = "user-email"
    _prefix = ""
    _error_msg = "AllAuthenticatedUsers doesn't take any value"
    _compiled_re = re.compile(ALL_AUTHENTICATED_USERS)

    """Share to the public"""

    def __new__(cls) -> "AllAuthenticatedUsers":
        return super().__new__(cls, ALL_AUTHENTICATED_USERS)

    # For documentation purposes
    def __init__(self):
        pass


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
    ...     labels={"project": "arithmetic"},
    ...     tags=["test", "cool", "deep learning", "AI", "digital twin"])  # doctest: +SKIP
    >>> num = Int(1) + 1  # doctest: +SKIP
    >>> workflow.set_version(num, version="0.0.1")  # doctest: +SKIP
    >>> workflow  # doctest: +SKIP
    Workflow: "Bob's Super Cool Addition Model"
        - id: bob@gmail.com:cool_addition_model
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
        labels=None,
        tags=None,
        client=None,
    ):
        """
        Construct a `Workflow` object referencing a new or existing Workflow.

        Parameters
        ----------
        id: str
            ID for the new `Workflow`. This should be of the form ``"email:workflow_name"`` and should be
            globally unique. If this ID is not of the proper format, you will not be able to save the `Workflow`.
            Cannot be changed once set.
        title: str, default ""
            User-friendly title for the `Workflow`.
        description: str, default ""
            Long-form description of this `Workflow`. Markdown is supported.
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
        ...     labels={"project": "arithmetic"},
        ...     tags=["test", "cool", "deep learning", "AI", "digital twin"]) # doctest: +SKIP
        >>> workflow # doctest: +SKIP
        Workflow: "Bob's Super Cool Addition Model"
            - id: bob@gmail.com:cool_addition_model
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
            - labels: {'project': 'arithmetic'}
            - tags: ['test', 'cool', 'deep learning', 'AI', 'digital twin']
            - versions: '0.0.1', '0.0.2'
            The result of 1 plus 1
        """
        if client is None:
            client = get_global_grpc_client()

        message = client.api["GetWorkflow"](
            workflow_pb2.GetWorkflowRequest(id=id),
            timeout=client.DEFAULT_TIMEOUT,
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
            timeout=client.STREAM_TIMEOUT,
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

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> workflow = wf.Workflow.get("bob@gmail.com:cool_thing_wf") # doctest: +SKIP
        >>> wf.Workflow.delete_id(workflow.id) # doctest: +SKIP
        """
        if client is None:
            client = get_global_grpc_client()

        client.api["DeleteWorkflow"](
            workflow_pb2.DeleteWorkflowRequest(id=id),
            timeout=client.DEFAULT_TIMEOUT,
        )

    def delete(self, client=None):
        """
        Delete this `Workflow` and every `VersionedGraft` it contains.

        After deletion, all fields on ``self`` are reset.

        **Warning:** this cannot be undone!

        Parameters
        ----------
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> workflow = wf.Workflow.get("bob@gmail.com:cool_thing_wf") # doctest: +SKIP
        >>> workflow.delete() # doctest: +SKIP
        """
        if client is None:
            client = self._client

        self.delete_id(self.id, client=client)
        self._message.Clear()

    def save(self, client=None):
        """
        Save the current state of this Workflow and its VersionedGrafts.

        If the Workflow doesn't exist yet, it will be created. You can save up to
        1000 workflows; saving more will cause a ``ResourceExhausted`` exception.

        If it does exist, it'll attempt to update the VersionedGrafts with the ones
        set on this object. Any new versions will be added. Since VersionedGrafts are
        immutable, if a version already exists on the backend that is also set on this
        object, `save` will raise an error if the grafts of those version are different,
        otherwise updating that version will be a no-op if the grafts are the same.

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

        Raises
        ------
        ~descarteslabs.common.retry.RetryError
            Within `RetryError.exceptions <descarteslabs.common.retry.RetryError.exceptions>`
            there will be `~descarteslabs.client.grpc.exceptions.ResourceExhausted`
            exceptions.
            You reached your maximum number (1000) of saved workflows

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.update_version("0.0.1", docstring="Runs Bob's cool model") # doctest: +SKIP
        >>> # ^ update version 0.0.1's docstring
        >>> workflow.save() # doctest: +SKIP
        >>> # ^ save the updated version to the backend
        """
        if client is None:
            client = self._client

        update_request = workflow_pb2.UpsertWorkflowRequest(
            id=self._message.id,
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

    def set_version(
        self,
        version,
        obj=None,
        docstring="",
        labels=None,
        viz_options=None,
    ):
        """
        Register a version to an object. Can also be used as a decorator.

        Note: this doesn't save the new version to the backend. Call `~.save` afterwords to do so.

        If the object depends on any parameters (``obj.params`` is not empty),
        it's first internally converted to a `.Function` that takes those parameters
        (using `.Function.from_object`).

        Parameters
        ----------
        version: str
            The version to be set, tied to the given `obj`. This should adhere
            to the semantic versioning schema (https://semver.org).
        obj: Proxytype, optional
            The object to store as this version.
            If it depends on parameters, ``obj`` is first converted
            to a `.Function` that takes those parameters.

            If not provided, it's assumed that `set_version` is being
            used as a decorator on a function.
        docstring: str, default ""
            The docstring for this version. If not provided and `set_version` isn't
            being used as a decorator, then the version's docstring will be set to the
            empty string. If not provided and `set_version` is being used as a decorator,
            then the version's docstring will be set to the docstring of the
            decorated Python function.
        labels: dict, optional
            Key-value pair labels to add to the version.
        viz_options: list, default None
            List of `~.models.VizOption` visualization parameter sets.

        Returns
        -------
        version: VersionedGraft or Function
            The version set.
            If used as a decorator, returns the `~.Function` instead.

        Example
        -------
        >>> from descarteslabs.workflows import Int, Workflow
        >>> workflow = Workflow(id="bob@gmail.com:cool_model") # doctest: +SKIP
        >>> num = Int(1) + 1 # doctest: +SKIP
        >>> workflow.set_version('0.0.1', num)  # doctest: +SKIP
        >>> workflow.get_version('0.0.1') # doctest: +SKIP
        VersionedGraft: 0.0.1
            - type: Int
            - channel: master
            - labels: {}
            - viz_options: []
        """
        if obj is None:
            # decorator format
            def version_decorator(py_func):
                from descarteslabs.workflows.types import Function

                wf_func = Function.from_callable(py_func)
                docstring_ = docstring
                if not docstring_ and py_func.__doc__:
                    docstring_ = textwrap.dedent(py_func.__doc__)
                self.set_version(
                    version,
                    wf_func,
                    docstring=docstring_,
                    labels=labels,
                    viz_options=viz_options,
                )
                return wf_func  # TODO what should we return here?

            return version_decorator

        new_vg = VersionedGraft(
            version,
            obj,
            docstring=docstring,
            labels=labels,
            viz_options=viz_options,
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

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> workflow = wf.Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.get_version("0.0.1")  # doctest: +SKIP
        VersionedGraft: 0.0.1
            - type: Int
            - channel: master
            - labels: {'source_product': 'sentinel2:L1C'}
            - viz_options: []
        >>> workflow.update_version('0.0.1', labels={'source_product': 'usda:cdl:v1'}) # doctest: +SKIP
        VersionedGraft: 0.0.1
            - type: Int
            - channel: master
            - labels: {'source_product': 'usda:cdl:v1'}
            - viz_options: []
        >>> workflow.save() # doctest: +SKIP
        >>> # ^ save the updated version to the backend
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

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> workflow = wf.Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.get_version('0.0.1') # doctest: +SKIP
        VersionedGraft: 0.0.1
            - type: Int
            - channel: master
            - labels: {}
            - viz_options: []
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
    def versions(self) -> Tuple[VersionedGraft, ...]:
        """tuple: Every `VersionedGraft` in this `Workflow`, in the order they were created."""
        return tuple(
            VersionedGraft._from_proto(v) for v in self._message.versioned_grafts
        )

    @property
    def version_names(self) -> Tuple[str, ...]:
        """tuple: The names of all the versions of this `Workflow`, in the order they were created."""
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

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> new_workflow = workflow.duplicate("bob@gmail.com:even_cooler_model") # doctest: +SKIP
        >>> new_workflow.id # doctest: +SKIP
        "bob@gmail.com:even_cooler_model"
        >>> new_workflow.save() # doctest: +SKIP
        >>> # ^ save the duplicated workflow to the backend
        """
        if client is None:
            client = self._client

        new_message = workflow_pb2.Workflow()
        new_message.CopyFrom(self._message)
        new_message.id = new_id
        if not include_versions:
            new_message.ClearField("versioned_grafts")
        return self._from_proto(new_message, client)

    def add_reader(self, target, client=None):
        """
        Share the workflow with another user or organization.

        The workflow must have previously been saved to the backend;
        to do so, call `~.save`.

        Parameters
        ----------
        target: str, UserEmail, Organization
            The `UserEmail` or `Organization` with whom the workflow is to be shared.
            You can only share with your own organization.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Example
        -------
        >>> from descarteslabs.workflows import Workflow, UserEmail, Organization
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.add_reader(UserEmail("betty@gmail.com")) # doctest: +SKIP
        >>> workflow.add_reader(Organization("myorg")) # doctest: +SKIP
        """
        if client is None:
            client = self._client

        if self.created_timestamp is None:
            raise ValueError("Workflow must be saved before it can be shared")

        if target == ALL_AUTHENTICATED_USERS:
            target = AllAuthenticatedUsers()
        else:
            target = _convert_target(target)

        access_grant_request = discover_pb2.CreateAccessGrantRequest(
            access_grant=discover_pb2.AccessGrant(
                asset_name=self._asset_name,
                entity=discover_pb2.Entity(
                    type=target.entityType,
                    id=target,
                ),
                access=ROLE_WORKFLOWS_VIEWER,
            )
        )

        client.api["CreateAccessGrant"](
            access_grant_request, timeout=client.DEFAULT_TIMEOUT
        )

    def remove_reader(self, target, client=None):
        """
        Revoke sharing of the workflow with another user or organization.

        The workflow must have previously been saved to the backend; to do so, call `~.save`.

        Parameters
        ----------
        user: str
            The `UserEmail` or `Organization` for whom premission to access the workflow
            is to be revoked.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Example
        -------
        >>> from descarteslabs.workflows import Workflow, UserEmail, Organization
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.remove_reader(UserEmail("betty@gmail.com")) # doctest: +SKIP
        >>> workflow.remove_reader(Organization("myorg")) # doctest: +SKIP
        """
        if client is None:
            client = self._client

        if self.created_timestamp is None:
            raise ValueError("Workflow must be saved before it can be shared")

        if target == ALL_AUTHENTICATED_USERS:
            target = AllAuthenticatedUsers()
        else:
            target = _convert_target(target)

        delete_access_grant_request = discover_pb2.DeleteAccessGrantRequest(
            access_grant=discover_pb2.AccessGrant(
                asset_name=self._asset_name,
                entity=discover_pb2.Entity(
                    type=target.entityType,
                    id=target,
                ),
                access=ROLE_WORKFLOWS_VIEWER,
            )
        )

        client.api["DeleteAccessGrant"](
            delete_access_grant_request, timeout=client.DEFAULT_TIMEOUT
        )

    def list_readers(self, client=None):
        """
        Iterator over users with whom the workflow has been shared.

        The workflow must have previously been saved to the backend; to do so, call `~.save`.

        Parameters
        ----------
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Yields
        -----
        user: UserEmail or Organization
            `UserEmail` or `Organization` with whom the workflow has been shared
            for reading.

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> for user in workflow.list_readers(): # doctest: +SKIP
        ...     print(user) # doctest: +SKIP
        """
        if client is None:
            client = self._client

        if self.created_timestamp is None:
            raise ValueError("Workflow must be saved before it can be shared")

        list_access_grants_request = discover_pb2.ListAccessGrantsStreamRequest(
            asset_name=self._asset_name,
        )

        for r in client.api["ListAccessGrantsStream"](
            list_access_grants_request,
            timeout=client.DEFAULT_TIMEOUT,
        ):
            if r.access_grant.entity.type == Organization.entityType:
                yield Organization(r.access_grant.entity.id)
            elif r.access_grant.entity.id == ALL_AUTHENTICATED_USERS:
                yield AllAuthenticatedUsers()
            else:
                yield UserEmail(r.access_grant.entity.id)

    def add_public_reader(self, client=None):
        """
        Share the workflow with all users of the Descartes Labs Platform.
        Additional permissions are required to share a workflow publicly.
        Please contact Descartes Labs if you have a need to do so.

        The workflow must have previously been saved to the backend; to do so, call `~.save`.

        Parameters
        ----------
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.add_public_reader() # doctest: +SKIP
        """
        self.add_reader(ALL_AUTHENTICATED_USERS, client=client)

    def remove_public_reader(self, client=None):
        """
        Revoke sharing of the workflow with all users.

        The workflow must have previously been saved to the backend; to do so, call `~.save`.

        Parameters
        ----------
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.remove_public_reader() # doctest: +SKIP
        """
        self.remove_reader(ALL_AUTHENTICATED_USERS, client=client)

    def has_public_reader(self, client=None):
        """
        Check if workflow is shared with all users.

        The workflow must have previously been saved to the backend; to do so, call `~.save`.

        Parameters
        ----------
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.has_public_reader() # doctest: +SKIP
        """
        return ALL_AUTHENTICATED_USERS in self.list_readers(client=client)

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

    @property
    def _asset_name(self):
        return f"asset/workflow/{self.id}"

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._message == other._message

    def __repr__(self):
        return """\
Workflow: "{self.title}"
    - id: {self.id}
    - labels: {self.labels}
    - tags: {self.tags}
    - versions: {versions}
    {description}
""".format(
            self=self,
            versions=", ".join(map(repr, self.version_names)),
            description=textwrap.indent(self.description, "    "),
        )

    def wmts_url(self, tile_matrix_sets=None, dimensions=None) -> str:
        """
        Get the WMTS endpoint which gives access to this workflow.

        Parameters
        ----------
        tile_matrix_sets: str or list, optional
            Desired tile matrix sets. Defaults to EPSG:3857.
        dimensions: bool, optional
            If True, then provide dimensions definitions to WMTS. If
            False, then generate a layer for each possible dimensions
            attribute combination. If not specified, the WMTS service itself
            will determine how to handle dimensions (currently it does
            not provide dimensions definitions).

        Returns
        -------
        wmts_url: str
            The URL for the WMTS service endpoint corresponding to this workflow.

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get("bob@gmail.com:cool_model") # doctest: +SKIP
        >>> workflow.wmts_url() # doctest: +SKIP
        'https://workflows.prod.descarteslabs.com/master/wmts/workflow/...'
        """
        url_params = {}

        if tile_matrix_sets:
            url_params["tile_matrix_sets"] = tile_matrix_sets
        if dimensions is not None:
            url_params["dimensions"] = "true" if dimensions else "false"

        wmts_url = self._message.wmts_url_template
        if url_params:
            wmts_url = f"{wmts_url}?{urlencode(url_params, doseq=True)}"

        return wmts_url


def wmts_url(tile_matrix_sets=None, dimensions=None, client=None) -> str:
    """
    Get the WMTS endpoint which gives access to all workflows accessible to this user.

    Parameters
    ----------
    tile_matrix_sets: str or list, optional
        Desired tile matrix sets. Defaults to EPSG:3857.
    dimensions: bool, optional
        If True, then provide dimensions definitions to WMTS. If
        False, then generate a layer for each possible dimensions
        attribute combination. If not specified, the WMTS service itself
        will determine how to handle dimensions (currently it does
        not provide dimensions definitions).
    client: `.workflows.client.Client`, optional
        Allows you to use a specific client instance with non-default
        auth and parameters.

    Returns
    -------
    wmts_url: str
        The URL for the WMTS service endpoint corresponding to this channel.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.wmts_url() # doctest: +SKIP
    'https://workflows.prod.descarteslabs.com/master/wmts/workflow/1.0.0/WMTSCapabilities.xml'
    """
    if client is None:
        client = get_global_grpc_client()

    response = client.api["GetWmtsUrlTemplate"](
        workflow_pb2.Empty(), timeout=client.DEFAULT_TIMEOUT
    )

    url_params = {}

    if tile_matrix_sets:
        url_params["tile_matrix_sets"] = tile_matrix_sets
    if dimensions is not None:
        url_params["dimensions"] = "true" if dimensions else "false"

    wmts_url = response.wmts_url_template
    if url_params:
        wmts_url = f"{wmts_url}?{urlencode(url_params, doseq=True)}"

    return wmts_url
