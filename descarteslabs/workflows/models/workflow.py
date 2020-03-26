import json

from descarteslabs.common.graft import client as graft_client

from descarteslabs.common.proto.workflow import (
    workflow_pb2
)


from .. import _channel
from ..cereal import deserialize_typespec, serialize_typespec
from ..client import get_global_grpc_client
from .utils import pb_milliseconds_to_datetime


class Workflow(object):
    """
    A proxy object, and metadata about it.

    To construct, use:

    * `Workflow.build`: build a new Workflow from a proxy object.
    * `Workflow.get`: load a stored Workflow by ID.

    Examples
    --------
    >>> from descarteslabs.workflows import Int, Workflow, retrieve
    >>> num = Int(1) + 1
    >>> workflow = Workflow.build(num, name="one-plus-one", description="The result of 1 plus 1")
    >>> workflow
    <descarteslabs.workflows.models.workflow.Workflow object at 0x...>
    >>> workflow.save() # doctest: +SKIP
    >>> workflow.id # doctest: +SKIP
    '0eb6676dbe2de3ceb1990d9669f4ceb35c9309795d842c86'
    >>> same_workflow = retrieve('0eb6676dbe2de3ceb1990d9669f4ceb35c9309795d842c86') # doctest: +SKIP
    >>> same_workflow.object # doctest: +SKIP
    <descarteslabs.workflows.types.primitives.number.Int object at 0x...>
    >>> same_workflow.object.compute() # doctest: +SKIP
    2
    """

    def __init__(self, proxy_object, proto_message, client=None):
        """
        Construct a Workflow object from a proxy object and Protobuf message.

        Do not use this method directly; use the `Workflow.build` and `Workflow.get`
        classmethods instead.

        Parameters
        ----------
        proxy_object: Proxytype
            The proxy object to store in this Workflow
        proto_message: workflow_pb2.Workflow message
            Protobuf message for the Workflow
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters
        """
        if client is None:
            client = get_global_grpc_client()
        self._object = proxy_object
        self._message = proto_message
        self._client = client

    @classmethod
    def build(cls, proxy_object, name="", description="", client=None):
        """
        Construct a new Workflow from a proxy object.

        Note that this does not persist the `Workflow`,
        call `save()` on the returned `Workflow` to do that.

        Parameters
        ----------
        proxy_object: Proxytype
            The proxy object to store in this Workflow
        name: str, default ""
            Name for the new Workflow
        description: str, default ""
            Long-form description of this Workflow. Markdown is supported.
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        Workflow

        Example
        -------
        >>> from descarteslabs.workflows import Workflow, Int
        >>> my_int = Int(1) + 1
        >>> workflow = Workflow.build(my_int, name="one-plus-one", description="The result of 1 plus 1")
        >>> workflow
        <descarteslabs.workflows.models.workflow.Workflow object at 0x...>
        """
        typespec = serialize_typespec(type(proxy_object))
        graft = proxy_object.graft

        message = workflow_pb2.Workflow(
            name=name,
            description=description,
            serialized_graft=json.dumps(graft),
            typespec=typespec,
            channel=_channel.__channel__,
        )
        return cls(proxy_object, message, client=client)

    @classmethod
    def get(cls, workflow_id, client=None):
        """
        Get an existing workflow by id.

        Parameters
        ----------
        id : string
            The unique id of a `Workflow`
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        Workflow

        Example
        -------
        >>> from descarteslabs.workflows import Workflow
        >>> workflow = Workflow.get('0eb6676dbe2de3ceb1990d9669f4ceb35c9309795d842c86') # doctest: +SKIP
        >>> workflow # doctest: +SKIP
        <descarteslabs.workflows.models.workflow.Workflow object at 0x...>
        """
        if client is None:
            client = get_global_grpc_client()

        message = client.api["GetWorkflow"](
            workflow_pb2.GetWorkflowRequest(id=workflow_id),
            timeout=client.DEFAULT_TIMEOUT,
        )
        return cls._from_proto(message, client)

    # Not implemented on the backend yet
    # def update(self, proxy_object=None, name=None, description=None):
    #     """
    #     Update the proxy object and/or metadata of this Workflow.

    #     Values given as None are left unchanged.

    #     Parameters
    #     ----------
    #     proxy_object: Proxytype or None, default None
    #         New proxy object for this workflow
    #     name: str or None, default None
    #         New name for the Workflow
    #     description: str or None, default None
    #         New long-form description of this Workflow. Markdown is supported.

    #     Example
    #     -------
    #     >>> from descarteslabs.workflows import Workflow, Int
    #     >>> my_int = Int(1) + 1
    #     >>> workflow = Workflow.build(my_int, name="one-plus-one", description="The result of 1 plus 1")
    #     >>> updated = workflow.update(name="new-name", description="new-description") # doctest: +SKIP
    #     """
    #     message = self._message
    #     if proxy_object is not None:
    #         typespec = serialize_typespec(type(proxy_object))
    #         message.typespec = typespec
    #         message.serialized_graft = json.dumps(proxy_object.graft)

    #     if name is not None:
    #         message.name = name
    #     if description is not None:
    #         message.description = description

    #     new_message = self._client.api["UpdateWorkflow"](
    #         workflow_pb2.UpdateWorkflowRequest(workflow=message),
    #         timeout=self._client.DEFAULT_TIMEOUT,
    #     )
    #     self._message = new_message

    def save(self):
        """
        Persist this Workflow.

        After saving, ``self.id`` will contain the new ID of the Workflow.

        Example
        -------
        >>> from descarteslabs.workflows import Workflow, Int
        >>> my_int = Int(1) + 1
        >>> workflow = Workflow.build(my_int, name="one-plus-one", description="The result of 1 plus 1")
        >>> workflow.save() # doctest: +SKIP
        >>> workflow.id # doctest: +SKIP
        '0eb6676dbe2de3ceb1990d9669f4ceb35c9309795d842c86'
        """
        message = self._client.api["CreateWorkflow"](
            workflow_pb2.CreateWorkflowRequest(workflow=self._message),
            timeout=self._client.DEFAULT_TIMEOUT,
        )
        self._message = message

    @classmethod
    def _from_proto(cls, message, client=None):
        typespec = message.typespec
        proxytype = deserialize_typespec(typespec)

        if message.serialized_graft:
            graft = json.loads(message.serialized_graft)
            isolated = graft_client.isolate_keys(graft)
            obj = proxytype._from_graft(isolated)
        else:
            obj = proxytype._from_apply("Workflow.use", workflow_id=message.id)

        return cls(obj, message, client=client)

    @property
    def object(self):
        """
        Proxytype: The proxy object of this Workflow.

        Raises ValueError if the Workflow is not compatible with the current channel.
        """
        if self.channel != _channel.__channel__:
            raise ValueError(
                "This client is compatible with channel '{}', "
                "but the Workflow '{}' is only defined for channel '{}'.".format(
                    _channel.__channel__, self.id, self.channel
                )
            )
        return self._object

    @property
    def type(self):
        "type: The type of the proxy object."
        return type(self._object)

    @property
    def id(self):
        """
        str or None: The globally unique identifier for the Workflow,
        or None if it hasn't been saved yet.
        """
        return None if self._message.id == "" else self._message.id

    @property
    def created_timestamp(self):
        """
        datetime.datetime or None: The UTC date this Workflow was created,
        or None if it hasn't been saved yet. Cannot be modified.
        """
        return pb_milliseconds_to_datetime(self._message.created_timestamp)

    @property
    def updated_timestamp(self):
        """
        datetime.datetime or None: The UTC date this Workflow was most recently modified,
        or None if it hasn't been saved yet. Updated automatically.
        """
        return pb_milliseconds_to_datetime(self._message.updated_timestamp)

    @property
    def name(self):
        "str: The name of this Workflow."
        return self._message.name

    @property
    def description(self):
        "str: A long-form description of this workflow. Markdown is supported."
        return self._message.description

    @property
    def channel(self):
        "str: The channel name this Workflow is compatible with."
        return self._message.channel
