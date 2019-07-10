import json

from descarteslabs.common.proto import workflow_pb2

from .. import _channel
from ..cereal import deserialize_typespec, serialize_typespec
from ..client import Client
from .utils import pb_milliseconds_to_datetime


class Workflow(object):
    """
    A proxy object, and metadata about it.

    To construct, use:

    * `Workflow.build`: build a new Workflow from a proxy object.
    * `Workflow.get`: load a stored Workflow by ID.
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
            client = Client()
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
        """
        typespec = serialize_typespec(type(proxy_object))
        graft = proxy_object.graft

        message = workflow_pb2.Workflow(
            name=name,
            description=description,
            serialized_graft=json.dumps(graft),
            serialized_typespec=json.dumps(typespec),
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
        """
        if client is None:
            client = Client()

        message = client.api["GetWorkflow"](
            workflow_pb2.GetWorkflowRequest(id=workflow_id),
            timeout=client.DEFAULT_TIMEOUT,
        )
        return cls._from_proto(message, client)

    def update(self, proxy_object=None, name=None, description=None):
        """
        Update the proxy object and/or metadata of this Workflow.

        Values given as None are left unchanged.

        Parameters
        ----------
        proxy_object: Proxytype or None, default None
            New proxy object for this workflow
        name: str or None, default None
            New name for the Workflow
        description: str or None, default None
            New long-form description of this Workflow. Markdown is supported.
        """
        message = self._message
        if proxy_object is not None:
            typespec = serialize_typespec(type(proxy_object))
            message.serialized_typespec = json.dumps(typespec)
            message.serialized_graft = json.dumps(proxy_object.graft)

        if name is not None:
            message.name = name
        if description is not None:
            message.description = description

        new_message = self._client.api["UpdateWorkflow"](
            workflow_pb2.UpdateWorkflowRequest(workflow=message),
            timeout=self._client.DEFAULT_TIMEOUT,
        )
        self._message = new_message

    def save(self):
        """
        Persist this Workflow.

        After saving, ``self.id`` will contain the new ID of the Workflow.
        """
        message = self._client.api["CreateWorkflow"](
            workflow_pb2.CreateWorkflowRequest(workflow=self._message),
            timeout=self._client.DEFAULT_TIMEOUT,
        )
        self._message = message

    @classmethod
    def _from_proto(cls, message, client=None):
        typespec = json.loads(message.serialized_typespec)
        proxytype = deserialize_typespec(typespec)

        if message.serialized_graft:
            graft = json.loads(message.serialized_graft)
            obj = proxytype._from_graft(graft)
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

    @property
    def owners(self):
        raise NotImplementedError("ACLs are not yet supported for Workflows")

    @property
    def readers(self):
        raise NotImplementedError("ACLs are not yet supported for Workflows")

    @property
    def writers(self):
        raise NotImplementedError("ACLs are not yet supported for Workflows")
