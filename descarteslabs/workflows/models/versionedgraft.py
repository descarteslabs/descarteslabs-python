import json
import textwrap

from descarteslabs.common.graft import client as graft_client
from descarteslabs.common.proto.workflow import workflow_pb2
from descarteslabs.workflows import _channel
from descarteslabs.workflows.cereal import deserialize_typespec, serialize_typespec
from descarteslabs.workflows.client import get_global_grpc_client


class VersionedGraft:
    """
    A specific version of a Workflow.

    Except in advanced cases, you shouldn't need to interact with this object much—you'll primarily
    use the `Workflow` object and `wf.use <.models.use>`.
    """

    def __init__(self, version, proxy_object, docstring="", labels=None):
        """
        Construct a VersionedGraft object from a proxy object.

        You shouldn't construct a `VersionedGraft` directly; use `Workflow.set_version`
        or `wf.publish <.models.publish>` instead.

        Parameters
        ----------
        version: str
            Version of the graft. This should adhere to the semantic versioning schema (https://semver.org).
        proxy_object: Proxytype
            The proxy object source of the graft.
        docstring: str, default ""
            Docstring for the VersionedGraft.
        labels: dict, optional
            Key-value pair labels to add to the VersionedGraft.

        Returns
        -------
        VersionedGraft
        """
        typespec = serialize_typespec(type(proxy_object))
        graft = proxy_object.graft

        message = workflow_pb2.VersionedGraft(
            version=version,
            serialized_graft=json.dumps(graft),
            channel=_channel.__channel__,
            typespec=typespec,
            docstring=textwrap.dedent(docstring),
            labels=labels,
        )
        self._object = proxy_object
        self._message = message

    @classmethod
    def get(cls, workflow_id, version, client=None):
        """
        Get a specific `VersionedGraft` of a `Workflow`.

        Parameters
        ----------
        workflow_id: str
            The ID of the `Workflow`.
        version: str
            The version of the `Workflow` that you wish to fetch.
        client: `.workflows.client.Client`, optional
            Allows you to use a specific client instance with non-default
            auth and parameters.

        Returns
        -------
        VersionedGraft
        """
        if client is None:
            client = get_global_grpc_client()

        req = workflow_pb2.GetVersionRequest(id=workflow_id, version=version)

        versioned_graft_message = client.api["GetVersion"](
            req, timeout=client.DEFAULT_TIMEOUT
        )

        return cls._from_proto(versioned_graft_message)

    @classmethod
    def _from_proto(cls, message):
        """
        Low-level constructor for a `VersionedGraft` object from a Protobuf message.

        Do not use this method directly; use `VersionedGraft.__init__`
        or `VersionedGraft.get` instead.

        Parameters
        ----------
        proto_message: workflow_pb2.VersionedGraft message
            Protobuf message for the VersionedGraft

        Returns
        -------
        VersionedGraft
        """
        obj = cls.__new__(cls)  # bypass __init__

        obj._message = message
        obj._object = None

        return obj

    @property
    def type(self):
        """type: The type of the proxy object."""
        return type(self.object)

    @property
    def version(self):
        """str: The version of this `VersionedGraft`."""
        return self._message.version

    @property
    def labels(self):
        """dict: The labels attached to this `VersionedGraft`."""
        return self._message.labels

    @property
    def channel(self):
        """str: The channel under which this `VersionedGraft` was created."""
        return self._message.channel

    @property
    def docstring(self):
        """str: The docstring for this `VersionedGraft`."""
        return self._message.docstring

    @property
    def object(self):
        """
        Proxytype: The proxy object of this Workflow.

        Raises ValueError if the VersionedGraft is not compatible with the current channel.
        """
        if self.channel != _channel.__channel__:
            raise ValueError(
                "This client is compatible with channel '{}', "
                "but the VersionedGraft is only defined for channel '{}'.".format(
                    _channel.__channel__, self.channel
                )
            )
        if self._object is None:
            proxy_type = deserialize_typespec(self._message.typespec)
            graft = json.loads(self._message.serialized_graft)
            isolated = graft_client.isolate_keys(graft)
            proxy_obj = proxy_type._from_graft(isolated)
            proxy_obj.__doc__ = self.docstring
            self._object = proxy_obj

        return self._object

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._message == other._message

    def __repr__(self):
        return """\
VersionedGraft: {self.version}
    - type: {self.type.__name__}
    - labels: {self.labels}
    - channel: {self.channel}
    {docstring}
""".format(
            self=self, docstring=textwrap.indent(self.docstring, "    ")
        )
