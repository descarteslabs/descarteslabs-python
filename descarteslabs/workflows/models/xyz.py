import json

from descarteslabs.common.proto import xyz_pb2
from .. import _channel

from ..cereal import deserialize_typespec, serialize_typespec
from ..client import Client
from .utils import pb_datetime_to_milliseconds, pb_milliseconds_to_datetime


class XYZ(object):
    def __init__(self, proxy_object, proto_message, client=None):
        """
        Construct a XYZ object from a proxy object and Protobuf message.

        Do not use this method directly; use the `XYZ.build` and `XYZ.get`
        classmethods instead.

        Parameters
        ----------
        proxy_object: Proxytype
            The proxy object to store in this XYZ
        proto_message: xyz_pb2.XYZ message
            Protobuf message for the XYZ
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
    def _from_proto(cls, message, client=None):
        typespec = json.loads(message.serialized_typespec)
        proxytype = deserialize_typespec(typespec)

        if message.serialized_graft:
            graft = json.loads(message.serialized_graft)
            obj = proxytype._from_graft(graft)
        else:
            raise AttributeError(
                (
                    "The serialized graft attribute does not exist or "
                    "acces is not authorized for XYZ '{}'. To share "
                    "objects with others, please use a Workflow instead."
                ).format(message.id)
            )

        return cls(obj, message, client=client)

    @classmethod
    def build(cls, proxy_object, name="", description="", client=None):
        """
        Construct a new XYZ from a proxy object.

        Note that this does not persist the `XYZ`,
        call `save()` on the returned `XYZ` to do that.

        Parameters
        ----------
        proxy_object: Proxytype
            The proxy object to store in this XYZ
        name: str, default ""
            Name for the new XYZ
        description: str, default ""
            Long-form description of this XYZ. Markdown is supported.
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        XYZ
        """
        typespec = serialize_typespec(type(proxy_object))
        graft = proxy_object.graft

        message = xyz_pb2.XYZ(
            name=name,
            description=description,
            serialized_graft=json.dumps(graft),
            serialized_typespec=json.dumps(typespec),
            channel=_channel.__channel__,
        )
        return cls(proxy_object, message, client=client)

    @classmethod
    def get(cls, xyz_id, client=None):
        """
        Get an existing XYZ by id.

        Parameters
        ----------
        id : string
            The unique id of a `XZY` object
        client : Compute, optional
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        XYZ
        """
        if client is None:
            client = Client()

        message = client.api["GetXYZ"](
            xyz_pb2.GetXYZRequest(xyz_id=xyz_id), timeout=client.DEFAULT_TIMEOUT
        )
        return cls._from_proto(message, client)

    def update(self, *args, **kwargs):
        raise NotImplementedError("XYZ.update not implemented")

    def save(self):
        """
        Persist this XYZ layer.

        After saving, ``self.id`` will contain the new ID of the XYZ layer.
        """
        message = self._client.api["CreateXYZ"](
            xyz_pb2.CreateXYZRequest(xyz=self._message),
            timeout=self._client.DEFAULT_TIMEOUT,
        )
        self._message = message

    def iter_tile_errors(self, session_id, start_datetime=None):
        return iter_tile_errors(
            self.id, session_id, start_datetime, client=self._client
        )

    @property
    def object(self):
        """
        Proxytype: The proxy object of this XYZ.

        Raises ValueError if the XYZ is not compatible with the current channel.
        """
        if self.channel != _channel.__channel__:
            raise ValueError(
                "This client is compatible with channel '{}', "
                "but the XYZ '{}' is only defined for channel '{}'.".format(
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
        str or None: The globally unique identifier for the XYZ,
        or None if it hasn't been saved yet.
        """
        return None if self._message.id == "" else self._message.id

    @property
    def created_timestamp(self):
        """
        datetime.datetime or None: The UTC date this XYZ was created,
        or None if it hasn't been saved yet. Cannot be modified.
        """
        return pb_milliseconds_to_datetime(self._message.created_timestamp)

    @property
    def updated_timestamp(self):
        """
        datetime.datetime or None: The UTC date this XYZ was most recently modified,
        or None if it hasn't been saved yet. Updated automatically.
        """
        return pb_milliseconds_to_datetime(self._message.updated_timestamp)

    @property
    def name(self):
        "str: The name of this XYZ."
        return self._message.name

    @property
    def description(self):
        "str: A long-form description of this xyz. Markdown is supported."
        return self._message.description

    @property
    def channel(self):
        "str: The channel name this XYZ is compatible with."
        return self._message.channel

    @property
    def owners(self):
        raise NotImplementedError("ACLs are not yet supported for XYZs")

    @property
    def readers(self):
        raise NotImplementedError("ACLs are not yet supported for XYZs")

    @property
    def writers(self):
        raise NotImplementedError("ACLs are not yet supported for XYZs")


def iter_tile_errors(xyz_id, session_id, start_datetime=None, client=None):
    if client is None:
        client = Client()

    if start_datetime is None:
        start_timestamp = 0
    else:
        start_timestamp = pb_datetime_to_milliseconds(start_datetime)

    for error in client.api["GetXYZSessionErrors"](
        xyz_pb2.GetXYZSessionErrorsRequest(
            session_id=session_id, xyz_id=xyz_id, start_timestamp=start_timestamp
        )
    ):
        yield error
