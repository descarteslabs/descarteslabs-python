from typing import ClassVar, List, Optional, Sequence, Type, TypeVar, Tuple
import json

from google.protobuf import message

from descarteslabs.client.version import __version__
from descarteslabs.common.graft import client as graft_client
from descarteslabs.workflows.cereal import deserialize_typespec, serialize_typespec
from descarteslabs.workflows.client import get_global_grpc_client, Client

from descarteslabs.workflows.types import Proxytype, Function, proxify, parameter
from descarteslabs.workflows.types.widget import serialize_params, deserialize_params

from .visualization import VizOption


MT = TypeVar("MessageType", bound=message.Message)


class PublishedGraft:
    """
    Base class for classes that represent a published graft; do not use directly.

    Subclasses must pass `message_type` keyword argument when subclassing.
    The message must have the fields:

        * serialized_graft
        * channel
        * client_version
        * typespec
        * parameters

    """

    _message_type: ClassVar[Type[MT]]

    _client: Client
    _object: Optional[Proxytype]
    _params: Optional[Tuple[Proxytype]]
    _message: MT

    def __init_subclass__(cls, message_type: Type[MT], **kwargs):
        super().__init_subclass__(**kwargs)
        cls._message_type = message_type

    def __init__(
        self,
        proxy_object: Proxytype,
        viz_options: Optional[Sequence[VizOption]] = None,
        client: Optional[Client] = None,
    ):
        """
        Construct a PublishedGraft object from a proxy object.

        If the proxy object depends on any parameters (``proxy_object.params`` is not empty),
        it's first internally converted to a `.Function` that takes those parameters
        (using `.Function.from_object`).

        Parameters
        ----------
        proxy_object
            The proxy object to publish.
            If it depends on parameters, ``proxy_obj`` is first converted
            to a `.Function` that takes those parameters.
        viz_options: list, default None
            List of `~.models.VizOption` visualization option sets.
        client
            Allows you to use a specific client instance with non-default
            auth and parameters
        """
        if client is None:
            client = get_global_grpc_client()

        proxy_object = proxify(proxy_object)

        if len(proxy_object.params) > 0:
            # turn objects that depend on parameters into Functions;
            # use their current `.params` as the parameter annotations
            params = proxy_object.params
            proxy_object = Function.from_object(proxy_object)

        elif isinstance(proxy_object, Function):
            # generate parameter annotations from the arguments of the Function.
            try:
                # try to get parameter names directly from the graft---this will work
                # even for unnamed positional arguments in the typespec
                names = proxy_object.graft["parameters"]
            except KeyError:
                # but if it's not a function graft (likely a higher-order function returned
                # from another function), use the `__signature__` so positional-only arguments
                # still get reasonable-ish (if inaccurate) names.
                names = list(proxy_object.__signature__.parameters)

            params = tuple(
                parameter(name, type_)
                for name, type_ in zip(names, proxy_object.all_arg_types)
            )

        else:
            params = ()

        typespec = serialize_typespec(type(proxy_object))
        graft = proxy_object.graft
        proto_params = serialize_params(params)

        message = self._message_type(
            serialized_graft=json.dumps(graft),
            channel=client._wf_channel,
            client_version=__version__,
            typespec=typespec,
            parameters=proto_params,
        )

        if viz_options:
            if not isinstance(viz_options, (list, tuple)):
                viz_options = [viz_options]
            message.viz_options.extend([v._message for v in viz_options])

        self._client = client
        self._object = proxy_object
        self._params = params
        self._message = message

    @classmethod
    def _from_proto(
        cls, message: MT, client: Optional[Client] = None
    ) -> "PublishedGraft":
        """
        Low-level constructor for a `PublishedGraft` object from a Protobuf message.

        Do not use this method directly; use `__init__` or `get` instead.

        Parameters
        ----------
        proto_message
            Protobuf message for the `PublishedGraft`
        client
            Allows you to use a specific client instance with non-default
            auth and parameters

        Returns
        -------
        PublishedGraft
        """

        obj = cls.__new__(cls)  # bypass __init__

        if client is None:
            client = get_global_grpc_client()

        obj._client = client
        obj._message = message
        obj._object = None
        obj._params = None

        return obj

    @property
    def object(self) -> Proxytype:
        """
        Proxytype: The stored proxy object.

        Note that if this was originally constructed from a Workflows object that depended on parameters,
        then this `object` property won't hold the same object, but rather a `.Function` which takes
        those parameters and returns that object.

        Raises ValueError if not compatible with the current channel.
        """
        if self.channel != self._client._wf_channel:
            raise ValueError(
                f"This client is compatible with channel {self._client._wf_channel!r}, "
                f"but the {type(self).__name__} is only defined for channel {self.channel!r}."
            )
        if self._object is None:
            proxy_type = deserialize_typespec(self._message.typespec)
            graft = json.loads(self._message.serialized_graft)
            isolated = graft_client.isolate_keys(graft)
            proxy_obj = proxy_type._from_graft(isolated)
            # ^ NOTE: `proxy_obj` never gets params, because if `proxy_obj` is a Function
            # it doesn't need params, and if it's not, then `self.params` should be empty.
            self._object = proxy_obj

        return self._object

    @property
    def params(self) -> Tuple[Proxytype]:
        """
        tuple: Parameter objects corresponding to the arguments to `object`, if it's a `.Function`.

        These represent any parameters (including widget objects from ``wf.widgets``) that
        `object` depended on before it became a `.Function`.
        """
        if self._params is None:
            self._params = self._make_params()
        return self._params

    def _make_params(self) -> Tuple[Proxytype]:
        return deserialize_params(self._message.parameters)

    @property
    def type(self) -> Type[Proxytype]:
        """
        type: The type of the proxy object.

        Raises ValueError if the `object` is not compatible with the current client version.
        """
        return type(self.object)

    @property
    def channel(self) -> str:
        "str: The channel the `object` is compatible with."
        return self._message.channel

    @property
    def client_version(self) -> str:
        "str: The client version the `object` was created with, and will run under."
        return self._message.client_version

    @property
    def viz_options(self) -> List[VizOption]:
        """list: `~.models.VizOption` parameter sets for this `VersionedGraft`."""
        return [VizOption._from_proto(p) for p in self._message.viz_options]

    def __eq__(self, other):
        if not isinstance(other, type(self)):
            return NotImplemented
        return self._message == other._message
