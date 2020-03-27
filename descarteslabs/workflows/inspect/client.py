import os

import pyarrow as pa
import requests

from descarteslabs.client.services.service import Service

from descarteslabs.common.workflows import unmarshal
from descarteslabs.common.workflows.arrow_serialization import serialization_context

from ..cereal import serialize_typespec, typespec_to_unmarshal_str
from .. import _channel

from ..models.exceptions import TimeoutError
from ..models.parameters import parameters_to_grafts

from descarteslabs.workflows import results  # noqa: F401 isort:skip

# ^ we must import to register its unmarshallers


class InspectClient(Service):
    def __init__(self, channel=None, url=None, auth=None, retries=None):
        if channel is None:
            channel = _channel.__channel__
        self._channel = channel

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_WORKFLOWS_URL_HTTP",
                "https://workflows.descarteslabs.com/{}".format(channel),
            )

        super().__init__(url, auth=auth, retries=retries)

    def inspect(self, obj, timeout=30, **params):
        graft = obj.graft
        params_dict = parameters_to_grafts(**params)

        # TODO little dumb to have to serialize the typespec just to get the unmarshal name; EC-300 plz
        typespec = serialize_typespec(type(obj))
        result_type = typespec_to_unmarshal_str(typespec)
        # ^ this also preemptively checks whether the result type is something we'll know how to unmarshal

        # TODO stream=True, use resp.raw and stream through pyarrow?
        try:
            resp = self.session.post(
                "/inspect",
                json={"graft": graft, "parameters": params_dict},
                timeout=timeout,
            )
        except requests.exceptions.Timeout as e:
            raise TimeoutError(e) from None

        buffer = pa.decompress(
            resp.content,
            codec=resp.headers["X-Arrow-Codec"],
            decompressed_size=int(resp.headers["X-Decompressed-Size"]),
        )

        marshalled = pa.deserialize(buffer, context=serialization_context)
        return unmarshal.unmarshal(result_type, marshalled)


global_inspect_client = None


def get_global_inspect_client():
    global global_inspect_client
    if global_inspect_client is None:
        global_inspect_client = InspectClient()
    return global_inspect_client
