import os
import random
import json

import pyarrow as pa
import requests
from urllib3.util.retry import Retry

from descarteslabs.client.services.service.service import (
    Service,
    HttpStatusCode,
    HttpRequestMethod,
)

from descarteslabs.common.workflows import unmarshal
from descarteslabs.common.workflows.arrow_serialization import serialization_context

from ..cereal import serialize_typespec, typespec_to_unmarshal_str
from .. import _channel

from ..models.exceptions import TimeoutError
from ..models.parameters import parameters_to_grafts

from .mimetype import format_to_mimetype

from descarteslabs.workflows import results  # noqa: F401 isort:skip

# ^ we must import to register its unmarshallers


class InspectClient(Service):
    RETRY_CONFIG = Retry(
        total=3,
        connect=2,
        read=2,
        status=2,
        backoff_factor=random.uniform(1, 3),
        method_whitelist=frozenset([HttpRequestMethod.HEAD, HttpRequestMethod.GET]),
        status_forcelist=[
            HttpStatusCode.BadGateway,
            HttpStatusCode.ServiceUnavailable,
            HttpStatusCode.GatewayTimeout,
        ],
    )

    def __init__(self, channel=None, url=None, auth=None, retries=None):
        if channel is None:
            channel = _channel.__channel__
        self._channel = channel

        if url is None:
            url = os.environ.get(
                "DESCARTESLABS_WORKFLOWS_URL_HTTP",
                "https://workflows.descarteslabs.com/{}".format(channel),
            )

        super().__init__(
            url,
            auth=auth,
            retries=retries if retries is not None else self.RETRY_CONFIG,
        )

    def inspect(self, obj, timeout=30, format="pyarrow", format_options=None, **params):
        graft = obj.graft
        params_dict = parameters_to_grafts(**params)

        # TODO little dumb to have to serialize the typespec just to get the unmarshal name; EC-300 plz
        typespec = serialize_typespec(type(obj))
        result_type = typespec_to_unmarshal_str(typespec)
        # ^ this also preemptively checks whether the result type is something we'll know how to unmarshal

        mimetype = format_to_mimetype(format, format_options=format_options)

        # TODO stream=True, use resp.raw and stream through pyarrow?
        try:
            resp = self.session.post(
                "/inspect",
                json={"graft": graft, "parameters": params_dict},
                timeout=timeout,
                headers={"Accept": mimetype},
            )
        except requests.exceptions.Timeout as e:
            raise TimeoutError(e) from None

        if resp.headers["content-type"] == "application/vnd.pyarrow":
            buffer = pa.decompress(
                resp.content,
                codec=resp.headers["X-Arrow-Codec"],
                decompressed_size=int(resp.headers["X-Decompressed-Size"]),
            )

            marshalled = pa.deserialize(buffer, context=serialization_context)
            return unmarshal.unmarshal(result_type, marshalled)
        elif resp.headers["content-type"] == "application/json":
            return json.loads(resp.content)
        else:
            # return the raw data
            return resp.content


global_inspect_client = None


def get_global_inspect_client():
    global global_inspect_client
    if global_inspect_client is None:
        global_inspect_client = InspectClient()
    return global_inspect_client
