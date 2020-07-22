import os
import random
import shutil

import requests
from urllib3.util.retry import Retry

from descarteslabs.client.services.service.service import (
    Service,
    HttpStatusCode,
    HttpRequestMethod,
)
from descarteslabs.common.workflows.outputs import (
    user_format_to_mimetype,
    field_name_to_mimetype,
)
from descarteslabs.common.workflows.arrow_serialization import deserialize_pyarrow

from .. import _channel
from ..cereal import serialize_typespec, typespec_to_unmarshal_str
from ..models.exceptions import JobTimeoutError
from ..models.parameters import parameters_to_grafts
from ..result_types import unmarshal


_pyarrow_content_type = field_name_to_mimetype["pyarrow"]


class InspectClient(Service):
    RETRY_CONFIG = Retry(
        total=3,
        backoff_factor=random.uniform(1, 3),
        method_whitelist=frozenset(
            [HttpRequestMethod.HEAD, HttpRequestMethod.GET, HttpRequestMethod.POST]
        ),
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

    def inspect(self, obj, format="pyarrow", file=None, timeout=30, **params):
        graft = obj.graft
        params_dict = parameters_to_grafts(**params)

        # TODO little dumb to have to serialize the typespec just to get the unmarshal name; EC-300 plz
        typespec = serialize_typespec(type(obj))
        result_type = typespec_to_unmarshal_str(typespec)
        # ^ this also preemptively checks whether the result type is something we'll know how to unmarshal

        mimetype = user_format_to_mimetype(format)

        if file is not None and not hasattr(file, "read"):
            # assume it's a path
            file = open(os.path.expanduser(file), "wb")
            close_file = True
        else:
            close_file = False

        try:
            # TODO stream=True, use resp.raw and stream through pyarrow?
            try:
                resp = self.session.post(
                    "/inspect",
                    json={"graft": graft, "parameters": params_dict},
                    timeout=timeout,
                    headers={"Accept": mimetype},
                    stream=file is not None,
                )
                resp.raise_for_status()
            except requests.exceptions.Timeout as e:
                raise JobTimeoutError(e) from None

            if file is None:
                if resp.headers["Content-Type"] == _pyarrow_content_type:
                    codec = resp.headers["X-Arrow-Codec"]
                    marshalled = deserialize_pyarrow(resp.content, codec)
                    return unmarshal.unmarshal(result_type, marshalled)
                else:
                    return resp.content
            else:
                shutil.copyfileobj(resp.raw, file)
        finally:
            if close_file:
                file.close()


global_inspect_client = None


def get_global_inspect_client():
    global global_inspect_client
    if global_inspect_client is None:
        global_inspect_client = InspectClient()
    return global_inspect_client
