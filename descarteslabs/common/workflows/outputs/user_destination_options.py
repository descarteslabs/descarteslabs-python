from typing import Union

from descarteslabs.common.proto.destinations import destinations_pb2
from descarteslabs.common.workflows.proto_munging import user_dict_to_has_proto


DEFAULTS = {
    destinations_pb2.Email: {
        "subject": "Your job has completed",
        "body": "Your Workflows job is done.",
    }
}


def user_destination_to_proto(params: Union[dict, str]) -> destinations_pb2.Destination:
    if isinstance(params, str):
        if "@" in params:
            params = {"type": "email", "to": params}
        else:
            params = {"type": params}
    else:
        if "type" not in params:
            raise ValueError(
                "The destination dictionary must include a destination type "
                "(like `'type': 'download'`), but key 'type' does not exist."
            )

    return user_dict_to_has_proto(params, destinations_pb2.Destination, DEFAULTS)
