from typing import Type, List

from google.protobuf.message import Message


def which_has(msg: Message) -> str:
    """
    Like WhichOneof, but for our fake oneofs using "has_" fields.
    Returns the non-"has_" field name which is set on `msg`.
    """
    # TODO remove this once we can use oneofs in our protos
    for field_descriptor, value in msg.ListFields():
        if not field_descriptor.name.startswith("has_"):
            continue
        if value is True:
            return field_descriptor.name[4:]
    raise ValueError(
        "Invalid {} protobuf: none of the has_ values are set.".format(
            type(msg).__name__
        )
    )


def has_options(msg_type: Type[Message]) -> List[str]:
    "Names of all the has_ fields on a message, with the has_ removed."
    return [
        desc.name[4:]
        for desc in msg_type.DESCRIPTOR.fields
        if desc.name.startswith("has_")
    ]
