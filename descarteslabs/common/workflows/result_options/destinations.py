from descarteslabs.common.proto.destinations import destinations_pb2

from .helpers import (
    user_options_to_proto,
    user_facing_options,
    proto_field_type_to_python_type,
)


DEFAULTS = {
    destinations_pb2.Download: {},
    destinations_pb2.Email: {
        "subject": "Your Computation is Finished",
        "body": "The computation of your job has finished.",
    },
}


destination_name_to_type = {
    field_name: getattr(destinations_pb2, field_descriptor.message_type.name)
    for field_name, field_descriptor in destinations_pb2.Destination.DESCRIPTOR.fields_by_name.items()
    if not field_name.startswith("has_")
}


destination_message_type_to_field_names_and_types = {
    getattr(destinations_pb2, message_name): {
        field_name: proto_field_type_to_python_type[field_descriptor.type]
        for field_name, field_descriptor in message_descriptor.fields_by_name.items()
    }
    for message_name, message_descriptor in destinations_pb2.DESCRIPTOR.message_types_by_name.items()
    if not message_name == "Destination"
}


def user_destination_to_proto(destination: dict) -> destinations_pb2.Destination:
    # Classic dictionary mutation
    destination_copy = destination.copy()
    try:
        destination_name = destination_copy.pop("type")
    except KeyError:
        raise ValueError(
            "The destination dictionary must include a destination type "
            "(like `'type': 'download'`), but key 'type' does not exist."
        )

    try:
        specific_type = destination_name_to_type[destination_name]
    except KeyError:
        raise ValueError(
            "Unknown destination {!r}. Must be one of: {}".format(
                destination_name, list(destination_name_to_type)
            )
        )

    return user_options_to_proto(
        destination_copy,
        destinations_pb2.Destination,
        specific_type,
        destination_message_type_to_field_names_and_types,
        DEFAULTS,
    )


def destination_proto_to_user_facing_destination(
    destination: destinations_pb2.Destination
) -> dict:
    if destination.has_download:
        specific_format = destination.download
        type_ = "download"
    elif destination.has_email:
        specific_format = destination.email
        type_ = "email"
    else:
        raise ValueError(
            "Invalid Destination protobuf: none of the has_ values are set."
        )

    return user_facing_options(
        type_, specific_format, destination_message_type_to_field_names_and_types
    )
