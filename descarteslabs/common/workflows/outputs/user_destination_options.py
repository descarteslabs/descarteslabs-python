from typing import Union
import json

from descarteslabs.common.proto.destinations import destinations_pb2
from descarteslabs.common.workflows.proto_munging import user_dict_to_has_proto

from descarteslabs.catalog import Image, DocumentState


DEFAULTS = {
    destinations_pb2.Email: {
        "subject": "Your job has completed",
        "body": "Your Workflows job is done.",
    }
}


def user_destination_to_proto(
    params: Union[dict, str, Image]
) -> destinations_pb2.Destination:
    if isinstance(params, str):
        params = {"type": params}
    elif isinstance(params, Image):
        params = {"type": "catalog", "image": params}
    else:
        if "type" not in params:
            raise ValueError(
                "The destination dictionary must include a destination type "
                "(like `'type': 'download'`), but key 'type' does not exist."
            )

    if params["type"].lower() == "catalog":
        try:
            img = params.pop("image")
        except KeyError:
            if (
                "name" not in params
                and "product_id" not in params
                and "attributes_json" not in params
            ):
                raise ValueError(
                    "For the Catalog destination, the options dict must contain an `image` field "
                    "with the `dl.catalog.Image` to upload to."
                ) from None
        params = _image_to_catalog_params(img, **params)

    return user_dict_to_has_proto(params, destinations_pb2.Destination, DEFAULTS)


def _image_to_catalog_params(img: Image, **non_img_args) -> dict:
    if not img.id:
        raise ValueError("Catalog Image must have 'id' field.")
    if img.product.state != DocumentState.SAVED:
        raise ValueError(
            "Product {!r} has not been saved. Please save before uploading images".format(
                img.product_id
            )
        )

    cereal = img.serialize(modified_only=False, jsonapi_format=False)

    return {
        "type": "catalog",
        "name": cereal.pop("name"),
        "product_id": cereal.pop("product_id"),
        "attributes_json": {k: json.dumps(v) for k, v in cereal.items()},
        **non_img_args,
    }
