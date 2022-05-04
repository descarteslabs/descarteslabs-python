from typing import Union

from ...proto.formats import formats_pb2
from ..proto_munging import user_dict_to_has_proto


DEFAULTS = {
    formats_pb2.Geotiff: {
        "compression": "GEOTIFFCOMPRESSION_LZW",
        "overview_resampler": "GEOTIFFOVERVIEWRESAMPLER_NEAREST",
    },
    formats_pb2.Pyarrow: {"compression": "PYARROWCOMPRESSION_LZ4"},
}


def user_format_to_proto(params: Union[dict, str]) -> formats_pb2.Format:
    if isinstance(params, str):
        params = {"type": params}
    else:
        if "type" not in params:
            raise ValueError(
                "The format dictionary must include a serialization type "
                "(like `'type': 'json'`), but key 'type' does not exist."
            )

    return user_dict_to_has_proto(params, formats_pb2.Format, DEFAULTS)
