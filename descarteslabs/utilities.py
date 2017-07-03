# Copyright 2017 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import struct

from .addons import blosc, numpy as np
from .exceptions import ServerError


def as_json_string(str_or_dict):
    if not str_or_dict:
        return str_or_dict
    elif isinstance(str_or_dict, dict):
        return json.dumps(str_or_dict)
    else:
        return str_or_dict


def read_blosc_buffer(data):
    header = data.read(16)

    _, size, _, compressed_size = struct.unpack('<IIII', header)
    body = data.read(compressed_size - 16)

    return size, header + body


def read_blosc_array(metadata, data):
    output = np.empty(metadata['shape'], dtype=np.dtype(metadata['dtype']))
    ptr = output.__array_interface__['data'][0]

    for _ in metadata['chunks']:
        raw_size, buffer = read_blosc_buffer(data)
        blosc.decompress_ptr(buffer, ptr)
        ptr += raw_size

    bytes_received = ptr - output.__array_interface__['data'][0]

    if bytes_received != output.nbytes:
        raise ServerError("Did not receive complete array (got {}, expected {})".format(
            bytes_received, output.nbytes))

    return output


def read_blosc_string(metadata, data):
    output = b''

    for _ in metadata['chunks']:
        _, buffer = read_blosc_buffer(data)
        output += blosc.decompress(buffer)

    return output
