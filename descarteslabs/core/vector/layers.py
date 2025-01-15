# © 2025 EarthDaily Analytics Corp.
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

try:
    from ipyleaflet import VectorTileLayer
    from traitlets import Dict
except ImportError:
    raise ImportError(
        "The visualization support in the `descarteslabs.vector` Python package"
        " requires the `viz` extra to be installed."
        " Please run `pip install descarteslabs[viz]` and try again."
        " Alternatively you can install the `ipyleaflet` package directly."
    )


class DLVectorTileLayer(VectorTileLayer):
    """
    A minimal wrapper around VectorTileLayer to add fetch_options
    """

    fetch_options = Dict({"credentials": "include"}).tag(sync=True, o=True)
