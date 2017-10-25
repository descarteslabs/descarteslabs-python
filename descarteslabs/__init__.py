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

# flake8: noqa

__version__ = "0.5.0"

from .clients.auth import Auth
descartes_auth = Auth.from_environment_or_token_json()

from .clients.services.metadata import Metadata
from .clients.services.places import Places
from .clients.services.raster import Raster

metadata = Metadata()
places = Places()
raster = Raster()
