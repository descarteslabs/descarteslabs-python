# Copyright 2018 Descartes Labs.
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
from descarteslabs.client.services.metadata import Metadata
from descarteslabs.client.services.places import Places
from descarteslabs.client.services.raster import Raster
from descarteslabs.client.services.storage import Storage, cached
from descarteslabs.client.services.catalog import Catalog
from descarteslabs.client.services.tasks import AsyncTasks, Tasks, FutureTask, CloudFunction
from descarteslabs.client.services.vector import Vector

__all__ = [
    "Raster",
    "Metadata",
    "Places",
    "Storage",
    "Catalog",
    "AsyncTasks",
    "Tasks",
    "FutureTask",
    "CloudFunction",
    "Vector"
]
