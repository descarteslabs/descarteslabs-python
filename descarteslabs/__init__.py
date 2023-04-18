"""Descartes Labs Python Client

.. code-block:: bash

    pip install descarteslabs[complete]

Documentation is available at https://docs.descarteslabs.com.

Source code and version information is at
https://github.com/descarteslabs/descarteslabs-python.

The Descartes Labs Platform simplifies analysis of **global-scale raster data**
by providing:

    * Access to a catalog of petabytes of disparate geospatial data,
      all normalized and interoperable through one **common interface**
    * A Python client library to access these systems
"""

# Copyright 2018-2023 Descartes Labs.
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

# This enables the use of namespace packages for descarteslabs
# while still maintaining this __init__.py here in the core
# client package
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)  # noqa F821

from descarteslabs import auth
from descarteslabs import config
from descarteslabs import exceptions
from descarteslabs.core.client.version import __version__

from descarteslabs import geo
from descarteslabs import utils
from descarteslabs import catalog
from descarteslabs import scenes

select_env = config.select_env
get_settings = config.get_settings
AWS_ENVIRONMENT = config.AWS_ENVIRONMENT
GCP_ENVIRONMENT = config.GCP_ENVIRONMENT

__author__ = "Descartes Labs"

__all__ = [
    "__version__",
    "AWS_ENVIRONMENT",
    "GCP_ENVIRONMENT",
    "auth",
    "catalog",
    "config",
    "exceptions",
    "geo",
    "get_settings",
    "scenes",
    "select_env",
    "utils",
]
