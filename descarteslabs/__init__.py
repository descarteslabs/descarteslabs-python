# Copyright 2018-2019 Descartes Labs.
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
"""Descartes Labs Python Client

.. code-block:: bash

    pip install descarteslabs[complete]

Documentation is available at https://docs.descarteslabs.com.

Source code and version information is at https://github.com/descarteslabs/descarteslabs-python.

The Descartes Labs Platform simplifies analysis of **global-scale raster data** by providing:

  * Access to a catalog of petabytes of disparate geospatial data,
    all normalized and interoperable through one **common interface**
  * **Infrastructure** to parallelize any code across thousands of machines co-located with that data
  * The ability to **add new data to that catalog**-whether the output of analysis on existing data,
    or from a proprietary source-which can then be used as an input for more analysis
  * A Python client library to access these systems
  * Web interfaces to `browse this catalog <https://catalog.descarteslabs.com/>`_
    and `view imagery <https://viewer.descarteslabs.com/>`_, including your data you create
"""

import sys
import warnings

if sys.version_info.major == 2:
    with warnings.catch_warnings():
        warnings.simplefilter("once")
        warnings.warn(
            DeprecationWarning(
                """
You are using the Descartes Labs Python client library with Python 2.
After January 1st, 2020, all future client library releases will be Python 3 only.
For more information, please contact support@descarteslabs.com.
For porting to Python 3, please visit https://docs.python.org/3/howto/pyporting.html."""
            ),
            DeprecationWarning,
            2,
        )
else:
    # By importing cloudpickle here, we avoid a DeprecationWarning
    import cloudpickle

# Turn on deprecation warnings for all API clients
warnings.simplefilter("always", DeprecationWarning)

# flake8: noqa
from .client import exceptions
from .client import services
from .client.auth import Auth
from .client.services import *
from .client.version import __version__
from .common.property_filtering import GenericProperties

try:
    from . import scenes
except ImportError:
    pass

try:
    from . import vectors
except ImportError:
    pass

try:
    from . import catalog
except ImportError:
    pass


descartes_auth = Auth.from_environment_or_token_json()
metadata = Metadata(auth=descartes_auth)
places = Places(auth=descartes_auth)
raster = Raster(auth=descartes_auth)
storage = Storage(auth=descartes_auth)
tasks = Tasks(auth=descartes_auth)
vector = Vector(auth=descartes_auth)
properties = GenericProperties()

__all__ = [
    "descartes_auth",
    "metadata",
    "places",
    "raster",
    "storage",
    "tasks",
    "vector",
    "properties",
    "Auth",
    "exceptions",
] + services.__all__
__author__ = "Descartes Labs"
