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

import sys

# these must be imported prior to installing the Finder and Module below
from descarteslabs import auth  # noqa: F401
from descarteslabs import config
from descarteslabs import exceptions

from descarteslabs.config._helpers import (
    DescartesLabsFinder,
    DescartesLabsModule,
)


# install the special Finder
_finder = DescartesLabsFinder()
sys.meta_path.append(_finder)

# install the special Module
sys.modules[__name__] = DescartesLabsModule(sys.modules[__name__])

select_env = config.select_env
get_settings = config.get_settings
AWS_ENVIRONMENT = config.AWS_ENVIRONMENT
GCP_ENVIRONMENT = config.GCP_ENVIRONMENT

__author__ = "Descartes Labs"

__all__ = [
    "AWS_ENVIRONMENT",
    "GCP_ENVIRONMENT",
    "auth",
    "config",
    "exceptions",
    "get_settings",
    "select_env",
]
