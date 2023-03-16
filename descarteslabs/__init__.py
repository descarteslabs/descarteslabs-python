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

# This enables the use of namespace packages for descarteslabs
# while still maintaining this __init__.py here in the core
# client package
from pkgutil import extend_path

__path__ = extend_path(__path__, __name__)  # noqa F821

from descarteslabs import auth
from descarteslabs import config
from descarteslabs import exceptions
from descarteslabs.core.client.version import __version__

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
    "config",
    "exceptions",
    "get_settings",
    "select_env",
]
