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


def setup_aws():
    import importlib
    import sys
    import types

    # ultimately I am not sure everything should be handled in one
    # place, it might be better to distribute this around...

    descarteslabs = sys.modules["descarteslabs"]

    # auth
    auth = importlib.import_module("descarteslabs._dl_modules.client.auth")
    sys.modules["descarteslabs.auth"] = auth
    descarteslabs.auth = auth

    # config
    config = types.ModuleType("descarteslabs.config")
    sys.modules["descarteslabs.config"] = config
    descarteslabs.config = config

    # exceptions
    exceptions = importlib.import_module("descarteslabs._dl_modules.client.exceptions")
    sys.modules["descarteslabs.exceptions"] = exceptions
    descarteslabs.exceptions = exceptions

    # utils
    utils = types.ModuleType("descarteslabs.utils")

    dotdict = importlib.import_module("descarteslabs._dl_modules.common.dotdict")
    utils.DotDict = dotdict.DotDict
    utils.DotList = dotdict.DotList
    utils.__all__ = ["DotDict", "DotList"]

    sys.modules["descarteslabs.utils"] = utils
    descarteslabs.utils = utils

    # scenes
    scenes = importlib.import_module("descarteslabs._dl_modules.scenes")
    sys.modules["descarteslabs.scenes"] = scenes
    descarteslabs.scenes = scenes

    # geo
    geo = types.ModuleType("descarteslabs.geo")

    geo.GeoContext = scenes.GeoContext
    geo.AOI = scenes.AOI
    geo.DLTile = scenes.DLTile
    geo.XYZTile = scenes.XYZTile

    sys.modules["descarteslabs.geo"] = geo
    descarteslabs.geo = geo
