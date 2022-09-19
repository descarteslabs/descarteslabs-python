import importlib
import sys
import types

import descarteslabs

from ._helpers import clone_module


def _setup_aws():
    version = importlib.import_module("descarteslabs._dl_modules.client.version")
    setattr(descarteslabs, "__version__", version.__version__)

    # catalog
    dl_catalog = importlib.import_module("descarteslabs._dl_modules.catalog")
    catalog = clone_module("descarteslabs.catalog", dl_catalog)
    sys.modules[catalog.__name__] = catalog
    setattr(descarteslabs, "catalog", catalog)

    # scenes
    dl_scenes = importlib.import_module("descarteslabs._dl_modules.scenes")
    scenes = clone_module("descarteslabs.scenes", dl_scenes)
    sys.modules[scenes.__name__] = scenes
    setattr(descarteslabs, "scenes", scenes)

    # geo
    dl_geo = importlib.import_module("descarteslabs._dl_modules.common.geo")
    geo = clone_module("descarteslabs.geo", dl_geo)
    sys.modules[geo.__name__] = geo
    setattr(descarteslabs, "geo", geo)

    # utils
    utils = types.ModuleType("descarteslabs.utils")

    dotdict = importlib.import_module("descarteslabs._dl_modules.common.dotdict")
    utils.DotDict = dotdict.DotDict
    utils.DotList = dotdict.DotList

    display = importlib.import_module("descarteslabs._dl_modules.common.display")
    utils.display = display.display
    utils.save_image = display.save_image

    property_filtering = importlib.import_module(
        "descarteslabs._dl_modules.common.property_filtering"
    )
    utils.Properties = property_filtering.Properties

    utils.__all__ = [
        "DotDict",
        "DotList",
        "Properties",
        "display",
        "save_image",
    ]

    sys.modules["descarteslabs.utils"] = utils
    descarteslabs.utils = utils

    descarteslabs.__all__ = [
        "__version__",
        "catalog",
        "geo",
        "scenes",
        "utils",
    ]
