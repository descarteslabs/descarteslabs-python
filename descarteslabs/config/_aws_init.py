import importlib
import sys
import types

import descarteslabs

from ._helpers import clone_module


def _setup_aws():
    version = importlib.import_module("descarteslabs._dl_modules.client.version")
    setattr(descarteslabs, "__version__", version.__version__)

    deprecation = importlib.import_module(
        "descarteslabs._dl_modules.client.deprecation"
    )

    # scenes
    dl_scenes = importlib.import_module("descarteslabs._dl_modules.scenes")
    scenes = clone_module("descarteslabs.scenes", dl_scenes)

    # deprecate the hidden internal clients
    setattr(
        scenes,
        "search",
        deprecation.deprecate(removed=["raster_client", "metadata_client"])(
            scenes.search
        ),
    )
    setattr(
        scenes,
        "get_product",
        deprecation.deprecate(removed=["metadata_client"])(scenes.get_product),
    )
    setattr(
        scenes,
        "search_products",
        deprecation.deprecate(removed=["metadata_client"])(scenes.search_products),
    )
    setattr(
        scenes,
        "get_band",
        deprecation.deprecate(removed=["metadata_client"])(scenes.get_band),
    )
    setattr(
        scenes,
        "search_bands",
        deprecation.deprecate(removed=["metadata_client"])(scenes.search_bands),
    )
    setattr(
        scenes,
        "get_derived_band",
        deprecation.deprecate(removed=["metadata_client"])(scenes.get_derived_band),
    )
    setattr(
        scenes,
        "search_derived_bands",
        deprecation.deprecate(removed=["metadata_client"])(scenes.search_derived_bands),
    )
    setattr(
        scenes.Scene,
        "from_id",
        deprecation.deprecate(removed=["metadata_client"])(scenes.Scene.from_id),
    )
    setattr(
        scenes.Scene,
        "ndarray",
        deprecation.deprecate(removed=["raster_client"])(scenes.Scene.ndarray),
    )
    setattr(
        scenes.Scene,
        "download",
        deprecation.deprecate(removed=["raster_client"])(scenes.Scene.download),
    )
    setattr(
        scenes.SceneCollection,
        "__init__",
        deprecation.deprecate(removed=["raster_client"])(
            scenes.SceneCollection.__init__
        ),
    )

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

    utils.__all__ = ["DotDict", "DotList", "display", "save_image"]

    sys.modules["descarteslabs.utils"] = utils
    descarteslabs.utils = utils

    descarteslabs.__all__ = [
        "__version__",
        "geo",
        "scenes",
        "utils",
    ]
