"""
The Scenes submodule provides a higher-level, object-based
interface to the Descartes Labs Platform catalog of imagery and
raster services that makes most interactions easier.

* :doc:`Collection <docs/collection>`: convenience methods for mapping and filtering
* :doc:`GeoContext <docs/geocontext>`: consistent spatial parameters to use when loading a raster
* :doc:`Scene <docs/scene>`: metadata about a single scene
* :doc:`SceneCollection <docs/scenecollection>`: conveniently work with Scenes in aggregate
* :doc:`search <docs/search>`: search for Scenes
* :doc:`display <docs/display>`: display ndarrays with matplotlib

It's available under ``descarteslabs.scenes``.
"""

from ..common.geo import AOI, DLTile, XYZTile, GeoContext
from ..common.collection import Collection
from ..common.display import display, save_image
from .search_api import (
    get_product,
    get_band,
    get_derived_band,
    search,
    search_products,
    search_bands,
    search_derived_bands,
)
from .scene import Scene
from .scenecollection import SceneCollection

__all__ = [
    "Scene",
    "SceneCollection",
    "Collection",
    "AOI",
    "DLTile",
    "XYZTile",
    "GeoContext",
    "search",
    "get_product",
    "get_band",
    "get_derived_band",
    "search_products",
    "search_bands",
    "search_derived_bands",
    "display",
    "save_image",
]
