"""
The Scenes submodule provides a higher-level, object-based
interface that makes most interactions with the Descartes Labs platform easier.

* `Collection`: convenience methods for mapping and filtering
* `geocontext`: consistent spatial parameters to use when loading a raster
* `Scene`: metadata about a single scene
* `SceneCollection`: conveniently work with Scenes in aggregate
* `search <search.search>`: search for Scenes
* `display <display.display>`: display ndarrays with matplotlib

It's available under ``dl.scenes``.

Examples
========

Define our Area of Interest:

>>> aoi_geometry = {
...     'type': 'Polygon',
...     'coordinates': ((
...         (-93.52300099792355, 41.241436141055345),
...         (-93.7138666, 40.703737),
...         (-94.37053769704536, 40.83098709945576),
...         (-94.2036617, 41.3717716),
...         (-93.52300099792355, 41.241436141055345)
...     ),)
... }

Search for Scenes within it:

>>> import descarteslabs as dl
>>> scenes, ctx = dl.scenes.search(aoi_geometry,
...                                products=["landsat:LC08:PRE:TOAR"],
...                                start_datetime="2013-07-01",
...                                end_datetime="2013-09-01",
...                                limit=10)
>>> scenes
SceneCollection of 10 scenes
  * Dates: Jul 07, 2013 to Aug 24, 2013
  * Products: landsat:LC08:PRE:TOAR: 10
>>> ctx
AOI(geometry=<shapely.geom...t 0x10291c2e8>,
    resolution=60,
    crs='EPSG:32615',
    align_pixels=True,
    bounds=(-94.37053769704536, 40.703737, -93.52300099792355, 41.3717716),
    shape=None)

Quickly inspect metadata:

>>> scenes.each.properties.id
'landsat:LC08:PRE:TOAR:meta_LC80260312013188_v1'
'landsat:LC08:PRE:TOAR:meta_LC80260312013204_v1'
'landsat:LC08:PRE:TOAR:meta_LC80260312013220_v1'
'landsat:LC08:PRE:TOAR:meta_LC80260312013236_v1'
'landsat:LC08:PRE:TOAR:meta_LC80260322013188_v1'
'landsat:LC08:PRE:TOAR:meta_LC80260322013204_v1'
'landsat:LC08:PRE:TOAR:meta_LC80260322013220_v1'
'landsat:LC08:PRE:TOAR:meta_LC80260322013236_v1'
...
>>> scenes.each.properties.date.month
7
7
7
7
7
7
8
8
...

Load and display a scene:

>>> scene = scenes[-2]
>>> arr = scene.ndarray("red green blue", ctx)
>>> dl.scenes.display(arr)

.. image:: https://cdn.descarteslabs.com/docs/api/scenes/init-1.png

Monthly median composites of NDVI within our area of interest:

>>> import numpy as np
>>> monthly_composites = {}
>>> for month, month_scenes in scenes.groupby("properties.date.month"):
...     stack = month_scenes.stack("red nir", ctx)
...     red, nir = stack[:, 0], stack[:, 1]
...     ndvi = (nir - red) / (nir + red)
...     ndvi_composite = np.ma.median(ndvi, axis=0)
...     monthly_composites[month] = ndvi_composite

And the mean NDVI value of each month's composite is:

>>> {month: composite.mean() for month, composite in monthly_composites.items()}
{7: 0.32535368527404873, 8: 0.41194597940346561}

View the NDVI composites:

>>> dl.scenes.display(*monthly_composites.values(), title=list(monthly_composites.keys()))

.. image:: https://cdn.descarteslabs.com/docs/api/scenes/init-2.png

"""

from .geocontext import AOI, DLTile, GeoContext
from .display import display
from .search import search
from .scene import Scene
from .collection import Collection
from .scenecollection import SceneCollection

__all__ = ["Scene", "SceneCollection", "Collection", "AOI", "DLTile", "GeoContext", "search", "display"]
