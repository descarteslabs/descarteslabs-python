"""
The Scenes submodule provides a higher-level, object-based
interface that makes most interactions with the Descartes Labs platform easier.

* :doc:`Collection <docs/collection>`: convenience methods for mapping and filtering
* :doc:`geocontext <docs/geocontext>`: consistent spatial parameters to use when loading a raster
* :doc:`Scene <docs/scene>`: metadata about a single scene
* :doc:`SceneCollection <docs/scenecollection>`: conveniently work with Scenes in aggregate
* :doc:`search <docs/search>`: search for Scenes
* :doc:`display <docs/display>`: display ndarrays with matplotlib

It's available under ``dl.scenes``.

Example
=======

Define our Area of Interest:

.. ipython::

    In [1]: aoi_geometry = {
       ...:     'type': 'Polygon',
       ...:     'coordinates': ((
       ...:         (-93.52300099792355, 41.241436141055345),
       ...:         (-93.7138666, 40.703737),
       ...:         (-94.37053769704536, 40.83098709945576),
       ...:         (-94.2036617, 41.3717716),
       ...:         (-93.52300099792355, 41.241436141055345)
       ...:     ),)
       ...: }

Search for Scenes within it:

.. ipython::

   In [2]: import descarteslabs as dl

   In [3]: scenes, ctx = dl.scenes.search(aoi_geometry,
      ...:                                products=["landsat:LC08:PRE:TOAR"],
      ...:                                start_datetime="2013-07-01",
      ...:                                end_datetime="2013-09-01",
      ...:                                limit=10)

   @doctest
   In [4]: scenes
   Out[4]:
   SceneCollection of 10 scenes
     * Dates: Jul 07, 2013 to Aug 24, 2013
     * Products: landsat:LC08:PRE:TOAR: 10

   In [5]: ctx
   Out[5]:
   AOI(geometry=<shapely.geom...t 0x10291c2e8>,
       resolution=60,
       crs='EPSG:32615',
       align_pixels=True,
       bounds=(-94.37053769704536, 40.703737, -93.52300099792355, 41.3717716),
       shape=None)

   In [6]: ctx_lowres = ctx.assign(resolution=60)

Quickly inspect metadata:

.. ipython::

    In [7]: scenes = scenes.sorted("properties.date")

    @doctest
    In [8]: scenes.each.properties.id
    Out[8]:
    u'landsat:LC08:PRE:TOAR:meta_LC80260312013188_v1'
    u'landsat:LC08:PRE:TOAR:meta_LC80260322013188_v1'
    u'landsat:LC08:PRE:TOAR:meta_LC80270312013195_v1'
    u'landsat:LC08:PRE:TOAR:meta_LC80260312013204_v1'
    u'landsat:LC08:PRE:TOAR:meta_LC80260322013204_v1'
    u'landsat:LC08:PRE:TOAR:meta_LC80270312013211_v1'
    u'landsat:LC08:PRE:TOAR:meta_LC80260312013220_v1'
    u'landsat:LC08:PRE:TOAR:meta_LC80260322013220_v1'
    ...

    @doctest
    In [9]: scenes.each.properties.date.month
    Out[9]:
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

.. ipython::

    In [10]: scene = scenes[-1]

    In [11]: arr = scene.ndarray("red green blue", ctx_lowres)

    @savefig scene.png
    In [12]: dl.scenes.display(arr)

Monthly median composites of NDVI within our area of interest:

.. ipython::

    In [13]: import numpy as np

    In [14]: monthly_composites = {}

    In [15]: for month, month_scenes in scenes.groupby("properties.date.month"):
       ....:     stack = month_scenes.stack("red nir", ctx_lowres)
       ....:     stack = stack.astype(float)  # otherwise division for NDVI truncates to uint16
       ....:     red, nir = stack[:, 0], stack[:, 1]
       ....:     ndvi = (nir - red) / (nir + red)
       ....:     ndvi_composite = np.ma.median(ndvi, axis=0)
       ....:     monthly_composites[month] = ndvi_composite
       ....:

And the mean NDVI value of each month's composite is:

.. ipython::

    @doctest
    In [16]: {month: composite.mean() for month, composite in monthly_composites.items()}
    Out[16]: {7: 0.32396951619824726, 8: 0.3973330207454807}

View the NDVI composites:

.. ipython::

    @savefig composite.png
    In [17]: dl.scenes.display(*monthly_composites.values(), title=list(monthly_composites.keys()))

"""

from .geocontext import AOI, DLTile, GeoContext
from ._display import display
from ._search import search
from .scene import Scene
from .collection import Collection
from .scenecollection import SceneCollection

__all__ = ["Scene", "SceneCollection", "Collection", "AOI", "DLTile", "GeoContext", "search", "display"]
