GeoContext
----------

.. default-role:: any

Datasets in the Descartes Labs catalog have many different resolutions and
projections. In two different images, even covering the same place on Earth,
the pixels ``(i, j)`` usually correspond to two different points on the ground.

GeoContexts are a way to ensure multiple images from different sources
are **spatially compatible**---that is, they all have the same shape
(same width and height, in pixels), and the same pixel in each image
corresponds to the same area on Earth.

They do this by simply capturing all the spatial parameters that affect how
imagery is rasterized---namely output resolution, coordinate reference system,
and bounding box---in one object that can be passed into different method calls.
In typical use, these contexts are created for you with reasonable defaults,
so you only need to understand the different parameters when you need more control.

The different subclasses of `GeoContext` implement different functionality.

* `AOI` clips to arbitrary geometry, and lets you specify
  any output resolution and projection.
* `DLTile` helps you split large regions up into a grid of
  any spacing and resolution, and represents a single tile in that grid, in UTM projection.

.. automodule:: descarteslabs.common.geo
  :autosummary:
  :members:
  :inherited-members:
  :special-members: __geo_interface__
