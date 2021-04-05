from collections import namedtuple
import numpy as np
import shapely.geometry as geo
from typing import Sequence

from .conversions import normalize_polygons, AnyShapes
from .tile import Tile
from .utm import utm_to_rowcol, lonlat_to_utm


def rasterize_shape(
    tile: Tile,
    shapes: AnyShapes,
    values: Sequence[int] = None,
    out: np.ndarray = None,
    mode="burn",
    dtype=np.byte,
    shape_coords="lonlat",
    all_touched=False,
) -> np.ndarray:
    """ Rasterize a collection of lon,lat shapes onto a DLTile.
    This is included in this directory's __init__.py, so you can import like:

        from descarteslabs.commmon.dltile import rasterize_shape

    Parameters
    ----------

    tile : Tile
        Defines the output raster geocontext
    shapes : AnyShapes
        Vector shapes to rasterize
    values : Sequence[int], optional
        If given, burns/adds from this sequence of values for the respective
        shape in order. Must have the same length as `shapes`.
        If not given, we burn the shape's index or add 1 by default.
    out : np.ndarray, optional
        If given, writes the output to this array
    mode : str
        'burn' or 'add'
    dtype : np.dtype, optional
        dtype to use if creating `out` array
    shape_coords : str
        'lonlat', 'utm', or 'rowcol'
    all_touched : bool, optional
        If True, affect all pixels where the shapes touch their neighborhood.
        Otherwise, only affect pixels which are contained within each shape.

    Returns
    -------

    out: np.ndarray
        An array containing raster data according to mode. This will be the
        same array as the `out` parameter, if given.

    Raises
    ------

    ValueError
        If invalid parameters are given

    """
    shapes = normalize_polygons(shapes)

    if values is None:
        if mode == "burn":
            values = range(1, len(shapes) + 1)
        elif mode == "add":
            values = (1 for _ in range(len(shapes)))
        else:
            raise ValueError("Expected mode of 'burn' or 'add', got %s" % mode)
    elif len(values) != len(shapes):
        raise ValueError(
            "Expected parameter 'values' to have the same length as parameter "
            "'shapes', got %i and %i." % (len(values), len(shapes))
        )

    if out is None:
        out = np.zeros((tile.tile_extent, tile.tile_extent), dtype=dtype)

    # Convert shapes to pixel coordinates
    tilebox = geo.box(0, 0, tile.tile_extent, tile.tile_extent)
    if shape_coords == "lonlat":
        shapes_rowcol = [
            utm_to_rowcol(
                lonlat_to_utm(shape, zone=tile.zone), tile=tile
            ).intersection(tilebox)
            for shape in shapes
        ]
    elif shape_coords == "utm":
        shapes_rowcol = [
            utm_to_rowcol(shape, tile=tile).intersection(tilebox)
            for shape in shapes
        ]
    elif shape_coords == "rowcol":
        shapes_rowcol = [shape.intersection(tilebox) for shape in shapes]
    else:
        raise ValueError(
            "Parameter shape_coords of function rasterize_shape() must be one "
            "of 'lonlat', 'rowcol', or 'utm'."
        )

    # We use a quadtree algorithm to rasterize.
    TreeNode = namedtuple(
        "TreeNode", ("min_col", "min_row", "max_col", "max_row")
    )
    for shape_i, (shape, value) in enumerate(zip(shapes_rowcol, values)):
        nodes = [TreeNode(0, 0, tile.tile_extent, tile.tile_extent)]
        while len(nodes) > 0:
            node = nodes.pop()
            # "min_nodebox" is the smallest box we need to contain to cover all
            # pixels in the box. "max_nodebox" is the larger box we would need
            # to miss entirely in order to cover no pixels in the box.
            if all_touched:
                min_nodebox = geo.box(
                    node.min_row + 1,
                    node.min_col + 1,
                    node.max_row - 1,
                    node.max_col - 1,
                )
                max_nodebox = geo.box(
                    node.min_row, node.min_col, node.max_row, node.max_col
                )
            else:
                min_nodebox = geo.box(
                    node.min_row + 0.5,
                    node.min_col + 0.5,
                    node.max_row - 0.5,
                    node.max_col - 0.5,
                )
                max_nodebox = min_nodebox
            node_w = node.max_col - node.min_col
            node_h = node.max_row - node.min_row

            if node_w <= 3 and node_h <= 3:
                # Check each pixel for being within shape
                for row in range(node.min_row, node.max_row):
                    for col in range(node.min_col, node.max_col):
                        if all_touched:
                            pixelbox = geo.box(row, col, row + 1.0, col + 1.0)
                            condition = shape.intersects(pixelbox)
                        else:
                            pixel = geo.Point(row + 0.5, col + 0.5)
                            condition = shape.intersects(pixel)
                        if condition:
                            if mode == "burn":
                                out[row, col] = value
                            elif mode == "add":
                                out[row, col] += value
                            else:
                                raise ValueError(
                                    "Expected mode of 'burn' or 'add', got %s"
                                    % mode
                                )
            elif shape.contains(min_nodebox):
                # Apply to all pixels in box
                if mode == "burn":
                    out[
                        node.min_row : node.max_row,
                        node.min_col : node.max_col,
                    ] = value
                elif mode == "add":
                    out[
                        node.min_row : node.max_row,
                        node.min_col : node.max_col,
                    ] += value
                else:
                    raise ValueError(
                        "Expected mode of 'burn' or 'add', got %s" % mode
                    )
            elif max_nodebox.disjoint(shape):
                # No intersection, do nothing.
                pass
            else:
                # There is some intersection.
                # Split node into child nodes
                node_mid_row = int((node.max_row + node.min_row) / 2)
                node_mid_col = int((node.max_col + node.min_col) / 2)

                if node_h <= 1:
                    left_node = TreeNode(
                        node.min_col, node.min_row, node_mid_col, node.max_row
                    )
                    right_node = TreeNode(
                        node_mid_col, node.min_row, node.max_col, node.max_row
                    )
                    nodes.extend([left_node, right_node])
                elif node_w <= 1:
                    upper_node = TreeNode(
                        node.min_col, node.min_row, node.max_col, node_mid_row
                    )
                    lower_node = TreeNode(
                        node.min_col, node_mid_row, node.max_col, node.max_row
                    )
                    nodes.extend([upper_node, lower_node])
                else:
                    ul_node = TreeNode(
                        node.min_col, node.min_row, node_mid_col, node_mid_row
                    )
                    ur_node = TreeNode(
                        node_mid_col, node.min_row, node.max_col, node_mid_row
                    )
                    ll_node = TreeNode(
                        node.min_col, node_mid_row, node_mid_col, node.max_row
                    )
                    lr_node = TreeNode(
                        node_mid_col, node_mid_row, node.max_col, node.max_row
                    )
                    nodes.extend([ul_node, ur_node, ll_node, lr_node])

    return out
