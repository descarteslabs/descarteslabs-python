import pytest
import shapely.geometry

from ..... import scenes

from .. import GeoContext
from ...containers import Tuple
from ...primitives import Int, Float


def test_from_scenes_wrong_type():
    with pytest.raises(
        TypeError, match=r"expected a `descarteslabs\.scenes\.GeoContext`"
    ):
        GeoContext.from_scenes("foo")


def test_from_scenes_aoi():
    aoi = scenes.AOI(
        geometry=shapely.geometry.box(-60.0, 30.0, -50.0, 40.0),
        resolution=1,
        crs="EPSG:4326",
        align_pixels=False,
    )
    ctx = GeoContext.from_scenes(aoi)
    assert ctx.graft[ctx.graft["returns"]][0] == "wf.GeoContext.create"

    promoted = GeoContext._promote(aoi)
    assert promoted.graft[promoted.graft["returns"]][0] == "wf.GeoContext.create"


def test_from_scenes_tile():
    tile_dict = {
        "geometry": {
            "coordinates": [
                [
                    [-100.10534464886125, 59.94175277369993],
                    [-99.91065247366876, 59.943240309707676],
                    [-99.91334037259435, 60.040922421458546],
                    [-100.10860694364838, 60.039429047992876],
                    [-100.10534464886125, 59.94175277369993],
                ]
            ],
            "type": "Polygon",
        },
        "properties": {
            "cs_code": "EPSG:32614",
            "geotrans": [438240.0, 20.0, 0, 6656320.0, 0, -20.0],
            "key": "512:16:20.0:14:-6:649",
            "outputBounds": [438240.0, 6645440.0, 449120.0, 6656320.0],
            "pad": 16,
            "proj4": "+proj=utm +zone=14 +datum=WGS84 +units=m +no_defs ",
            "resolution": 20.0,
            "ti": -6,
            "tilesize": 512,
            "tj": 649,
            "zone": 14,
        },
        "type": "Feature",
    }

    tile = scenes.DLTile(tile_dict)
    ctx = GeoContext.from_scenes(tile)
    assert ctx.graft[ctx.graft["returns"]][0] == "wf.GeoContext.from_dltile_key"

    promoted = GeoContext._promote(tile)
    assert (
        promoted.graft[promoted.graft["returns"]][0] == "wf.GeoContext.from_dltile_key"
    )


def test_from_scenes_xyztile():
    tile = scenes.XYZTile(3, 5, 4)
    ctx = GeoContext.from_scenes(tile)
    assert ctx.graft[ctx.graft["returns"]][0] == "wf.GeoContext.from_xyz_tile"

    promoted = GeoContext._promote(tile)
    assert promoted.graft[promoted.graft["returns"]][0] == "wf.GeoContext.from_xyz_tile"


def test_promote_dltile_from_key():
    ctx = GeoContext.from_dltile_key("500:0:10.0:13:-17:790")
    assert GeoContext._promote(ctx) is ctx


def test_promote_xyztile_from_xyz():
    ctx = GeoContext.from_xyz_tile(3, 5, 4)
    assert GeoContext._promote(ctx) is ctx


@pytest.mark.parametrize("attr", ["arr_shape", "gdal_geotrans", "projected_bounds"])
def test_readonly_attributes(attr):
    type_params = GeoContext._type_params[0]
    ctx = GeoContext.from_xyz_tile(3, 5, 4)

    assert isinstance(getattr(ctx, attr), type_params[attr])


def test_index_to_coords():
    aoi = scenes.AOI(
        geometry=shapely.geometry.box(-60.0, 30.0, -50.0, 40.0),
        resolution=1,
        crs="EPSG:4326",
        align_pixels=False,
    )
    ctx = GeoContext.from_scenes(aoi)

    coords = ctx.index_to_coords(0, 0)
    assert isinstance(coords, Tuple[Float, Float])


def test_coords_to_index():
    aoi = scenes.AOI(
        geometry=shapely.geometry.box(-60.0, 30.0, -50.0, 40.0),
        resolution=1,
        crs="EPSG:4326",
        align_pixels=False,
    )
    ctx = GeoContext.from_scenes(aoi)
    ctx = GeoContext._promote(ctx)

    index = ctx.coords_to_index(0.0, 1.0)
    assert isinstance(index, Tuple[Int, Int])
