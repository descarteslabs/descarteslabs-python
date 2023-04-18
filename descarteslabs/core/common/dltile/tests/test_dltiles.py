# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from unittest import TestCase
import numpy as np

from ..tile import Tile, Grid
from ..utm import lonlat_to_utm
from ..exceptions import (
    InvalidLatLonError,
    InvalidRowColError,
    InvalidTileError,
    InvalidShapeError,
)


class TileTest(TestCase):
    """Tests Tile class"""

    def test_from_key_1(self):
        key = "2048:16:30.2:15:3:80"
        tile = Tile.from_key(key)
        assert tile.key == key
        assert tile.zone == 15
        assert tile.resolution == 30.2
        assert tile.tilesize == 2048
        assert tile.path == 3
        assert tile.row == 80

    def test_from_key_2(self):
        key = "2048:16:30:15:-5:15"  # no decimal in key
        tile = Tile.from_key(key)
        assert tile.key == "2048:16:30.0:15:-5:15"  # decimal included
        assert tile.resolution == 30
        assert tile.path == -5
        assert tile.row == 15

    def test_get_invalid_dlkey_1(self):
        invalid_key = "2048:16:30.0:0:3:80"  # tilesize must be greater than zero
        with pytest.raises(InvalidTileError):
            Tile.from_key(invalid_key)

    def test_get_invalid_dlkey_2(self):
        invalid_key = "blah:16:30.0:1:3:80"  # invalid type
        with pytest.raises(InvalidTileError):
            Tile.from_key(invalid_key)

    def test_get_invalid_dlkey_3(self):
        invalid_key = "2048:16.4:30.0:15:3:80"  # pad must be int
        with pytest.raises(InvalidTileError):
            Tile.from_key(invalid_key)

    def test_dlkeys_subtile(self):
        params = {
            "resolution": 1,
            "tilesize": 1024,
            "pad": 0,
        }
        sub = 8
        lat, lon = 35.691544, -105.944183

        tile = Grid(**params).tile_from_lonlat(lon, lat)
        tiles = [t for t in tile.subtile(sub)]
        assert len(tiles) == sub * sub
        for t in tiles:
            assert t.tilesize == params["tilesize"] // sub

    def test_dlkeys_subtile_with_params(self):
        params = {
            "resolution": 1,
            "tilesize": 1024,
            "pad": 0,
        }
        new_resolution = 2
        new_pad = 13
        sub = 4
        lat, lon = 35.691544, -105.944183

        tile = Grid(**params).tile_from_lonlat(lon, lat)
        tiles = [
            t for t in tile.subtile(sub, new_resolution=new_resolution, new_pad=new_pad)
        ]
        assert len(tiles) == sub * sub
        for t in tiles:
            assert np.allclose(
                t.tilesize * new_resolution * sub,
                params["tilesize"] * params["resolution"],
            )
            assert t.pad == new_pad
            assert t.resolution == new_resolution

    def test_dlkeys_subtile_error_1(self):
        params = {
            "resolution": 1,
            "tilesize": 1024,
            "pad": 0,
        }
        sub = 11  # does not evenly divide tilesize
        lat, lon = 35.691544, -105.944183

        tile = Grid(**params).tile_from_lonlat(lon, lat)
        with pytest.raises(InvalidTileError):
            [t for t in tile.subtile(sub)]

    def test_dlkeys_subtile_error_2(self):
        params = {
            "resolution": 1,
            "tilesize": 1024,
            "pad": 0,
        }
        sub = 8
        lat, lon = 35.691544, -105.944183

        tile = Grid(**params).tile_from_lonlat(lon, lat)
        with pytest.raises(InvalidTileError):
            [t for t in tile.subtile(sub, new_resolution=13)]  # does not divide

    def test_rowcol_conversions(self):
        # get a polar tile
        tile = Grid(tilesize=1000, resolution=1000, pad=0).tile_from_lonlat(
            lon=0.0, lat=90.0
        )
        x, y = 567, 133
        lon, lat = tile.rowcol_to_lonlat(x, y)
        row, col = tile.lonlat_to_rowcol(lon, lat)
        assert row == x
        assert col == y

    def test_invalid_rowcol(self):
        tile = Grid(tilesize=1000, resolution=1000, pad=0).tile_from_lonlat(
            lon=0.0, lat=90.0
        )
        x, y = [1, 1, 2, 3, 5], [42]
        with pytest.raises(InvalidRowColError):
            lon, lat = tile.rowcol_to_lonlat(x, y)

    def test_assign(self):
        tile1 = Tile.from_key("2048:16:0.2:15:3:80")
        assert tile1.resolution == 0.2
        tile2 = tile1.assign(resolution=1)
        assert tile2.resolution == 1
        assert tile1.pad == tile2.pad
        assert tile1.tilesize == tile2.tilesize

    def test_bad_assign(self):
        tile1 = Tile.from_key("2048:16:0.2:15:3:80")
        with pytest.raises(InvalidTileError):
            # incompatible resolution and tilesize
            tile1.assign(resolution=1, tilesize=512)


class GridTest(TestCase):
    """Tests Grid class"""

    def test_make_invalid_grid(self):
        with pytest.raises(InvalidTileError):
            Grid(tilesize=0, resolution=1000, pad=0)

    def test_from_latlon(self):
        params = {"tilesize": 1, "resolution": 1.5, "pad": 99}
        lat, lon = (61.91, 5.26)
        tile = Grid(**params).tile_from_lonlat(lon, lat)
        assert tile.tilesize == params["tilesize"]
        assert tile.pad == params["pad"]
        assert tile.tile_extent == params["tilesize"] + 2 * params["pad"]
        assert np.allclose(
            [
                tile.polygon.centroid.xy[0][0],
                tile.polygon.centroid.xy[1][0],
            ],
            [lon, lat],
        )

    def test_dlkeys_from_invalid_latlon(self):
        lat, lon = -97.635, 212.723
        params = {"resolution": 60.0, "tilesize": 512, "pad": 0}
        with pytest.raises(InvalidLatLonError):
            Grid(**params).tile_from_lonlat(0, lat)
        with pytest.raises(InvalidLatLonError):
            Grid(**params).tile_from_lonlat(lon, 0)

    def test_tiles_from_shape_1(self):
        params = {
            "resolution": 10,
            "tilesize": 2048,
            "pad": 16,
        }
        shape = """{"coordinates":
                [[[-90.1897158, 44.2267595],
                [-87.9570052, 43.8067829],
                [-88.5766841, 42.1269533],
                [-90.7457357, 42.5435965],
                [-90.1897158, 44.2267595]]],
                "type": "Polygon"}"""

        grid = Grid(**params)
        gen = grid.tiles_from_shape(shape)
        tiles = [tile for tile in gen]
        assert len(tiles) == len(set(tiles))
        assert len(tiles) == 116

        est_ntiles = grid._estimate_ntiles_from_shape(shape)
        assert len(tiles) > (est_ntiles // 2)
        assert len(tiles) < (est_ntiles * 2)

    def test_tiles_from_shape_2(self):
        params = {
            "resolution": 1,
            "tilesize": 128,
            "pad": 8,
        }
        shape = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.51140471760839, 37.77130087547876],
                        [-122.45475646845254, 37.77475476721895],
                        [-122.45303985468301, 37.76657207194229],
                        [-122.51057242081689, 37.763446782666094],
                        [-122.51140471760839, 37.77130087547876],
                    ]
                ],
            },
            "properties": None,
        }

        grid = Grid(**params)
        gen = Grid(**params).tiles_from_shape(shape)
        tiles = [tile for tile in gen]
        assert len(tiles) == len(set(tiles))
        assert len(tiles) == 325

        est_ntiles = grid._estimate_ntiles_from_shape(shape)
        assert len(tiles) > (est_ntiles // 2)
        assert len(tiles) < (est_ntiles * 2)

    def test_tiles_from_shape_3(self):
        params = {
            "resolution": 1,
            "tilesize": 128,
            "pad": 8,
        }
        feature = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.51140471760839, 37.77130087547876],
                        [-122.45475646845254, 37.77475476721895],
                        [-122.45303985468301, 37.76657207194229],
                        [-122.51057242081689, 37.763446782666094],
                        [-122.51140471760839, 37.77130087547876],
                    ]
                ],
            },
            "properties": None,
        }

        # Any object with a __geo_interface__ property
        # e.g. wf.map.geocontext()
        class MockWfContext(object):
            @property
            def __geo_interface__(self):
                return feature

        mock_context = MockWfContext()

        grid = Grid(**params)
        gen = Grid(**params).tiles_from_shape(mock_context)
        tiles = [tile for tile in gen]
        assert len(tiles) == len(set(tiles))
        assert len(tiles) == 325

        est_ntiles = grid._estimate_ntiles_from_shape(mock_context)
        assert len(tiles) > (est_ntiles // 2)
        assert len(tiles) < (est_ntiles * 2)

    def test_dlkeys_from_invalid_shape(self):
        params = {
            "resolution": 30,
            "tilesize": 2048,
            "pad": 16,
        }
        shape = {"type": "Point", "coordinates": [-105.01621, 39.57422]}
        with pytest.raises(InvalidShapeError):
            for t in Grid(**params).tiles_from_shape(shape):
                pass

    def test_dltiles_utm_buffering(self):
        params = {
            "resolution": 10.0,
            "tilesize": 4096,
            "pad": 0,
        }

        # extremely long and narrow strip which should cover
        # most of the the known world
        y_min, y_max = -66, 66
        x_min, x_max = -98, -90

        feature = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [x_min, y_min],
                                [x_max, y_min],
                                [x_max, y_max],
                                [x_min, y_max],
                                [x_min, y_min],
                            ]
                        ],
                    },
                    "properties": {},
                }
            ],
        }

        gen = Grid(**params).tiles_from_shape(feature)

        counter = 0
        for tile in gen:
            counter += 1
            # simple check that all the tiles are in bounds
            tx_min, ty_min, tx_max, ty_max = tile.polygon.bounds

            if tx_max < x_min or tx_min > x_max or ty_min > y_max or ty_max < y_min:
                raise InvalidLatLonError("tile outside bounds")

        # simple check that coverage is greater than 100%
        p = lonlat_to_utm([(x_min, y_min), (x_max, y_max)], ref_lon=x_min)
        tile_area = (params["resolution"] * params["tilesize"]) ** 2
        est_total_area = (p[1][0] - p[0][0]) * (p[1][1] - p[0][1])
        ratio = (counter * tile_area) / est_total_area
        assert ratio > 1
