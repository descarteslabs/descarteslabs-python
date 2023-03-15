import tempfile
import numpy as np
import pytest
import sys

from ..geotiff_utils import make_rasterio_geotiff


def source_band(bidx, dtype="Float32"):
    return {
        "band": bidx,
        "block": [128, 128],
        "colorInterpretation": "Undefined",
        "description": {
            "data_range": [0.0, 1_000_000.0],
            "dtype": dtype,
            "jpx_layer": 0,
            "name": f"band{bidx}",
            "nodata": -99.0,
            "srcband": 7,
            "srcfile": 0,
            "tags": [],
            "type": "other",
        },
        "metadata": {},
        "noDataValue": -99.0,
        "overviews": [
            {"size": [180, 161]},
            {"size": [45, 41]},
            {"size": [12, 11]},
        ],
        "type": dtype,
    }


def simulate_npz_data(chunk_shapes, rowcol, bands, dtype):
    rows, cols = rowcol

    metadata = {
        "bands": list(source_band(x + 1, dtype) for x in range(bands)),
        "coordinateSystem": {
            "dataAxisToSRSAxisMapping": [2, 1],
            "epsg": 4326,
            "proj4": "+proj=longlat +datum=WGS84 +no_defs",
            "wkt": 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AXIS["Latitude",NORTH],AXIS["Longitude",EAST],AUTHORITY["EPSG","4326"]]',  # noqa
        },
        "cornerCoordinates": {
            "center": [-95.02000000000001, 35.620000000000005],
            "lowerLeft": [-99.98, 30.11],
            "lowerRight": [-90.06, 30.11],
            "upperLeft": [-99.98, 41.13],
            "upperRight": [-90.06, 41.13],
        },
        "driverLongName": "Virtual Raster",
        "driverShortName": "VRT",
        "geoTransform": [-99.98, 0.0125, 0.0, 41.13, 0.0, -0.0125],
        "metadata": {
            "GEOGCS": "WGS 84",
            "GEOGCS|DATUM": "WGS_1984",
            "GEOGCS|PRIMEM": "Greenwich",
            "GEOGCS|SPHEROID": "WGS 84",
        },
        "size": [720, 641],  # the source image size
        "wgs84Extent": {
            "coordinates": [
                [
                    [-180.0, 80.0],
                    [-180.0, -80.25],
                    [0.0, -80.25],
                    [0.0, 80.0],
                    [-180.0, 80.0],
                ]
            ],
            "type": "Polygon",
        },
        "id": "e",
    }

    blosc_meta = {
        "chunks": len(chunk_shapes),
        "dtype": dtype,
        "shape": [bands, rows, cols],
    }

    def gen_chunks():
        for shape in chunk_shapes:
            yield (np.random.rand(*shape) * 100.0).astype(dtype)

    return gen_chunks(), metadata, blosc_meta


# Block shapes are (rows, cols, bands) and
# are always returned in Left-to-right, Bottom-to-top order
CHUNK_SHAPES_MULTIBLOCK = [(512, 512), (512, 282), (370, 512), (370, 282)]
CHUNK_SHAPES_SINGLEBLOCK = [(400, 400)]

GOOD_COMBINATIONS = (
    (CHUNK_SHAPES_SINGLEBLOCK, (400, 400), 1, "float32"),
    (CHUNK_SHAPES_MULTIBLOCK, (882, 794), 3, "float32"),
    (CHUNK_SHAPES_SINGLEBLOCK, (400, 400), 3, "float32"),
    (CHUNK_SHAPES_MULTIBLOCK, (882, 794), 4, "float32"),
    (CHUNK_SHAPES_SINGLEBLOCK, (400, 400), 4, "float32"),
    (CHUNK_SHAPES_SINGLEBLOCK, (400, 400), 1, "uint8"),
    (CHUNK_SHAPES_MULTIBLOCK, (882, 794), 3, "uint8"),
    (CHUNK_SHAPES_SINGLEBLOCK, (400, 400), 3, "uint8"),
    (CHUNK_SHAPES_MULTIBLOCK, (882, 794), 4, "uint8"),
    (CHUNK_SHAPES_SINGLEBLOCK, (400, 400), 4, "uint8"),
)

# These don't work with tifffile for some reason
BAD_TIFFFILE_COMBINATIONS = (
    (CHUNK_SHAPES_MULTIBLOCK, (882, 794), 1, "float32"),
    (CHUNK_SHAPES_MULTIBLOCK, (882, 794), 1, "uint8"),
)

if "rasterio" in sys.modules:
    import rasterio

    @pytest.mark.parametrize("chunk_shapes,rowcol,bands,dtype", GOOD_COMBINATIONS)
    def test_geotiff_rasterio_lzw(chunk_shapes, rowcol, bands, dtype):
        nodata = 0
        compress = None  # default is LZW
        chunk_shapes_3d = [(cs[0], cs[1], bands) for cs in chunk_shapes]
        chunk_iter, metadata, blosc_meta = simulate_npz_data(
            chunk_shapes_3d, rowcol, bands, dtype
        )

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".tif") as tmp:
            make_rasterio_geotiff(
                tmp.name, chunk_iter, metadata, blosc_meta, compress, nodata
            )

            with rasterio.open(tmp.name) as dst:
                assert list(dst.read().shape) == blosc_meta["shape"]
                assert dst.profile["compress"].lower() == "lzw"

    @pytest.mark.parametrize("chunk_shapes,rowcol,bands,dtype", GOOD_COMBINATIONS)
    def test_geotiff_rasterio_deflate(chunk_shapes, rowcol, bands, dtype):
        nodata = 0
        compress = "DEFLATE"
        chunk_shapes_3d = [(cs[0], cs[1], bands) for cs in chunk_shapes]
        chunk_iter, metadata, blosc_meta = simulate_npz_data(
            chunk_shapes_3d, rowcol, bands, dtype
        )

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".tif") as tmp:
            make_rasterio_geotiff(
                tmp.name, chunk_iter, metadata, blosc_meta, compress, nodata
            )

            with rasterio.open(tmp.name) as dst:
                assert list(dst.read().shape) == blosc_meta["shape"]
                assert dst.profile["compress"].lower() == "deflate"

    @pytest.mark.parametrize("chunk_shapes,rowcol,bands,dtype", GOOD_COMBINATIONS)
    def test_geotiff_rasterio_jpeg(chunk_shapes, rowcol, bands, dtype):
        nodata = 0
        compress = "JPEG"
        chunk_shapes_3d = [(cs[0], cs[1], bands) for cs in chunk_shapes]
        chunk_iter, metadata, blosc_meta = simulate_npz_data(
            chunk_shapes_3d, rowcol, bands, dtype
        )

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".tif") as tmp:
            make_rasterio_geotiff(
                tmp.name, chunk_iter, metadata, blosc_meta, compress, nodata
            )

            with rasterio.open(tmp.name) as dst:
                assert list(dst.read().shape) == blosc_meta["shape"]
                assert dst.profile["compress"].lower() == "jpeg"


# @pytest.mark.parametrize("chunk_shapes,rowcol,bands,dtype", GOOD_COMBINATIONS)
# def test_geotiff_tifffile(chunk_shapes, rowcol, bands, dtype):
#     nodata = 0
#     compress = None
#     chunk_shapes_3d = [(cs[0], cs[1], bands) for cs in chunk_shapes]
#     chunk_iter, metadata, blosc_meta = simulate_npz_data(
#         chunk_shapes_3d, rowcol, bands, dtype
#     )

#     with tempfile.NamedTemporaryFile(mode="wb", suffix=".tif") as tmp:
#         make_tifffile_geotiff(
#             tmp.name, chunk_iter, metadata, blosc_meta, compress, nodata
#         )

#         with rasterio.open(tmp.name) as dst:
#             assert list(dst.read().shape) == blosc_meta["shape"]
#         assert "compress" not in dst.profile
