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

from typing import List, Tuple, Union
from enum import Enum
import numpy as np
from affine import Affine

from tifffile import TiffWriter

# Rasterio is used in `make_geotiffs` but ONLY if installed
try:
    import rasterio
    from rasterio.crs import CRS
except ImportError:
    rasterio = None


##############################################################################
# GeoTiff Tags
##############################################################################

# Geotiff-specific data structure, see spec

GeoKeyDirectory = List[int]

# 6 elements, defines the origin coordinate
ModelTiePoint = List[float]

# 3 elements, defines the resolution
ModelPixelScale = List[float]

# EPSG Code
ProjectedCSTypeGeoKey = int


# General type of coordinate system
# https://docs.opengeospatial.org/is/19-008r4/19-008r4.html#_requirements_class_gtmodeltypegeokey
class GTModelTypeGeoKey(Enum):
    UNKNOWN = 0
    PROJECTED_2D = 1
    GEOGRAPHIC_2D = 2
    GEOCENTRIC_3D = 3
    USER_DEFINED = 32767


# https://docs.opengeospatial.org/is/19-008r4/19-008r4.html#_requirements_class_units_geokeys
class GeogAngularUnitsGeoKey(Enum):
    ANGULAR_RADIAN = 9101
    ANGULAR_DEGREE = 9102  # most common
    ANGULAR_ARC_MINUTE = 9103
    ANGULAR_ARC_SECOND = 9104
    ANGULAR_GRAD = 9105
    ANGULAR_GON = 9106
    ANGULAR_DMS = 9107
    ANGULAR_DMS_HEMISPHERE = 9108


class ProjLinearUnitsGeoKey(Enum):
    LINEAR_METER = 9001
    LINEAR_FOOT = 9002
    LINEAR_FOOT_US_SURVEY = 9003
    LINEAR_FOOT_MODIFIED_AMERICAN = 9004
    LINEAR_FOOT_CLARKE = 9005
    LINEAR_FOOT_INDIAN = 9006
    LINEAR_LINK = 9007
    LINEAR_LINK_BENOIT = 9008
    LINEAR_LINK_SEARS = 9009
    LINEAR_CHAIN_BENOIT = 9010
    LINEAR_CHAIN_SEARS = 9011
    LINEAR_YARD_SEARS = 9012
    LINEAR_YARD_INDIAN = 9013
    LINEAR_FATHOM = 9014
    LINEAR_MILE_INTERNATIONAL_NAUTICAL = 9015


def make_geotiff_profile(metadata, blosc_meta):
    dtype = {
        "uint16": np.uint16,
        "uint8": np.uint8,
        "int16": np.int16,
        "uint32": np.uint32,
        "int32": np.int32,
        "float32": np.float32,
        "float64": np.float64,
    }

    if blosc_meta["dtype"] not in dtype:
        raise ValueError("Unknown data type {} returned".format(blosc_meta["dtype"]))

    gt = metadata["geoTransform"]
    transform = Affine.from_gdal(*gt)

    geotiff_profile = dict(
        driver="GTiff",
        count=blosc_meta["shape"][0],
        height=blosc_meta["shape"][1],
        width=blosc_meta["shape"][2],
        dtype=dtype[blosc_meta["dtype"]],
        tiled=True,
        transform=transform,
        blockxsize=512,
        blockysize=512,
    )

    return geotiff_profile


def construct_geokeydirectory(
    gtmodeltypegeokey: GTModelTypeGeoKey,
    geogangularunitsgeokey: GeogAngularUnitsGeoKey,
    projectedcstypegeokey: ProjectedCSTypeGeoKey,
    projlinearunitsgeokey: ProjLinearUnitsGeoKey,
    projcs,
    geogcs,
) -> GeoKeyDirectory:
    """Takes our nicely typed components and constructs this awful data structure

    Behold, the GeoKeyDirectory. Interpreted as groups of four integers with a header...
    """
    data = [
        # Header
        # ... KeyDirectoryVersion, KeyRevision, MinorRevision, NumberOfKeys
        1,
        1,
        0,
        7,
        #
        # Now the keys themselves
        # ... KeyID, TIFFTagLocation, Count, Value_Offset
        # GTModelTypeGeoKey
        1024,
        0,
        1,
        gtmodeltypegeokey.value,
        #
        # GTRasterTypeGeoKey
        1025,
        0,
        1,
        1,  # i.e. AREA_OR_POINT=Area in GDAL. hardcoded for now
        #
        # GTCitationGeoKey (First part of the projection desc)
        1026,
        34737,
        len(projcs),
        0,
        #
        # GeogCitationGeoKey (citation for geographic coordinate system)
        2049,
        34737,
        len(geogcs),
        len(projcs),
        #
        # GeogAngularUnitsGeoKey
        2054,
        0,
        1,
        geogangularunitsgeokey.value,
        #
        # ProjectedCSTypeGeoKey
        3072,
        0,
        1,
        projectedcstypegeokey,
        #
        # ProjLinearUnitsGeoKey
        3076,
        0,
        1,
        projlinearunitsgeokey.value,
    ]

    # TODO: custom SRS definitions

    # if projectedcstypegeokey == 32767:
    #     data.extend([
    #         # Datum
    #         2050,
    #         0,
    #         1,
    #         6308,
    #         #
    #     ])
    #     data[3] += 1

    return data


def parse_projection(metadata) -> Tuple[GeoKeyDirectory, str, str]:
    """
    Given a projection string or epsg code (TBD),
    construct the geokeydirectory and the proj strings

    TODO this is hardcoded for now. We need to have `libproj` or
    some way to parse the input into these items. Alternatively,
    we could get them from the npz endpoint.

    Returns:
        gkd, projcs, geogcs
    """

    if "PROJCS" in metadata["metadata"]:
        projcs = metadata["metadata"]["PROJCS"] + "|"
    else:
        projcs = "unknown|"

    if "epsg" in metadata["coordinateSystem"]:
        epsg_code = metadata["coordinateSystem"]["epsg"]
        if "GEOGCS" in metadata["metadata"]:
            geogcs = "{}|".format(metadata["metadata"]["GEOGCS"])
        else:
            geogcs = "unknown|"
    else:
        epsg_code = 32767  # designated user-defined srs code

        if "GEOGCS" in metadata["metadata"]:
            geogcs = "GCS Name = {}|".format(metadata["metadata"]["GEOGCS"])

            if "GEOGCS|DATUM" in metadata["metadata"]:
                geogcs += "Datum = {}|".format(metadata["metadata"]["GEOGCS|DATUM"])
            if "GEOGCS|SPHEROID" in metadata["metadata"]:
                geogcs += "Ellipsoid = {}|".format(
                    metadata["metadata"]["GEOGCS|SPHEROID"]
                )
            if "GEOGCS|PRIMEM" in metadata["metadata"]:
                geogcs += "Primem = {}||".format(metadata["metadata"]["GEOGCS|PRIMEM"])
        else:
            geogcs = "unknown|"

    epsg: ProjectedCSTypeGeoKey = epsg_code

    gtmodeltypegeokey = GTModelTypeGeoKey.PROJECTED_2D
    geogangularunitsgeokey = GeogAngularUnitsGeoKey.ANGULAR_DEGREE
    projlinearunitsgeokey = ProjLinearUnitsGeoKey.LINEAR_METER

    gkd = construct_geokeydirectory(
        gtmodeltypegeokey,
        geogangularunitsgeokey,
        epsg,
        projlinearunitsgeokey,
        projcs,
        geogcs,
    )
    return (gkd, projcs, geogcs)


def parse_transform(transform) -> Tuple[ModelTiePoint, ModelPixelScale]:
    """Given an affine transform defining the placement of pixels in space,
    convert to the geotiff model.
    """
    mtp = [0.0, 0.0, 0.0, transform.xoff, transform.yoff, 0.0]

    # We re-invert the y resolution here, geotiff spec needs a positive value here
    mps = [transform.a, -1 * transform.e, 0.0]

    return mtp, mps


def make_gdalinfo(metadata):
    info = "<GDALMetadata>\n"
    info += '  <Item name="id">{}</Item>\n'.format(metadata["id"])

    c = 0
    for b in metadata["bands"]:
        if "colorInterpretation" in b:
            info += '  <Item name="COLORINTERP" sample="{}" role="colorinterp">{}</Item>\n'.format(
                c, b["colorInterpretation"]
            )
        if "description" in b:
            info += '  <Item name="DESCRIPTION" sample="{}" role="description">{}</Item>\n'.format(
                c, b["description"]
            )
        c += 1
    info += "</GDALMetadata>"
    return info


def convert_to_geotiff_tags(
    gkd: GeoKeyDirectory,
    mtp: ModelTiePoint,
    mps: ModelPixelScale,
    projcs: str,
    geogcs: str,
    gdalinfo: str,
    nodata: Union[int, float, complex, np.number, str, None],
) -> List[Tuple]:
    """Creates the bare minimum geotiff tags to match typical GDAL output.

    Returns list of tuples suitable as direct input for tifffile

        Tifffile.write(extra_tags=convert_to_geotiff_tags(...))
    """
    projdesc = projcs + geogcs
    tags = [
        # ModelPixelScaleTag:
        (33550, 12, 3, mps, False),
        # ModelTiePointTag:
        (33922, 12, 6, mtp, False),
        # GeoKeyDirectoryTag:
        (34735, 3, len(gkd), gkd, False),
        # GeoAsciiParamsTag:
        (34737, 2, len(projdesc), projdesc, False),
        # GDAL Info
        (42112, 2, len(gdalinfo), gdalinfo, False),
    ]
    if nodata is not None:
        nodata_str = str(nodata)
        tags.append((42113, 2, len(nodata_str), nodata_str, False))
    return tags


def make_geotiff(outfile, chunk_iter, metadata, blosc_meta, compress, nodata):
    if rasterio is not None:
        make_rasterio_geotiff(
            outfile, chunk_iter, metadata, blosc_meta, compress, nodata
        )
    else:
        make_tifffile_geotiff(
            outfile, chunk_iter, metadata, blosc_meta, compress, nodata
        )


def make_rasterio_geotiff(outfile, chunk_iter, metadata, blosc_meta, compress, nodata):
    """Use rasterio to create a geotiff. Uses libgdal to write thus offering full functionality
    and more likely to be compatible with the rest of the geospatial software ecosystem

    :param outfile: string, path to output geotiff file.
    :param chunk_iter: Iterator yielding "chunks", a 3D array of (rows, cols, bands) representing one
        geotiff block. Streamed from the npz service. The order and length of the chunk sequence
        must match that of the underlying blocks on disk. npz and all our tiff writers
        agree on the correct order.
    :param metadata: dict of image and per-band metdata
    :param blosc_meta: dict of metadata describing the npz payload shape
    :param compress: string, compression method to use when writing geotiff. Defaults to "LZW".
        For rasterio geotiffs, accepts any of the algorithms supported by the
        underlying GDAL shared library.
    :param nodata: numeric, global value to represent masked (nodata) regions
    """
    geotiff_profile = make_geotiff_profile(metadata, blosc_meta)

    if compress is None:
        # Always default to lossless compression to save disk space.
        compress = "LZW"

    crs = CRS.from_proj4(metadata["coordinateSystem"]["proj4"])

    with rasterio.open(
        outfile, mode="w", compress=compress, nodata=nodata, crs=crs, **geotiff_profile
    ) as dst:
        for i, bandmeta in enumerate(metadata["bands"]):
            dst.update_tags(i + 1, **bandmeta["description"])

        # Windowed writing ensures that we never need more than one chunk in memory
        # each "chunk" corresponds to a raster block (ie tile) by design
        for chunk, (_, window) in zip(chunk_iter, dst.block_windows()):
            # swap axis order from (rows, cols, bands) to (bands, rows, cols)
            # single bands come back as 2d
            arr = np.transpose(np.atleast_3d(chunk), [2, 0, 1])
            dst.write(arr, window=window)


def make_tifffile_geotiff(outfile, chunk_iter, metadata, blosc_meta, compress, nodata):
    """
    Use the tiffwriter which makes viable GeoTiffs but with limited optionality
    specifically, they lack lossless compression

    :param outfile: string, path to output geotiff file.
    :param chunk_iter: Iterator yielding "chunks", a 3D array of (rows, cols, bands) representing one
        geotiff block. Streamed from the npz service. The order and length of the chunk sequenmce
        must match that of the underlying blocks on disk. npz and all our tiff writers
        agree on the correct order.
    :param metadata: dict of image and per-band metdata
    :param blosc_meta: dict of metadata describing the npz payload shape
    :param compress: string, compression method to use when writing geotiff.
        Defaults to uncompressed. Also supports "JPEG" and "PNG".
    :param nodata: numeric, global value to represent masked (nodata) regions
    """
    geotiff_profile = make_geotiff_profile(metadata, blosc_meta)
    gkd, projcs, geogcs = parse_projection(metadata)
    mtp, mps = parse_transform(geotiff_profile["transform"])
    gdalinfo = make_gdalinfo(metadata)
    extra_tags = convert_to_geotiff_tags(
        gkd, mtp, mps, projcs, geogcs, gdalinfo, nodata
    )

    nbands, height, width = blosc_meta["shape"]

    if compress == "JPEG":
        if nbands == 2 or nbands > 3:
            raise ValueError(
                "JPEG output format does not allow {} bands:".format(nbands)
                + "must be 1 (gray) or 3 (rgb) bands"
            )
    elif compress == "PNG":
        compress = None
        if nbands == 2 or nbands > 4:
            raise ValueError(
                "PNG output format does not allow {} bands:".format(nbands)
                + "must be 1 (gray), 3 (rgb), or 4 (rgba) bands"
            )

    if height < 1 or width < 1:
        raise ValueError("Height or width less than one pixel in dimension")

    with TiffWriter(outfile) as tif:
        if nbands > 1:
            pconfig = "CONTIG"
            shape = (
                geotiff_profile["height"],
                geotiff_profile["width"],
                geotiff_profile["count"],
            )
        else:
            pconfig = None
            shape = (geotiff_profile["height"], geotiff_profile["width"])

        if (
            height < geotiff_profile["blockxsize"]
            and width < geotiff_profile["blockysize"]
            and compress != "JPEG"
        ):
            tile = None
        else:
            tile = (
                geotiff_profile["blockxsize"],
                geotiff_profile["blockysize"],
            )

        tif.write(
            data=chunk_iter,
            tile=tile,
            shape=shape,
            dtype=geotiff_profile["dtype"],
            software="descarteslabs",
            extratags=extra_tags,
            compression=compress,
            planarconfig=pconfig,
        )
