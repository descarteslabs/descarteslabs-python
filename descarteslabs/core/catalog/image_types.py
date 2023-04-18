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

from strenum import StrEnum


class ResampleAlgorithm(StrEnum):
    """Valid GDAL resampler algorithms for rastering.

    Attributes
    ----------
    NEAR : enum
        Nearest neighbor.
    BILINEAR : enum
        Bilinear.
    CUBIC : enum
        Cubic.
    CUBICSPLINE : enum
        Cubic spline.
    AVERAGE : enum
        Average.
    MODE : enum
        Mode.
    MAX : enum
        Max.
    MIN : enum
        Min.
    MED : enum
        Median.
    Q1 : enum
        Q1.
    Q3 : enum
        Q3.
    """

    NEAR = "near"
    BILINEAR = "bilinear"
    CUBIC = "cubic"
    CUBICSPLINE = "cubicspline"
    LANCZOS = "lanczos"
    AVERAGE = "average"
    MODE = "mode"
    MAX = "max"
    MIN = "min"
    MEDIAN = "med"
    Q1 = "q1"
    Q3 = "q3"


class DownloadFileFormat(StrEnum):
    """Supported download file formats.

    Attributes
    ----------
    JPEG : enum
        JPEG encoded GeoTIFF format.
    PNG : enum
        PNG format.
    TIF : enum
        GeoTIFF format.
    """

    JPEG = "jpg"
    PNG = "png"
    TIF = "tif"
