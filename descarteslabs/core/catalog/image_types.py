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
