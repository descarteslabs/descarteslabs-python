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

import cachetools
import os.path
import json

from descarteslabs.exceptions import NotFoundError, BadRequestError
from ..client.services.raster import Raster
from ..common.property_filtering import Properties

from .band import Band, DerivedBand
from .search import Search
from .image_types import DownloadFileFormat, ResampleAlgorithm


@cachetools.cached(cachetools.TTLCache(maxsize=256, ttl=600), key=lambda p, c: p)
def cached_bands_by_product(product_id, client):
    bands = {
        band.name: band
        for band in Band.search(client=client).filter(
            Properties().product_id == product_id
        )
    }
    bands.update(
        {
            band.id: band
            for band in Search(
                DerivedBand,
                url="/products/{}/relationships/derived_bands".format(product_id),
                client=client,
                includes=False,
            )
        }
    )
    return bands


def bands_to_list(bands):
    if isinstance(bands, str):
        return bands.split(" ")
    if not isinstance(bands, (list, tuple)):
        raise TypeError(
            f"Expected list or tuple of band names, instead got {type(bands)}"
        )
    if len(bands) == 0:
        raise ValueError("No bands specified to load")
    return list(bands)


# map from file extensions to GDAL file format string
ext_to_format = {
    DownloadFileFormat.TIF: "GTiff",
    DownloadFileFormat.PNG: "PNG",
    DownloadFileFormat.JPEG: "JPEG",
}


def is_path_like(dest):
    return isinstance(dest, str) or (
        hasattr(os, "PathLike") and isinstance(dest, os.PathLike)
    )


def format_from_path(path):
    _, ext = os.path.splitext(path)
    return get_format(ext.lstrip("."))


def get_format(ext):
    try:
        return ext_to_format[ext]
    except KeyError:
        raise ValueError(
            "Unknown format '{}'. Possible values are {}.".format(
                ext, ", ".join(ext_to_format)
            )
        ) from None


# helper for shared code around creating downloads
def download(
    inputs,
    bands_list,
    geocontext,
    data_type,
    dest,
    format=DownloadFileFormat.TIF,
    resampler=ResampleAlgorithm.NEAR,
    processing_level=None,
    scales=None,
    nodata=None,
    progress=None,
):
    """
    Download inputs as an image file and save to file or path-like `dest`.
    Code shared by Scene.download and SceneCollection.download_mosaic
    """
    if dest is None:
        if len(inputs) == 0:
            raise ValueError("No inputs given to download")
        bands_str = "-".join(bands_list)
        if len(inputs) == 1:
            # default filename for a single scene
            dest = "{id}-{bands}.{ext}".format(
                id=inputs[0], bands=bands_str, ext=format
            )
        else:
            # default filename for a mosaic
            dest = "mosaic-{bands}.{ext}".format(bands=bands_str, ext=format)

    # Create any intermediate directories
    if is_path_like(dest):
        dirname = os.path.dirname(dest)
        if dirname != "" and not os.path.exists(dirname):
            os.makedirs(dirname)

        format = format_from_path(dest)
    else:
        format = get_format(format)

    raster_params = geocontext.raster_params
    full_raster_args = dict(
        inputs=inputs,
        bands=bands_list,
        scales=scales,
        data_type=data_type,
        resampler=resampler,
        processing_level=processing_level,
        output_format=format,
        outfile_basename=os.path.splitext(dest)[0],
        nodata=nodata,
        progress=progress,
        **raster_params,
    )

    try:
        Raster.get_default_client().raster(**full_raster_args)
    except NotFoundError:
        if len(inputs) == 1:
            msg = "'{}' does not exist in the Descartes catalog".format(inputs[0])
        else:
            msg = "Some or all of these IDs don't exist in the Descartes catalog: {}".format(
                inputs
            )
        raise NotFoundError(msg) from None
    except BadRequestError as e:
        msg = (
            "Error with request:\n"
            "{err}\n"
            "For reference, Raster.raster was called with these arguments:\n"
            "{args}"
        )
        msg = msg.format(err=e, args=json.dumps(full_raster_args, indent=2))
        raise BadRequestError(msg) from None
    except ValueError as e:
        raise e

    return dest
