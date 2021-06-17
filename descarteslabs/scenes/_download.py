import six
import os.path
import json

from descarteslabs.client.services.raster import Raster
from descarteslabs.client.exceptions import NotFoundError, BadRequestError


ext_to_format = {"tif": "GTiff", "png": "PNG", "jpg": "JPEG"}


def _is_path_like(dest):
    return isinstance(dest, six.string_types) or (
        hasattr(os, "PathLike") and isinstance(dest, os.PathLike)
    )


def _format_from_path(path):
    _, ext = os.path.splitext(path)
    return _get_format(ext.lstrip("."))


def _get_format(ext):
    try:
        return ext_to_format[ext]
    except KeyError:
        six.raise_from(
            ValueError(
                "Unknown format '{}'. Possible values are {}.".format(
                    ext, ", ".join(ext_to_format)
                )
            ),
            None,
        )


def _download(
    inputs,
    bands_list,
    ctx,
    dtype,
    dest,
    format,
    resampler="near",
    processing_level=None,
    scales=None,
    raster_client=None,
):
    """
    Download inputs as an image file and save to file or path-like `dest`.
    Code shared by Scene.download and SceneCollection.download_mosaic
    """
    if raster_client is None:
        raster_client = Raster()

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
    if _is_path_like(dest):
        dirname = os.path.dirname(dest)
        if dirname != "" and not os.path.exists(dirname):
            os.makedirs(dirname)

        format = _format_from_path(dest)
    else:
        format = _get_format(format)

    raster_params = ctx.raster_params
    full_raster_args = dict(
        inputs=inputs,
        bands=bands_list,
        scales=scales,
        data_type=dtype,
        resampler=resampler,
        processing_level=processing_level,
        output_format=format,
        outfile_basename=os.path.splitext(dest)[0],
        **raster_params
    )

    try:
        raster_client.raster(**full_raster_args)
    except NotFoundError:
        if len(inputs) == 1:
            msg = "'{}' does not exist in the Descartes catalog".format(inputs[0])
        else:
            msg = "Some or all of these IDs don't exist in the Descartes catalog: {}".format(
                inputs
            )
        six.raise_from(NotFoundError(msg), None)
    except BadRequestError as e:
        msg = (
            "Error with request:\n"
            "{err}\n"
            "For reference, dl.Raster.raster was called with these arguments:\n"
            "{args}"
        )
        msg = msg.format(err=e, args=json.dumps(full_raster_args, indent=2))
        six.raise_from(BadRequestError(msg), None)
    except ValueError as e:
        raise e

    return dest
