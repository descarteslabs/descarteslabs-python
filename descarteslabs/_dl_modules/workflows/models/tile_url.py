import json
import urllib

from ..execution import arguments_to_grafts, promote_arguments
from ..types import Proxytype


def tile_url(
    base_url_template: str,
    obj: Proxytype,
    session_id=None,
    colormap=None,
    bands=None,
    scales=None,
    reduction=None,
    checkerboard=None,
    **arguments,
):
    """
    Build the URL template for a tiles endpoint.

    Parameters
    ----------
    base_url_template: str
        Base URL template.
    obj: Proxytype
        The the object you're generating a URL for.
        If a `Function`, ``arguments`` will be checked against and promoted to these.
        Otherwise, no ``arguments`` should be given.
    session_id: str, optional, default None
        Unique, client-generated ID that logs will be stored under.
        Since multiple users may access tiles from the same `XYZ` object,
        each user should set their own ``session_id`` to get individual logs.
    colormap: str, optional, default None
        Name of the colormap to use. If set, the displayed `~.geospatial.Image`
        or `~.geospatial.ImageCollection` must have 1 band.
    bands: list of str, optional, default None
        The band names to select from the imagery. If None (default),
        the imagery should already have 1-3 bands selected.
    scales: list of lists, optional, default None
        The scaling to apply to each band in the `~.geospatial.Image`.
        If displaying an `~.geospatial.ImageCollection`, it is reduced into
        an `~.geospatial.Image` before scaling.

        If the `~.geospatial.Image` or `~.geospatial.ImageCollection` contains 3 bands,
        ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

        If the `~.geospatial.Image` or `~.geospatial.ImageCollection` contains 1 band,
        ``scales`` must be a list like ``[(0, 1)]``, or just ``(0, 1)`` for convenience

        If None, each 256x256 tile will be scaled independently.
    reduction: str, optional, default None
        One of "mosaic", "min", "max", "mean", "median", "sum", "std", or "count".
        If displaying an `~.geospatial.ImageCollection`, this method is used to reduce it into
        an `~.geospatial.Image`. The reduction is performed before applying a colormap or scaling.
        If displaying an `~.geospatial.Image`, reduction is ignored.
    checkerboard: bool, default None
        Whether to display a checkerboarded background for missing or masked data.
    **arguments: Any
        Values for all ``params``.
        Can be given as Proxytypes, or as Python objects like numbers,
        lists, and dicts that can be promoted to them.
        These arguments cannot depend on any parameters.

    Returns
    -------
    url: str
        Tile URL containing ``{z}``, ``{x}``, and ``{y}`` as Python format string parameters,
        and query arguments URL-quoted.

    Raises
    ------
    ValueError
        If the `XYZ` object has no `id` and `.save` has not been called yet.

    TypeError
        If the ``scales`` are of invalid type.

        If the ``arguments`` names or types don't match the `params`
        that the `object` depends on.
    """
    query_args = {}
    if session_id is not None:
        query_args["session_id"] = session_id
    if colormap is not None:
        query_args["colormap"] = colormap
    if reduction is not None:
        query_args["reduction"] = reduction
    if checkerboard is not None:
        query_args["checkerboard"] = "true" if checkerboard else "false"

    if bands is not None:
        try:
            nbands = len(bands)
        except Exception:
            raise TypeError(f"bands must be a sequence; got {bands!r}")

        if nbands > 3:
            raise ValueError(f"Up to 3 bands may be specified, not {nbands}: {bands!r}")

        query_args["band"] = bands

    if scales is not None:
        scales = validate_scales(scales)

        if any(scale != [None, None] for scale in scales):
            query_args["scales"] = json.dumps(scales)
        else:
            query_args["scales"] = "null"  # a json None

    _, promoted_arguments = promote_arguments(obj, arguments)
    if promoted_arguments:
        query_args.update(
            {
                param: json.dumps(graft)
                for param, graft in arguments_to_grafts(**promoted_arguments).items()
            }
        )

    if query_args:
        return base_url_template + "?" + urllib.parse.urlencode(query_args, doseq=True)
    return base_url_template


def validate_scales(scales):
    """
    Validate and normalize a list of scales for an XYZ layer.

    A _scaling_ is a 2-tuple (or 2-list) like ``[min, max]``,
    meaning the range of values in your data you want to stretch
    to the 0..255 output range.

    If ``min`` and ``max`` are ``None``, the min/max values in the
    data will be used automatically. Since each tile is computed
    separately and may have different min/max values, this can
    create a "patchwork" effect when viewed on a map.

    Scales can be given as:

    * Three scalings, for 3-band images, like ``[[0, 1], [0, 0.5], [None, None]]``
    * Two scalings, for 2-band images, like ``[[0, 1], [None, None]]``
    * 1-list/tuple of 1 scaling, for 1-band images, like ``[[0, 1]]``
    * 1 scaling (for convenience), which is equivalent to the above: ``[0, 1]``
    * None, or an empty list or tuple for no scalings

    Parameters
    ----------
    scales: list, tuple, or None
        The scales to validate, in the format shown above

    Returns
    -------
    scales: list
        0- to 3-length list of scalings, where each item is a float.
        (``[0, 1]`` would become ``[[0.0, 1.0]]``, for example.)
        If no scalings are given, an empty list is returned.

    Raises
    ------
    TypeError, ValueError
        If the scales do not match the correct format
    """
    if scales is not None:
        if not isinstance(scales, (list, tuple)):
            raise TypeError(
                "Expected a list or tuple of scales, but got {}".format(scales)
            )

        if (
            len(scales) == 2
            and not isinstance(scales[0], (list, tuple))
            and not isinstance(scales[1], (list, tuple))
        ):
            # allow a single 2-tuple for convenience with colormaps/1-band images
            scales = (scales,)

        if len(scales) > 3:
            raise (
                ValueError(
                    "Too many scales passed: expected up to 3 scales, but got {}".format(
                        len(scales)
                    )
                )
            )

        for i, scaling in enumerate(scales):
            if not isinstance(scaling, (list, tuple)):
                raise TypeError(
                    "Scaling {}: expected a 2-item list or tuple for the scaling, "
                    "but got {}".format(i, scaling)
                )
            if len(scaling) != 2:
                raise ValueError(
                    "Scaling {}: expected a 2-item list or tuple for the scaling, "
                    "but length was {}".format(i, len(scaling))
                )
            if not all(isinstance(x, (int, float, type(None))) for x in scaling):
                raise TypeError(
                    "Scaling {}: items in scaling must be numbers or None; "
                    "got {}".format(i, scaling)
                )
            # At this point we know they are all int, float, or None
            # So we check to see if we have an int/float and a None
            if any(isinstance(x, (int, float)) for x in scaling) and any(
                x is None for x in scaling
            ):
                raise ValueError(
                    "Invalid scales passed: one number and one None in scales[{}] {}".format(
                        i, scaling
                    )
                )

        return [
            [float(x) if isinstance(x, int) else x for x in scaling]
            for scaling in scales
        ]
        # be less strict about floats than traitlets is
    else:
        return []
