import json
import uuid

import ipyleaflet
import traitlets

from ..types import Image
from ..models import XYZ


class ScaleFloat(traitlets.CFloat):
    "Casting Float traitlet that also considers the empty string as None"

    def validate(self, obj, value):
        if value == "" and self.allow_none:
            return None
        return super(ScaleFloat, self).validate(obj, value)


class WorkflowsLayer(ipyleaflet.TileLayer):
    """
    Subclass of ``ipyleaflet.TileLayer`` for displaying a Workflows `~.geospatial.Image`.

    Attributes
    ----------
    image: ~.geospatial.Image
        The `~.geospatial.Image` to use
    displayed_image: ~.geospatial.Image
        Read-only: the `~.geospatial.Image` object that's actually displayed.
        This differs from `~.geospatial.Image` if `colormap` is set, for example.
    xyz_obj: ~.models.XYZ
        Read-only: The `XYZ` object this layer is displaying.
    colormap: str, optional, default None
        Name of the colormap to use.
        If set, `image` must have 1 band.
        Updates `displayed_image` by calling `~Image.colormap` on `image`.
    cmap_min: float, optional, default None
        Min value for scaling the single band when a `colormap` is given.
    cmap_max: float, optional, default None
        Max value for scaling the single band when a `colormap` is given.
    r_min: float, optional, default None
        Min value for scaling the red band.
    r_max: float, optional, default None
        Max value for scaling the red band.
    g_min: float, optional, default None
        Min value for scaling the green band.
    g_max: float, optional, default None
        Max value for scaling the green band.
    b_min: float, optional, default None
        Min value for scaling the blue band.
    b_max: float, optional, default None
        Max value for scaling the blue band.
    """

    attribution = traitlets.Unicode("Descartes Labs").tag(sync=True, o=True)
    min_zoom = traitlets.Int(5).tag(sync=True, o=True)
    url = traitlets.Unicode(read_only=True).tag(sync=True)

    image = traitlets.Instance(Image)
    displayed_image = traitlets.Instance(Image, read_only=True)
    xyz_obj = traitlets.Instance(XYZ, read_only=True)
    session_id = traitlets.Unicode(read_only=True)

    r_min = ScaleFloat(None, allow_none=True)
    r_max = ScaleFloat(None, allow_none=True)
    g_min = ScaleFloat(None, allow_none=True)
    g_max = ScaleFloat(None, allow_none=True)
    b_min = ScaleFloat(None, allow_none=True)
    b_max = ScaleFloat(None, allow_none=True)

    colormap = traitlets.Unicode(None, allow_none=True)
    cmap_min = ScaleFloat(None, allow_none=True)
    cmap_max = ScaleFloat(None, allow_none=True)

    def __init__(self, image, *args, **kwargs):
        super(WorkflowsLayer, self).__init__(*args, **kwargs)
        with self.hold_trait_notifications():
            self.image = image
            self.set_trait("session_id", uuid.uuid4().hex)

    def make_url(self):
        """
        Generate the URL for this layer.

        This is called automatically as the attributes (`image`, `colormap`, scales, etc.) are changed.
        """
        if not self.visible:
            # workaround for the fact that Leaflet still loads tiles from inactive layers,
            # which is expensive computation users don't want
            return ""

        if self.colormap is None:
            scales = [
                [self.r_min, self.r_max],
                [self.g_min, self.g_max],
                [self.b_min, self.b_max],
            ]
        else:
            scales = [[0.0, 1.0], [0.0, 1.0], [0.0, 1.0]]

        scales = [scale for scale in scales if scale != [None, None]]

        query_args = {"session_id": self.session_id}
        if len(scales) > 0:
            query_args["scales"] = json.dumps(scales)

        return self.xyz_obj.url(**query_args)

    @traitlets.observe("image", "colormap", "cmap_min", "cmap_max")
    def _update_colormap(self, change):
        if self.colormap is not None:
            cmapped = self.image.colormap(self.colormap, self.cmap_min, self.cmap_max)
            self.set_trait("displayed_image", cmapped)
        else:
            self.set_trait("displayed_image", self.image)

    @traitlets.observe("displayed_image")
    def _update_displayed_image(self, change):
        xyz = XYZ.build(change["new"], name=self.name)
        xyz.save()
        self.set_trait("xyz_obj", xyz)

    @traitlets.observe(
        "visible",
        "r_min",
        "r_max",
        "g_min",
        "g_max",
        "b_min",
        "b_max",
        "xyz_obj",
        "session_id",
    )
    def _update_url(self, change):
        self.set_trait("url", self.make_url())

    def set_scales(self, scales, new_colormap=False):
        """
        Update the scales for this layer by giving a list of scales

        Parameters
        ----------
        scales: list of lists, default None
            The scaling to apply to each band in the `Image`.

            If `Image` contains 3 bands, ``scales`` must be a list like ``[(0, 1), (0, 1), (-1, 1)]``.

            If `Image` contains 1 band, ``scales`` must be a list like ``[(0, 1)]``,
            or just ``(0, 1)`` for convenience

            If None, each 256x256 tile will be scaled independently
            based on the min and max values of its data.
        new_colormap: str, None, or False, optional, default False
            A new colormap to set at the same time, or False to use the current colormap.

            If changing both scales and colormap, this is more efficient
            than doing one at a time, since it will only re-publish one `Workflow`
            instead of two.
        """
        colormap = self.colormap if new_colormap is False else new_colormap

        if scales is not None:
            if not isinstance(scales, (list, tuple)):
                raise TypeError(
                    "Expected a list or tuple of scales, but got {}".format(scales)
                )

            if colormap is not None:
                if len(scales) == 2:
                    # allow a single 2-tuple for convenience with colormaps
                    scales = (scales,)

            scales_len = 1 if colormap is not None else 3
            if len(scales) != scales_len:
                raise ValueError(
                    "Expected {} scales, but got {}".format(scales_len, len(scales))
                )

            for i, scaling in enumerate(scales):
                if not isinstance(scaling, (list, tuple)):
                    raise TypeError(
                        "Scaling {}: expected a 2-item list or tuple for the scaling, "
                        "but got {}".format(i, scaling)
                    )
                if len(scaling) != 2:
                    raise TypeError(
                        "Scaling {}: expected a 2-item list or tuple for the scaling, "
                        "but length was {}".format(i, len(scaling))
                    )
                if not all(isinstance(x, (int, float, type(None))) for x in scaling):
                    raise TypeError(
                        "Scaling {}: items in scaling must be numbers or None; "
                        "got {}".format(i, scaling)
                    )

            scales = [
                [float(x) if isinstance(x, int) else x for x in scaling]
                for scaling in scales
            ]
            # be less strict about floats than traitlets is

            with self.hold_trait_notifications():
                if colormap is None:
                    self.r_min = scales[0][0]
                    self.r_max = scales[0][1]
                    self.g_min = scales[1][0]
                    self.g_max = scales[1][1]
                    self.b_min = scales[2][0]
                    self.b_max = scales[2][1]
                else:
                    self.cmap_min = scales[0][0]
                    self.cmap_max = scales[0][1]
                if new_colormap is not False:
                    self.colormap = new_colormap
        else:
            # scales is None
            with self.hold_trait_notifications():
                if colormap is None:
                    self.r_min = None
                    self.r_max = None
                    self.g_min = None
                    self.g_max = None
                    self.b_min = None
                    self.b_max = None
                else:
                    self.cmap_min = None
                    self.cmap_max = None
                if new_colormap is not False:
                    self.colormap = new_colormap
