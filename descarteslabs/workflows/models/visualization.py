from typing import List, Sequence, Union

from descarteslabs.common.proto.visualization import visualization_pb2

from descarteslabs.workflows.types.geospatial import Image


class VizOption:
    def __init__(
        self,
        id: str,
        bands: Union[str, Sequence[str]],
        checkerboard: bool = False,
        colormap: str = None,
        reduction: str = "mosaic",
        scales: Sequence[Sequence[float]] = None,
    ):
        """
        Construct a VizOption instance.

        Parameters
        ----------
        id: str
            Name of the VizOption instance.
        bands: str or list
            List of band names to render. Can be a list of band names, or a string
            of space separated band names.
        checkerboard: bool
            True if missing pixels should hold checkerboard pattern. Defaults to False.
        colormap: str
            Name of colormap to use if single band. Defaults to None.
        reduction: str
            Name of reduction operation. Defaults to mosaic.
        scales: list
            List of list of floats representing scales for rendering bands,
            must have same length as bands, or None for automatic scaling.
            Defaults to None.
        """
        if isinstance(bands, str):
            bands = [b for b in bands.split(" ") if b]
        message = visualization_pb2.VizOption(
            id=id, bands=bands, checkerboard=checkerboard, reduction=reduction,
        )
        if colormap:
            # validate that it is supported
            if colormap not in Image._colormaps:
                raise ValueError(f"Unknown colormap type:{colormap}")
            message.colormap = colormap
        if scales:
            message.scales.extend(
                [visualization_pb2.VizOption.Scales(min=s[0], max=s[1]) for s in scales]
            )
        self._message = message

    @classmethod
    def _from_proto(cls, message: visualization_pb2.VizOption,) -> "VizOption":
        """
        Low-level constructor for a `VizOption` object from a Protobuf message.

        Do not use this method directly; use `__init__` instead.

        Parameters
        ----------
        proto_message
            Protobuf message for the `PublishedGraft`

        Returns
        -------
        VizOption
        """

        obj = cls.__new__(cls)  # bypass __init__
        obj._message = message
        return obj

    @property
    def id(self) -> str:
        """Get the id of the visualization parameters."""
        return self._message.id

    @property
    def bands(self) -> List[str]:
        """Get the bands to visualize."""
        return self._message.bands

    @property
    def checkerboard(self) -> bool:
        """Get the checkerboard setting for visualization."""
        return self._message.checkerboard

    @property
    def colormap(self) -> Union[str, None]:
        """Get the colormap for visualization."""
        colormap = self._message.colormap
        return colormap if colormap else None

    @property
    def reduction(self) -> str:
        """Get the reduction operation for visualization."""
        return self._message.reduction

    @property
    def scales(self) -> Union[List[List[float]], None]:
        """Get the scales for visualization."""
        scales = self._message.scales
        return [[s.min, s.max] for s in scales] if scales else None

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and self._message == other._message

    def __repr__(self) -> str:
        return f"""VizOption(
    id={self.id!r},
    bands={self.bands!r},
    checkerboard={self.checkerboard!r},
    colormap={self.colormap!r},
    reduction={self.reduction!r},
    scales={self.scales!r}
)"""
