from typing import List, Optional, Sequence, Union

from descarteslabs.common.proto.visualization import visualization_pb2

from descarteslabs.workflows.types.geospatial import Image

from .tile_url import validate_scales


class VizOption:
    def __init__(
        self,
        id: str,
        bands: Union[str, Sequence[str]],
        checkerboard: bool = False,
        colormap: Optional[str] = None,
        reduction: str = "mosaic",
        scales: Optional[Sequence[Sequence[float]]] = None,
        description: str = "",
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
        checkerboard: bool, default False
            True if missing pixels should hold checkerboard pattern. Defaults to False.
        colormap: str, optional
            Name of colormap to use if single band. Defaults to None.
        reduction: str, default "mosaic".
            Name of reduction operation. Defaults to mosaic.
        scales: list, optional
            List of list of floats representing scales for rendering bands,
            must have same length as bands, or None for automatic scaling.
            Defaults to None.
        description: str, optional
            Description of the visualization option.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> v = wf.VizOption('RGB', bands='red green blue', reduction='median', description='Median RGB composite') # doctest: +SKIP
        >>> v # doctest: +SKIP
        VizOption(
        ... id='RGB',
        ... bands=['red', 'green', 'blue'],
        ... checkerboard=False,
        ... colormap=None,
        ... reduction='median',
        ... scales=None,
        ... description='Median RGB composite'
        )
        """  # noqa: E501
        if isinstance(bands, str):
            bands = [b for b in bands.split(" ") if b]
        message = visualization_pb2.VizOption(
            id=id,
            bands=bands,
            checkerboard=checkerboard,
            reduction=reduction,
            description=description,
        )
        if colormap:
            # validate that it is supported
            if colormap not in Image._colormaps:
                raise ValueError(f"Unknown colormap type:{colormap}")
            message.colormap = colormap
        if scales:
            message.scales.extend(
                [
                    visualization_pb2.VizOption.Scales(min=s[0], max=s[1])
                    for s in validate_scales(scales)
                ]
            )
        self._message = message

    @classmethod
    def _from_proto(
        cls,
        message: visualization_pb2.VizOption,
    ) -> "VizOption":
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

    @property
    def description(self) -> str:
        """Get the description of the visualization option."""
        return self._message.description

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and self._message == other._message

    def __repr__(self) -> str:
        return f"""VizOption(
    id={self.id!r},
    bands={self.bands!r},
    checkerboard={self.checkerboard!r},
    colormap={self.colormap!r},
    reduction={self.reduction!r},
    scales={self.scales!r},
    description={self.description!r}
)"""
