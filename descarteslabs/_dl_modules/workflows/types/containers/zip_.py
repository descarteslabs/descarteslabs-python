from ..core import ProxyTypeError

from .list_ import List
from .tuple_ import Tuple

from ..primitives import Str
from ..geospatial import ImageCollection, Image, FeatureCollection, GeometryCollection

zippable_types = (
    List,
    ImageCollection,
    Image,
    FeatureCollection,
    GeometryCollection,
    Str,
)


def zip(*sequences):
    """
    Returns a `List` of `Tuple`, where each tuple contains the i-th element
    from each of the arguments. All arguments must be Proxytype `List`,
    `~.geospatial.ImageCollection` (zips along axis="images"), `~.geospatial.Image` (zips along axis="bands"),
    `~.geospatial.FeatureCollection`, `~.geospatial.GeometryCollection`, or `Str`.

    The returned `List` is truncated in length to the length of the shortest
    argument sequence.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> imagecollection = wf.ImageCollection.from_id("sentinel-2:L1C")
    >>> int_list = wf.List[wf.Int]([1, 2, 3, 4])
    >>> str_list = wf.List[wf.Str](["foo", "bar", "baz"])
    >>> zipped = wf.zip(imagecollection, int_list, str_list)
    >>> zipped
    <descarteslabs.workflows.types.containers.list_.List[Tuple[Image, Int, Str]] object at 0x...>
    >>> wf.zip(int_list, str_list, wf.Str("abcdefg")).compute() # doctest: +SKIP
    [(1, 'foo', 'a'), (2, 'bar', 'b'), (3, 'baz', 'c')]
    """
    for i, seq in enumerate(sequences):
        if not isinstance(seq, zippable_types):
            raise ProxyTypeError(
                "All arguments to 'zip' must be Proxytype sequences (the Python equivalents are not supported): "
                "one of {}.\n"
                "Argument {} is {!r}: {}".format(
                    ", ".join(t.__name__ for t in zippable_types),
                    i,
                    type(seq).__name__,
                    seq,
                )
            )
    itemtypes = tuple(getattr(seq, "_element_type", type(seq)) for seq in sequences)
    tuple_type = Tuple[itemtypes]

    return List[tuple_type]._from_apply("wf.zip", *sequences)
