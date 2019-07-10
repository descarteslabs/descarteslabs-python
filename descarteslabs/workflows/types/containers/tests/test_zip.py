import pytest

from ...primitives import Int, Str
from ...core import ProxyTypeError

from .. import Tuple, List, zip as wf_zip


def test_zip():
    l1 = List[Int]([1, 2, 3])
    l2 = List[Str](["a", "b", "c"])
    l3 = List[Tuple[Int, Str]]([(1, "foo"), (3, "bar")])

    zipped = wf_zip(l1, l2, l3)
    assert isinstance(zipped, List[Tuple[Int, Str, Tuple[Int, Str]]])


@pytest.mark.parametrize(
    "seqs",
    [
        [List[Int]([1, 2, 3]), [1, 2, 3]],
        [List[Int]([1, 2, 3]), Tuple[Int, Int, Int]([1, 2, 3])],
    ],
)
def test_zip_wrong_args(seqs):
    with pytest.raises(
        ProxyTypeError, match="All arguments to 'zip' must be Proxytype Lists"
    ):
        wf_zip(*seqs)
