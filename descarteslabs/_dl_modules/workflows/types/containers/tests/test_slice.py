import pytest

from ...identifier import parameter
from ...primitives import Int
from .. import Slice


def test_init():
    s = Slice(0, 2)
    assert isinstance(s, Slice)
    assert s.params == ()


def test_from_slice():
    py_slice = slice(0, 2, 1)
    s = Slice.from_slice(py_slice)
    assert isinstance(s, Slice)
    assert s.params == ()


def test_init_merge_params():
    x = parameter("x", Int)
    y = parameter("y", Int)

    s = Slice(x, 10, y)
    assert s.params == (x, y)

    s = Slice.from_slice(slice(x, y, x))
    assert s.params == (x, y)


def test_promote():
    assert isinstance(Slice._promote(slice(1, 2, 3)), Slice)
    with pytest.raises(TypeError):
        Slice._promote(1, 2, 3)
