import pytest

from .. import Slice


def test_init():
    s = Slice(0, 2)
    assert isinstance(s, Slice)


def test_from_slice():
    py_slice = slice(0, 2, 1)
    s = Slice.from_slice(py_slice)
    assert isinstance(s, Slice)


def test_promote():
    assert isinstance(Slice._promote(slice(1, 2, 3)), Slice)
    with pytest.raises(TypeError):
        Slice._promote(1, 2, 3)
