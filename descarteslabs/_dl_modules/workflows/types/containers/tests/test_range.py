from ...primitives import Int

from .. import List, range as wf_range


def test_range():
    assert isinstance(wf_range(10), List[Int])
    assert isinstance(wf_range(0, 10), List[Int])
    assert isinstance(wf_range(0, 10, 2), List[Int])
