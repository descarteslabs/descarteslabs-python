import numpy as np

from descarteslabs.common.workflows.utils import masked_stack

# Fixtures:

foo = np.ma.masked_array(
    np.array([[1, 2], [3, 4]]), np.array([[True, False], [False, True]])
)
bar = np.ma.masked_array(
    np.array([[5, 6], [7, 8]]), np.array([[False, False], [True, False]])
)
baz = np.ma.masked_array(np.array([[9, 10], [11, 12]]), np.ma.nomask)
qux = np.ma.masked_array(
    np.array([[13, 14], [15, 16]]), np.array([[False, False], [False, False]])
)


def assert_stacked_correctly(a, b):
    stack = masked_stack([a, b])
    np.testing.assert_array_equal(np.ma.getdata(stack)[0], np.ma.getdata(a))
    np.testing.assert_array_equal(np.ma.getdata(stack)[1], np.ma.getdata(b))
    np.testing.assert_array_equal(np.ma.getmaskarray(stack)[0], np.ma.getmaskarray(a))
    np.testing.assert_array_equal(np.ma.getmaskarray(stack)[1], np.ma.getmaskarray(b))


def assert_shrunk_mask(a, b):
    stack = masked_stack([a, b])
    np.testing.assert_array_equal(np.ma.getmaskarray(stack), np.ma.nomask)


def test_masked_stack():
    assert_stacked_correctly(foo, bar)
    assert_stacked_correctly(foo, baz)
    assert_stacked_correctly(bar, baz)
    assert_stacked_correctly(foo, qux)
    assert_stacked_correctly(bar, qux)


def test_masked_stack_shrunk_mask():
    assert_shrunk_mask(baz, qux)
