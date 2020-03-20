from ...cereal import serializable
from ..core import allow_reflect
from .primitive import Primitive


def _delayed_numpy_overrides():
    # avoid circular imports
    from descarteslabs.workflows.types.numpy import numpy_overrides

    return numpy_overrides


@serializable()
class Bool(Primitive):
    """
    Proxy boolean.

    Note that this cannot be compared with Python's ``and`` and ``or`` operators;
    you must use the bitwise operators ``&`` and ``|``. Also note that more parenthesis are needed
    with bitwise operators than with ``and`` and ``or``.

    Examples
    --------
    >>> from descarteslabs.workflows import Bool
    >>> my_bool = Bool(True)
    >>> my_bool
    <descarteslabs.workflows.types.primitives.bool_.Bool object at 0x...>
    >>> other_bool = Bool(False)
    >>> val = my_bool | other_bool
    >>> val.compute() # doctest: +SKIP
    True
    """

    _pytype = bool

    def __bool__(self):
        raise TypeError(
            "Conditionals and Python binary operators (like `and` and `or`) "
            "are not supported on Proxytype {} objects. "
            "Instead, use bitwise operators (like & and |). "
            "Don't forget extra parenthesis around the expressions you're comparing, "
            "since the precedence of bitwise operators "
            "is lower than that of `and` and `or`.".format(type(self).__name__)
        )

    def __array_function__(self, func, types, args, kwargs):
        """
        Override the behavior of a subset of NumPy functionality.

        Parameters
        ----------
        func: The NumPy function object that was called
        types: Collection of unique argument types from the original NumPy function
            call that implement `__array_function__`
        args: arguments directly passed from the original call
        kwargs: kwargs directly passed from the original call
        """
        numpy_overrides = _delayed_numpy_overrides()

        if func not in numpy_overrides.HANDLED_FUNCTIONS:
            raise NotImplementedError(
                "Using `{}` with a Workflows "
                "{} is not supported. If you want to use "
                "this function, you will first need to call "
                "`.compute` on your Workflows Array.".format(
                    func.__name__, type(self).__name__
                )
            )

        try:
            return numpy_overrides.HANDLED_FUNCTIONS[func](*args, **kwargs)
        except TypeError as e:
            e.args = (
                "When attempting to call numpy.{} with a "
                "Workflows {}, the following error occurred:\n\n".format(
                    func.__name__, type(self).__name__
                )
                + e.args[0],
            )
            raise

    def __array_ufunc__(self, ufunc, method, *inputs, **kwargs):
        """
        Override the behavior of NumPy's ufuncs.

        Parameters
        ----------
        ufunc: The ufunc object that was called
        method: Which ufunc method was called (one of "__call__", "reduce",
            "reduceat", "accumulate", "outer" or "inner")
        inputs: Tuple of the input arguments to ufunc
        kwargs: Dict of optional input arguments to ufunc
        """
        numpy_overrides = _delayed_numpy_overrides()

        if method == "__call__":
            if ufunc.__name__ not in numpy_overrides.HANDLED_UFUNCS:
                return NotImplemented
            else:
                return numpy_overrides.HANDLED_UFUNCS[ufunc.__name__](*inputs, **kwargs)
        else:
            # We currently don't support ufunc methods apart from __call__
            return NotImplemented

    def __invert__(self):
        return _delayed_numpy_overrides().logical_not(self)

    @allow_reflect
    def __eq__(self, other):
        return _delayed_numpy_overrides().equal(self, other)

    @allow_reflect
    def __ne__(self, other):
        return _delayed_numpy_overrides().not_equal(self, other)

    @allow_reflect
    def __and__(self, other):
        return _delayed_numpy_overrides().logical_and(self, other)

    @allow_reflect
    def __or__(self, other):
        return _delayed_numpy_overrides().logical_or(self, other)

    @allow_reflect
    def __xor__(self, other):
        return _delayed_numpy_overrides().logical_xor(self, other)

    @allow_reflect
    def __rand__(self, other):
        return _delayed_numpy_overrides().logical_and(other, self)

    @allow_reflect
    def __ror__(self, other):
        return _delayed_numpy_overrides().logical_or(other, self)

    @allow_reflect
    def __rxor__(self, other):
        return _delayed_numpy_overrides().logical_xor(other, self)
