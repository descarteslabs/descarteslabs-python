def _delayed_numpy_functions():
    # avoid circular imports
    from ..numpy import numpy_functions

    return numpy_functions


def _delayed_numpy_ufuncs():
    # avoid circular imports
    from ..numpy import numpy_ufuncs

    return numpy_ufuncs


class NumPyMixin:
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
        numpy_functions = _delayed_numpy_functions()

        if func not in numpy_functions.HANDLED_FUNCTIONS:
            raise NotImplementedError(
                "Using `{0}` with a Workflows "
                "{1} is not supported. If you want to use "
                "this function, you will first need to call "
                "`.compute` on your Workflows {1}.".format(
                    func.__name__, type(self).__name__
                )
            )

        try:
            return numpy_functions.HANDLED_FUNCTIONS[func](*args, **kwargs)
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
        numpy_ufuncs = _delayed_numpy_ufuncs()

        if method == "__call__":
            if ufunc.__name__ not in numpy_ufuncs.HANDLED_UFUNCS:
                return NotImplementedError(
                    "{} is not supported by Workflows types.".format(ufunc.__name__)
                )
            else:
                return numpy_ufuncs.HANDLED_UFUNCS[ufunc.__name__](*inputs, **kwargs)
        elif method == "reduce":
            raise NotImplementedError(
                "The `reduce` ufunc method is not supported by Workflows types."
            )
        elif method == "reduceat":
            raise NotImplementedError(
                "The `reduceat` ufunc method is not supported by Workflows types."
            )
        elif method == "accumulate":
            raise NotImplementedError(
                "The `accumulate` ufunc method is not supported by Workflows types."
            )
        elif method == "outer":
            raise NotImplementedError(
                "The `outer` ufunc method is not supported by Workflows types."
            )
        else:
            return NotImplemented
