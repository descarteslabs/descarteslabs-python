from ..function import Function
from ..primitives import Any, Bool, Int
from ..core import Proxytype, ProxyTypeError, typecheck_promote


REDUCE_INITIAL_DEFAULT = "__NO_INITIAL_REDUCE_VALUE__"


def _DelayedList():
    from .list_ import List

    return List


def _initial_reduce_type(initial, element_type):
    if initial is not REDUCE_INITIAL_DEFAULT:
        # initial value may need to be promoted
        if not isinstance(initial, Proxytype):
            raise ProxyTypeError(
                "The initial value must be instance of 'Proxytype' and not {}.".format(
                    type(initial)
                )
            )
        return type(initial)
    else:
        return element_type


class CollectionMixin:
    def __init__(self):
        raise TypeError("Please use `List` instead.")

    @property
    def _element_type(self):
        return self._type_params[0]

    def map(self, func):
        """Map a function over an iterable proxytype.

        Parameters
        ----------
        func : Python callable
            A function that takes a single argument and returns another
            proxytype.

        Returns
        -------
        Proxtype
            The return type is dependent on the ``func`` and the type of
            the element returned. For example, calling `map` over an
            `.ImageCollection` with a function that returns the date of the
            `~.geospatial.Image` will now be a ``List[Datetime]``.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("sentinel-2:L1C")
        >>> dates = col.map(lambda img: img.properties["date"])
        >>> type(dates).__name__
        'List[Datetime]'
        """
        delayed_func = Function._delay(func, None, self._element_type)

        result_type = type(delayed_func)

        container_type = (
            type(self)
            if result_type is self._element_type
            else _DelayedList()[result_type]
        )

        return container_type._from_apply("map", self, delayed_func)

    def filter(self, func):
        """Filter elements from an iterable proxytype.

        Parameters
        ----------
        func : Python callable
            A function that takes a single argument and returns a `.Bool`
            Proxytype.

        Returns
        -------
        Proxtype
            A Proxytype of the same type having only the elements where
            ``func`` returns ``True``.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> col = wf.ImageCollection.from_id("sentinel-2:L1C")
        >>> filtered = col.filter(lambda img: img.properties["date"].year == 2018)
        """
        delayed_func = Function._delay(func, Bool, self._element_type)

        return self._from_apply("filter", self, delayed_func)

    def reduce(self, func, initial=REDUCE_INITIAL_DEFAULT):
        """Reduce a collection of elements to a single element.

        NOTE: Optimized reducers such as ``sum`` have been implemented
        for some classes.

        Parameters
        ----------
        func : Python callable
            A function where the left argument is the accumulated value
            (the result of the previous call to ``func``) and the right
            argument is the next value from the iterable. The function
            should return a single proxtype.
        initial : Proxytype, optional
            An optional proxtype to provide for the first iteration. If
            no value is provided, the first element will be used.

        Returns
        -------
        Proxytype
            The return value of ``func`` for the last iteration of the
            collection.

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> def add(accumulated, current):
        ...     return accumulated + current
        >>> l = wf.List[wf.Int]([1, 2])
        >>> reduced = l.reduce(add, initial=wf.Int(10))
        >>> reduced.compute()  # doctest: +SKIP
        13
        """
        initial_type = _initial_reduce_type(initial, self._element_type)

        delayed_func = Function._delay(
            func, initial_type, self._element_type, initial_type
        )

        if initial is REDUCE_INITIAL_DEFAULT:
            return initial_type._from_apply("reduce", self, delayed_func)
        else:
            return initial_type._from_apply("reduce", self, delayed_func, initial)

    def length(self):
        """Length is equivalent to the Python ``len`` operator.

        Returns
        -------
        Int
            An Int Proxytype
        """
        return Int._from_apply("length", self)

    @typecheck_promote(lambda self: self._element_type)
    def contains(self, other):
        """Contains is equivalient to the Python ``in`` operator.

        Parameters
        ----------
        other : Proxytype
            A Proxytype or type that can be promoted to a Proxytype

        Returns
        -------
        Bool
            A Bool Proxytype
        """
        return Bool._from_apply("contains", self, other)

    @typecheck_promote(key=None, reverse=Bool)
    def sorted(self, key=None, reverse=False):
        """
        Copy of this collection, sorted by a key function.

        Parameters
        ----------
        key: Function
            Function which takes an element and returns a value to sort by
        reverse: Bool, default False
            Sorts in ascending order if False (default), descending if True.
        """
        if key is not None:
            key = Function._delay(key, None, self._element_type)
            try:
                if type(key < key) not in (Any, Bool):
                    raise TypeError("Comparison did not result in Bool")
            except TypeError:
                raise TypeError(
                    "Key function produced non-orderable type {}".format(type(key))
                )

        return self._from_apply("sorted", self, key, reverse)
