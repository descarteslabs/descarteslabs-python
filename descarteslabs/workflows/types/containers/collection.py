import operator

from ..function import Function
from ..primitives import Bool, Int, NoneType
from ..core import Proxytype, ProxyTypeError, typecheck_promote


from ._check_valid_binop import check_valid_binop_for

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
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("sentinel-2:L1C")
        >>> dates = col.map(lambda img: img.properties["date"])
        >>> type(dates).__name__
        'List[Datetime]'
        """
        func = Function.from_callable(func, self._element_type)
        result_type = func.return_type

        container_type = (
            type(self)
            if result_type is self._element_type
            else _DelayedList()[result_type]
        )

        return container_type._from_apply("wf.map", self, func)

    @typecheck_promote(lambda self: Function[self._element_type, {}, Bool])
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
        >>> from descarteslabs.workflows import ImageCollection
        >>> col = ImageCollection.from_id("sentinel-2:L1C")
        >>> filtered = col.filter(lambda img: img.properties["date"].year == 2018)
        """
        return self._from_apply("wf.filter", self, func)

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
        >>> from descarteslabs.workflows import List, Int
        >>> def add(accumulated, current):
        ...     return accumulated + current
        >>> my_list = List[Int]([1, 2])
        >>> my_list.reduce(add, initial=Int(10)).compute()  # doctest: +SKIP
        13
        """
        initial_type = _initial_reduce_type(initial, self._element_type)
        func_type = Function[initial_type, self._element_type, {}, initial_type]

        delayed_func = func_type(func)

        if initial is REDUCE_INITIAL_DEFAULT:
            return initial_type._from_apply("wf.reduce", self, delayed_func)
        else:
            return initial_type._from_apply(
                "wf.reduce", self, delayed_func, initial=initial
            )

    def length(self):
        """Length is equivalent to the Python ``len`` operator.

        Returns
        -------
        Int
            An Int Proxytype

        Example
        -------
        >>> from descarteslabs.workflows import List, Int
        >>> my_list = List[Int]([1, 2, 3])
        >>> my_list.length().compute() # doctest: +SKIP
        3
        """
        return Int._from_apply("wf.length", self)

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

        Example
        -------
        >>> from descarteslabs.workflows import List, Int
        >>> my_list = List[Int]([1, 2, 3])
        >>> my_list.contains(2).compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.contains", self, other)

    @typecheck_promote(key=None, reverse=Bool)
    def sorted(self, key=None, reverse=False):
        """
        Copy of this collection, sorted by a key function.

        Parameters
        ----------
        key: Function, optional, default None
            Function which takes an element and returns a value to sort by.
            If not given, sorts by the elements themselves.
        reverse: Bool, default False
            Sorts in ascending order if False (default), descending if True.

        Example
        -------
        >>> from descarteslabs.workflows import List, Int
        >>> my_list = List[Int]([1, 4, 2, 3])
        >>> my_list.sorted().compute() # doctest: +SKIP
        [1, 2, 3, 4]
        """
        key = self._make_sort_key(key)
        return self._from_apply("wf.sorted", self, key=key, reverse=reverse)

    def _make_sort_key(self, key):
        "Delay a Python sort key function, or None, and ensure it produces an orderable type"
        if key is None or isinstance(key, NoneType):
            key_type = self._element_type
            invalid_msg = unsupported_msg = (
                "{} does not contain orderable items and cannot be sorted directly. "
                "Please provide a key function.".format(type(self).__name__)
            )
        else:
            key = Function.from_callable(key, self._element_type)
            key_type = key._type_params[-1]
            invalid_msg = (
                "Sort key function produced {type_name}, which is not orderable, "
                "since comparing {type_name}s produces {result_name}, not Bool."
            )
            unsupported_msg = (
                "Sort key function produced non-orderable type {type_name}"
            )

        check_valid_binop_for(
            operator.lt,
            key_type,
            error_prefix=None,
            unsupported_msg=unsupported_msg,
            invalid_msg=invalid_msg,
        )

        return key

    def __reversed__(self):
        return self._from_apply("wf.reversed", self)
