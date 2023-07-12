# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
A list-based sequence with helper methods, which serves as the base class for `SceneCollection`.
"""

import collections
import itertools
from typing import Generic, TypeVar

from ...client.deprecation import deprecate
from ...common.property_filtering.filtering import Expression

T = TypeVar("T")


# TODO: maybe subclass collections.UserList?
class Collection(Generic[T]):
    """
    List-based sequence with convenience methods for mapping and filtering,
    and NumPy-style fancy indexing
    """

    def __init__(self, iterable=None, item_type=None):
        if iterable is None:
            self._list = []
        else:
            self._list = list(iterable)
            if item_type is not None:
                self._item_type = item_type
            else:
                item_type = getattr(self, "_item_type", None)

            if item_type is not None:
                if not all(map(lambda i: isinstance(i, item_type), self._list)):
                    raise ValueError(
                        f"item is not of required type {item_type.__name__}"
                    )

    def __getitem__(self, idx):
        """
        self[start:stop:step] <--> Collection(list(self[start:stop:step]))
        self[<list>] <--> Collection(self[i] for i in <list>)
        Can slice like a normal list, or with a list of indices to select
        """

        if isinstance(idx, list) or type(idx).__name__ == "ndarray":
            subset = [self._list[i] for i in idx]
            return self._cast_and_copy_attrs_to(subset)
        else:
            if isinstance(idx, slice):
                return self._cast_and_copy_attrs_to(self._list[idx])
            else:
                return self._list[idx]

    def __setitem__(self, idx, item):
        """
        Can assign a scalar, or a list of equal length to the slice,
        to any valid slice (including an list of indices)
        """
        item_type = getattr(self, "_item_type", None)
        if isinstance(idx, (list, slice)) or type(idx).__name__ == "ndarray":
            if isinstance(idx, slice):
                idx = list(range(*idx.indices(len(self))))
            if isinstance(item, str) or not isinstance(
                item, collections.abc.Sequence
            ):  # if scalar
                item = [item] * len(idx)
            item = list(item)
            if len(idx) != len(item):
                raise ValueError(
                    "Cannot assign {} items to a slice {} items long".format(
                        len(item), len(idx)
                    )
                )
            for i, x in zip(idx, item):
                if item_type is not None and not isinstance(x, item_type):
                    raise ValueError(
                        f"item is not of required type {item_type.__name__}"
                    )
                self._list[i] = x
        else:
            if item_type is not None and not isinstance(x, item_type):
                raise ValueError(f"item is not of required type {item_type.__name__}")
            self._list[idx] = item

    def __iter__(self):
        return iter(self._list)

    def __reversed__(self):
        return reversed(self._list)

    def __contains__(self, other):
        return other in self._list

    def __len__(self):
        return len(self._list)

    def __eq__(self, other):
        return self._list == other

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, repr(self._list))

    def _cast_and_copy_attrs_to(self, other):
        # used to copy over any attrs a subclass may have set
        other = self.__class__(other)
        for k, v in self.__dict__.items():
            if k != "_list":
                setattr(other, k, v)
        return other

    @property
    def each(self):
        """
        Any operations chained onto
        :attr:`~descarteslabs.common.collection.Collection.each` (attribute access,
        item access, and calls) are applied to each item in the
        :class:`~descarteslabs.common.collection.Collection`.

        Yields
        ------
        Any
            The result of an item with all operations following
            :attr:`~descarteslabs.common.collection.Collection.each` applied to it.

        Notes
        -----
        * Add :meth:`~descarteslabs.common.collection.Eacher.combine`
          at the end of the operations chain to combine the results into a
          list by default, or any container type passed into
          :meth:`~descarteslabs.common.collection.Eacher.combine`
        * Use
          :meth:`pipe(f, *args, **kwargs) <descarteslabs.common.collection.Eacher.pipe>`
          to yield ``f(x, *args, **kwargs)`` for each item ``x`` yielded by the
          preceeding operations chain

        Examples
        --------
        >>> c = Collection(["one", "two", "three", "four"])
        >>> for x in c.each.capitalize():
        ...     print(x)
        One
        Two
        Three
        Four
        >>> c.each.capitalize()[:2]
        'On'
        'Tw'
        'Th'
        'Fo'
        >>> c.each.capitalize().pipe(len)
        3
        3
        5
        4
        >>> list(c.each.capitalize().pipe(len).combine(set))
        [3, 4, 5]
        """
        return Eacher(iter(self._list))

    def map(self, f):
        """Returns a :class:`~descarteslabs.common.collection.Collection` of ``f`` applied to each item.

        Parameters
        ----------
        f : callable
            Apply function ``f`` to each element of the collection and return the result
            as a collection.

        Returns
        -------
        Collection
            A collection with the results of the function ``f`` applied to each element
            of the original collection.
        """

        res = (f(x) for x in self._list)
        item_type = getattr(self, "_item_type", None)
        if item_type is None or all(map(lambda i: isinstance(i, item_type), res)):
            return self._cast_and_copy_attrs_to(res)
        else:
            return Collection(res)

    def filter(self, predicate):
        """Returns a :class:`~descarteslabs.common.collection.Collection` filtered by predicate.

        Predicate can either be a ``callable`` or an
        :py:class:`~descarteslabs.common.property_filtering.filtering.Expression`
        from :ref:`property_filtering`.

        If the predicate is a ``callable``, :py:meth:`filter` will return all items
        for which ``predicate(item)`` is ``True``.

        If the predicate is an
        :py:class:`~descarteslabs.common.property_filtering.filtering.Expression`,
        :py:meth:`filter` will return all items
        for which ``predicate.evaluate(item)`` is ``True``.

        Parameters
        ----------
        predicate : callable or Expression
            Either a callable or a :ref:`property_filtering` `Expression` which is
            called or evaluated for each item in the list.

        Returns
        -------
        Collection
            A new collection with only those items for which the predicate returned
            or evaluated to ``True``.
        """

        if isinstance(predicate, Expression):
            res = (x for x in self._list if predicate.evaluate(x))
        else:
            res = (x for x in self._list if predicate(x))

        return self._cast_and_copy_attrs_to(res)

    def sorted(self, *predicates, **reverse):
        """
        Returns a :class:`~descarteslabs.common.collection.Collection`,
        sorted by predicates in ascending order.

        Each predicate can be a key function, or a string of dot-chained attributes
        to use as sort keys. The reverse flag returns results in descending order.

        Parameters
        ----------
        predicates : callable or str
            Any positional arguments are predicates. If the predicate is a string,
            it denotes an attribute for each element, potentially with levels separated
            by a dot. If the predicate is a callable, it must return the value to sort
            by for each given element.
        reverse : bool
            The sort is ascending by default, by setting ``reverse`` to
            ``True``, the sort will be descending.

        Returns
        -------
        Collection
            The sorted collection.

        Examples
        --------
        >>> import collections
        >>> FooBar = collections.namedtuple("FooBar", ["foo", "bar"])
        >>> X = collections.namedtuple("X", "x")
        >>> c = Collection([FooBar(1, X("one")), FooBar(2, X("two")), FooBar(3, X("three"))])

        >>> c.sorted("foo")
        Collection([FooBar(foo=1, bar=X(x='one')), FooBar(foo=2, bar=X(x='two')), FooBar(foo=3, bar=X(x='three'))])
        >>> c.sorted("bar.x")
        Collection([FooBar(foo=1, bar=X(x='one')), FooBar(foo=3, bar=X(x='three')), FooBar(foo=2, bar=X(x='two'))])
        """

        if len(predicates) == 0:
            raise TypeError("No predicate(s) given to sorted")
        predicates = [
            self._str_to_predicate(p) if isinstance(p, str) else p for p in predicates
        ]
        if len(predicates) == 1:
            predicate = predicates[0]
        else:

            def predicate(v):
                return tuple(p(v) for p in predicates)

        res = sorted(self, key=predicate, **reverse)
        return self._cast_and_copy_attrs_to(res)

    def sort(self, field, ascending=True):
        """
        Returns a :class:`~descarteslabs.common.collection.Collection`,
        sorted by the given field and direction.

        Parameters
        ----------
        field : str
            The name of the field to sort by
        ascending : bool
            Sorts results in ascending order if True (the default),
            and in descending order if False.

        Returns
        -------
        Collection
            The sorted collection.

        Example
        -------
        >>> from descarteslabs.catalog import Product
        >>> collection = Product.search().collect() # doctest: +SKIP
        >>> sorted_collection = collection.sort("created", ascending=False) # doctest: +SKIP
        >>> sorted_collection # doctest: +SKIP
        """
        return self.sorted(field, reverse=not ascending)

    def groupby(self, *predicates):
        """Groups items by predicates.

        Groups items by predicates and yields tuple of ``(group, items)``
        for each group, where ``items`` is a
        :class:`~descarteslabs.common.collection.Collection`.

        Each predicate can be a key function, or a string of dot-chained attributes
        to use as sort keys.

        Parameters
        ----------
        predicates : callable or str
            Any positional arguments are predicates. If the predicate is a string,
            it denotes an attribute for each element, potentially with levels separated
            by a dot. If the predicate is a callable, it must return the value to sort
            by for each given element.

        Yields
        ------
        Tuple[str, Collection]
            A tuple of ``(group, Collection)`` for each group.

        Examples
        --------
        >>> import collections
        >>> FooBar = collections.namedtuple("FooBar", ["foo", "bar"])
        >>> c = Collection([FooBar("a", True), FooBar("b", False), FooBar("a", False)])

        >>> for group, items in c.groupby("foo"):
        ...     print(group)
        ...     print(items)
        a
        Collection([FooBar(foo='a', bar=True), FooBar(foo='a', bar=False)])
        b
        Collection([FooBar(foo='b', bar=False)])
        >>> for group, items in c.groupby("bar"):
        ...     print(group)
        ...     print(items)
        False
        Collection([FooBar(foo='b', bar=False), FooBar(foo='a', bar=False)])
        True
        Collection([FooBar(foo='a', bar=True)])
        """

        if len(predicates) == 0:
            raise TypeError("No predicate(s) given to groupby")
        predicates = [
            self._str_to_predicate(p) if isinstance(p, str) else p for p in predicates
        ]
        if len(predicates) == 1:
            predicate = predicates[0]
        else:

            def predicate(v):
                return tuple(p(v) for p in predicates)

        ordered = self.sorted(predicate)
        for group, items in itertools.groupby(ordered, predicate):
            yield group, self._cast_and_copy_attrs_to(items)

    def append(self, x):
        """Append x to the end of this :class:`~descarteslabs.common.collection.Collection`.

        The type of the item must match the type of the collection.

        Parameters
        ----------
        x : Any
            Add an item to the collection
        """

        item_type = getattr(self, "_item_type", None)
        if item_type is not None and not isinstance(x, item_type):
            raise ValueError(f"item is not of required type {item_type.__name__}")

        self._list.append(x)

    def extend(self, x):
        """Extend this :class:`~descarteslabs.common.collection.Collection` by appending elements from the iterable.

        The type of the items in the list must all match the type of the collection.

        Parameters
        ----------
        x : List[Any]
            Extend a collection with the items from the list.
        """

        item_type = getattr(self, "_item_type", None)
        if item_type is not None and not all(
            map(lambda i: isinstance(i, item_type), x)
        ):
            raise ValueError(f"item is not of required type {item_type.__name__}")

        self._list.extend(x)

    @staticmethod
    def _str_to_predicate(string):
        attrs = string.split(".")

        def predicate(x):
            result = x
            for attr in attrs:
                result = getattr(result, attr)
            return result

        return predicate


class Eacher(object):
    "Applies operations chained onto it to each item in an iterator"

    __slots__ = "_iterable"

    def __init__(self, iterable):
        self._iterable = iterable

    def __iter__(self):
        return iter(self._iterable)

    def __getattr__(self, attr):
        return Eacher(getattr(x, attr) for x in self)

    def __getitem__(self, idx):
        return Eacher(x[idx] for x in self)

    def __call__(self, *args, **kwargs):
        return Eacher(x(*args, **kwargs) for x in self)

    @deprecate(renamed={"collection": "op"})
    def combine(self, op=list):
        "self.combine(collection) <--> op(iter(self))"

        return op(iter(self))

    def collect(self, collection=Collection):
        "self.collect(collection) <--> collection(iter(self))"

        return collection(iter(self))

    def pipe(self, callable, *args, **kwargs):
        "self.pipe(f, *args, **kwargs) <--> f(x, *args, **kwargs) for x in self"

        return Eacher(callable(x, *args, **kwargs) for x in self)

    def __repr__(self):
        max_length = 8
        objs = list(itertools.islice(self, max_length))
        try:
            head = [repr(x) for x in objs]
        except Exception:
            return "<%s instance at %#x>" % (self.__class__.__name__, id(self))
        else:
            s = "\n".join(head)
            if len(head) == max_length:
                s = s + "\n..."
            return s
