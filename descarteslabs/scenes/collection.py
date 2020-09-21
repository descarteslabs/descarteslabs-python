# Copyright 2018-2020 Descartes Labs.
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

import itertools
import collections
import six


# TODO: maybe subclass collections.UserList?
class Collection(object):
    """
    List-based sequence with convenience methods for mapping and filtering,
    and NumPy-style fancy indexing
    """

    def __init__(self, iterable=None):
        if iterable is None:
            self._list = []
        else:
            self._list = list(iterable)

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

        if isinstance(idx, (list, slice)) or type(idx).__name__ == "ndarray":
            if isinstance(idx, slice):
                idx = list(range(*idx.indices(len(self))))
            if isinstance(item, six.string_types) or not isinstance(
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
                self._list[i] = x
        else:
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
        for k, v in six.iteritems(self.__dict__):
            if k != "_list":
                setattr(other, k, v)
        return other

    @property
    def each(self):
        """
        Any operations chained onto
        :attr:`~descarteslabs.scenes.collection.Collection.each` (attribute access,
        item access, and calls) are applied to each item in the
        :class:`~descarteslabs.scenes.collection.Collection`.

        Notes
        -----
            * Add :meth:`~descarteslabs.scenes.collection.Eacher.combine`
              at the end of the operations chain to combine the results into a
              list by default, or any container type passed into
              :meth:`~descarteslabs.scenes.collection.Eacher.combine`
            * Use
              :meth:`pipe(f, *args, **kwargs) <descarteslabs.scenes.collection.Eacher.pipe>`
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

        Yields
        ------
            item with all operations following :attr:`~descarteslabs.scenes.collection.Collection.each` applied to it

        """
        return Eacher(iter(self._list))

    def map(self, f):
        "Returns a :class:`~descarteslabs.scenes.collection.Collection` of ``f`` applied to each item"

        res = (f(x) for x in self._list)
        return self._cast_and_copy_attrs_to(res)

    def filter(self, predicate):
        "Returns a :class:`~descarteslabs.scenes.collection.Collection` of items for which ``predicate(item)`` is True"

        res = (x for x in self._list if predicate(x))
        return self._cast_and_copy_attrs_to(res)

    def sorted(self, *predicates, **reverse):
        """
        Returns a :class:`~descarteslabs.scenes.collection.Collection`,
        sorted by predicates in ascending order.

        Each predicate can be a key function, or a string of dot-chained attributes
        to use as sort keys. The reverse flag returns results in descending order.

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
            self._str_to_predicate(p) if isinstance(p, six.string_types) else p
            for p in predicates
        ]
        if len(predicates) == 1:
            predicate = predicates[0]
        else:

            def predicate(v):
                return tuple(p(v) for p in predicates)

        res = sorted(self, key=predicate, **reverse)
        return self._cast_and_copy_attrs_to(res)

    def groupby(self, *predicates):
        """
        Groups items by predicates and yields tuple of ``(group, items)``
        for each group, where ``items`` is a
        :class:`~descarteslabs.scenes.collection.Collection`.

        Each predicate can be a key function, or a string of dot-chained attributes
        to use as sort keys.

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
            self._str_to_predicate(p) if isinstance(p, six.string_types) else p
            for p in predicates
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
        "Append x to the end of this :class:`~descarteslabs.scenes.collection.Collection`"

        self._list.append(x)

    def extend(self, x):
        "Extend this :class:`~descarteslabs.scenes.collection.Collection` by appending elements from the iterable"

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

    def combine(self, collection=list):
        "``self.combine(collection) <--> collection(iter(self))``"

        return collection(iter(self))

    def pipe(self, callable, *args, **kwargs):
        "``self.pipe(f, *args, **kwargs) <--> f(x, *args, **kwargs) for x in self``"

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
