import collections
import pytest
import unittest

from descarteslabs.scenes import Collection


class SubCollection(Collection):
    def __init__(self, iterable=None, foo=1):
        super(SubCollection, self).__init__(iterable)
        self.foo = foo
        self._secret = True


class TestCollection(unittest.TestCase):
    def test_init_and_overloads(self):
        c = Collection()
        assert len(c) == 0

        c = Collection([0, 1, 2])
        assert len(c) == 3
        for i, x in enumerate(c):
            assert i == x
        assert 1 in c
        assert c == c
        # self.assertEqual(reversed(reversed(c)), c)

    def test_slicing_and_setting(self):
        c = Collection(range(10))
        assert c[4] == 4
        assert c[:2] == [0, 1]
        assert c[:] == list(range(10))

        slice = [0, 3, 5]
        shouldbe = [c[i] for i in slice]
        sliced = c[slice]
        assert shouldbe == sliced

        c[0] = True
        assert c[0] is True
        c[-2:] = "foo"
        assert c[-2:] == ["foo", "foo"]
        c[-2:] = ["foo", "bar"]
        assert c[-2:] == ["foo", "bar"]
        with pytest.raises(ValueError):
            c[:5] = [1, 2, 3]

        c = Collection(range(10))
        shouldbe = list(c)
        for i in slice:
            shouldbe[i] = True
        c[slice] = True
        assert c == shouldbe

        shouldbe = list(c)
        for i, v in zip(slice, "abc"):
            shouldbe[i] = v
        c[slice] = list("abc")
        assert c == shouldbe

    def test_each(self):
        c = Collection([{"index": i} for i in range(10)])

        assert c.each.combine() == c
        assert c.each["index"].combine() == list(range(10))
        assert c.each.get("index").combine() == list(range(10))
        assert c.each["index"].pipe(str).zfill(2).combine() == [
            str(i).zfill(2) for i in range(10)
        ]

    def test_cast_and_copy_attrs_to(self):
        c = Collection([])
        c_copy = c._cast_and_copy_attrs_to([])
        assert isinstance(c_copy, Collection)

        subc = SubCollection([], foo="bar")
        subc_copied = subc._cast_and_copy_attrs_to([])
        assert isinstance(subc_copied, SubCollection)
        assert subc_copied.foo == "bar"
        assert subc_copied._secret is True

    def test_str_to_predicate(self):
        nt = collections.namedtuple("FooBar", "foo bar")
        obj1 = nt(foo=True, bar=None)
        obj2 = nt(foo=None, bar=obj1)

        assert obj2.bar.foo is True

        pred = Collection._str_to_predicate("bar.foo")
        assert pred(obj2) is True

        pred = Collection._str_to_predicate("bar")
        assert pred(obj2) == obj1

        pred = Collection._str_to_predicate("bar.xyz")
        with pytest.raises(AttributeError):
            pred(obj2)

    def test_sorted(self):
        nt = collections.namedtuple("FooBar", "foo bar")
        orig = [nt(i, "baz") for i in range(10)]
        orig_rev = list(reversed(orig))
        c = Collection(orig)
        c.attr = -1

        with pytest.raises(TypeError):
            c.sorted()

        s = c.sorted(lambda x: x.foo)
        assert s.attr == -1
        assert s == orig

        s = c.sorted(lambda x: x.foo, reverse=True)
        assert s.attr == -1
        assert s == orig_rev

        s = c.sorted("foo", reverse=True)
        assert s == orig_rev

        s = c.sorted("foo", "bar", reverse=True)
        assert s == orig_rev

    def test_groupby(self):
        nt = collections.namedtuple("FooBar", "foo bar")
        c = Collection(nt(i, i % 2) for i in range(10))
        c.attr = "baz"

        with pytest.raises(TypeError):
            list(c.groupby())

        grouped = list(c.groupby(lambda x: x.bar))
        assert len(grouped) == 2
        for group, items in grouped:
            assert isinstance(items, Collection)
            assert list(items.each.bar) == [group] * len(items)
            assert items.attr == "baz"

        groups, items = zip(*c.groupby("bar", "foo"))
        assert groups == (
            (0, 0),
            (0, 2),
            (0, 4),
            (0, 6),
            (0, 8),
            (1, 1),
            (1, 3),
            (1, 5),
            (1, 7),
            (1, 9),
        )
