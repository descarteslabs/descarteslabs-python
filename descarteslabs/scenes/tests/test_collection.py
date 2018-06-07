import collections
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
        self.assertEqual(len(c), 0)

        c = Collection([0, 1, 2])
        self.assertEqual(len(c), 3)
        for i, x in enumerate(c):
            self.assertEqual(i, x)
        self.assertIn(1, c)
        self.assertEqual(c, c)
        # self.assertEqual(reversed(reversed(c)), c)

    def test_slicing_and_setting(self):
        c = Collection(range(10))
        self.assertEqual(c[4], 4)
        self.assertEqual(c[:2], [0, 1])
        self.assertEqual(c[:], list(range(10)))

        slice = [0, 3, 5]
        shouldbe = [c[i] for i in slice]
        sliced = c[slice]
        self.assertEqual(shouldbe, sliced)

        c[0] = True
        self.assertEqual(c[0], True)
        c[-2:] = "foo"
        self.assertEqual(c[-2:], ["foo", "foo"])
        c[-2:] = ["foo", "bar"]
        self.assertEqual(c[-2:], ["foo", "bar"])
        with self.assertRaises(ValueError):
            c[:5] = [1, 2, 3]

        c = Collection(range(10))
        shouldbe = list(c)
        for i in slice:
            shouldbe[i] = True
        c[slice] = True
        self.assertEqual(c, shouldbe)

        shouldbe = list(c)
        for i, v in zip(slice, "abc"):
            shouldbe[i] = v
        c[slice] = list("abc")
        self.assertEqual(c, shouldbe)

    def test_each(self):
        c = Collection([{"index": i} for i in range(10)])

        self.assertEqual(c.each.combine(), c)
        self.assertEqual(c.each["index"].combine(), list(range(10)))
        self.assertEqual(c.each.get("index").combine(), list(range(10)))
        self.assertEqual(c.each["index"].pipe(str).zfill(2).combine(), [str(i).zfill(2) for i in range(10)])

    def test_cast_and_copy_attrs_to(self):
        c = Collection([])
        c_copy = c._cast_and_copy_attrs_to([])
        self.assertIsInstance(c_copy, Collection)

        subc = SubCollection([], foo="bar")
        subc_copied = subc._cast_and_copy_attrs_to([])
        self.assertIsInstance(subc_copied, SubCollection)
        self.assertEqual(subc_copied.foo, "bar")
        self.assertEqual(subc_copied._secret, True)

    def test_str_to_predicate(self):
        nt = collections.namedtuple("FooBar", "foo bar")
        obj1 = nt(foo=True, bar=None)
        obj2 = nt(foo=None, bar=obj1)

        self.assertEqual(obj2.bar.foo, True)

        pred = Collection._str_to_predicate("bar.foo")
        self.assertEqual(pred(obj2), True)

        pred = Collection._str_to_predicate("bar")
        self.assertEqual(pred(obj2), obj1)

        pred = Collection._str_to_predicate("bar.xyz")
        with self.assertRaises(AttributeError):
            pred(obj2)

    def test_sorted(self):
        nt = collections.namedtuple("FooBar", "foo bar")
        orig = [nt(i, "baz") for i in range(10)]
        orig_rev = list(reversed(orig))
        c = Collection(orig)
        c.attr = -1

        with self.assertRaises(TypeError):
            c.sorted()

        s = c.sorted(lambda x: x.foo)
        self.assertEqual(s.attr, -1)
        self.assertEqual(s, orig)

        s = c.sorted(lambda x: x.foo, reverse=True)
        self.assertEqual(s.attr, -1)
        self.assertEqual(s, orig_rev)

        s = c.sorted("foo", reverse=True)
        self.assertEqual(s, orig_rev)

        s = c.sorted("foo", "bar", reverse=True)
        self.assertEqual(s, orig_rev)

    def test_groupby(self):
        nt = collections.namedtuple("FooBar", "foo bar")
        c = Collection(nt(i, i % 2) for i in range(10))
        c.attr = "baz"

        with self.assertRaises(TypeError):
            list(c.groupby())

        grouped = list(c.groupby(lambda x: x.bar))
        self.assertEqual(len(grouped), 2)
        for group, items in grouped:
            self.assertIsInstance(items, Collection)
            self.assertEqual(list(items.each.bar), [group] * len(items))
            self.assertEqual(items.attr, "baz")

        groups, items = zip(*c.groupby("bar", "foo"))
        self.assertEqual(groups, ((0, 0), (0, 2), (0, 4), (0, 6), (0, 8), (1, 1), (1, 3), (1, 5), (1, 7), (1, 9)))
