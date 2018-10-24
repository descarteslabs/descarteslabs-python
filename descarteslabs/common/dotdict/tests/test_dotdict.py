import unittest
import textwrap
import random
import string
import ast
import json
import six
from descarteslabs.common.dotdict import DotDict, DotList, DotDict_items, DotDict_values
from descarteslabs.common.dotdict.dotdict import IndentedRepr, idr, untruncated_idr


class TestDotDict(unittest.TestCase):

    def test_from_dict(self):
        template = {"a": 1, "b": 2, "c": [0, -1]}
        d = DotDict(template)
        self.assertEqual(d, template)
        self.assertIsInstance(d, DotDict)
        self.assertIsInstance(d, dict)

    def test_getitem_access(self):
        d = DotDict(alpha=1, beta=2)
        self.assertEqual(d["alpha"], 1)
        with self.assertRaises(KeyError):
            d["nonexistent"]

    def test_getattr_access(self):
        d = DotDict(alpha=1, beta=2)
        self.assertEqual(d.alpha, 1)
        with self.assertRaises((KeyError, AttributeError)):
            d["nonexistent"]

    def test_setattr(self):
        d = DotDict()
        d.key = "value"
        self.assertEqual(d.key, "value")
        self.assertEqual(d["key"], "value")

    def test_delattr(self):
        d = DotDict(delete=0)
        del d.delete
        self.assertNotIn("delete", d)
        with self.assertRaises(AttributeError):
            del d.delete
        pass

    def test_mutable(self):
        d = DotDict({
            "a": 1,
            "subdict": {
                "x": 0
            }
        })
        d.subdict.y = 4
        self.assertEqual(d["subdict"]["y"], 4)
        d.subdict.x = -1
        self.assertEqual(d["subdict"]["x"], -1)

    def test_dir(self):
        d = DotDict({"a": 1, "b": 2, "c": [0, -1]})
        _dir = dir(d)
        properDir = sorted(dir(dict) + list(d.keys()))
        self.assertEqual(properDir, _dir)

    def test_repr(self):
        d = DotDict(long=list(range(100)))
        # long lists should be truncated with "..."
        with self.assertRaises((SyntaxError, ValueError)):
            ast.literal_eval(repr(d))

        d = DotDict({i: i for i in range(100)})
        # a long top-level dict should not be truncated
        self.assertEqual(d, {i: i for i in range(100)})

        # short lists and dicts should not be truncated
        d = DotDict(short=list(range(2)), other_key=list(range(3)))
        self.assertEqual(d, ast.literal_eval(repr(d)))

    def test_str(self):
        d = DotDict({"a": 1, "b": 2, "c": [0, -1]})
        self.assertEqual(ast.literal_eval(str(d)), d)

    def test_untruncated_str(self):
        d = DotDict(long=[[list(range(100))]])
        _ = d.long[0][0][0]  # force list to be converted to DotList  # noqa: F841
        self.assertEqual(d, ast.literal_eval(str(d)))

    def test_str_none(self):
        d = DotDict({"none": None})
        self.assertEquals("{\n  'none': None\n}", str(d))

    def test_getattr_returns_dotdict(self):
        d = DotDict({
            "a": 1,
            "subdict": {
                "x": 0
            }
        })
        subdict = d.subdict
        self.assertIsInstance(subdict, DotDict)
        self.assertEqual(subdict.x, 0)

    def test_getitem_returns_dotdict(self):
        d = DotDict({
            "a": 1,
            "subdict": {
                "x": 0
            }
        })
        subdict = d["subdict"]
        self.assertIsInstance(subdict, DotDict)
        self.assertEqual(subdict.x, 0)

    def test_getattr_returns_dotlist(self):
        d = DotDict({
            "a": 1,
            "sublist": [{
                "x": 0
            }]
        })
        sublist = d.sublist
        self.assertIsInstance(sublist, DotList)
        self.assertEqual(sublist[0].x, 0)

    def test_getitem_returns_dotlist(self):
        d = DotDict({
            "a": 1,
            "sublist": [{
                "x": 0
            }]
        })
        sublist = d["sublist"]
        self.assertIsInstance(sublist, DotList)
        self.assertEqual(sublist[0].x, 0)

    def test_nested_lists(self):
        d = DotDict(x=[[{
            "sublist": [{
                "key": "value"
            }]
        }]])
        self.assertEqual(d.x[0][0]["sublist"][0].key, "value")

    def test_jsonable(self):
        d = DotDict(long=[[list(range(100))]])
        _ = d.long[0][0][0]  # force list to be converted to DotList  # noqa: F841
        from_json = json.loads(json.dumps(d))
        self.assertEqual(from_json, d)

    def test_six_iteritems(self):
        d = DotDict({
            "a": 1,
            "subdict": {
                "x": 0,
                "z": -1
            },
            "sublist": [{
                "y": "foo"
            }]
        })
        iterator = six.iteritems(d)
        self.assertNotIsInstance(iterator, list)
        for k, v in iterator:
            if isinstance(v, dict):
                self.assertIsInstance(v, DotDict)
                v.foo = "bar"
            if isinstance(v, list):
                self.assertIsInstance(v, DotList)
                v.append(None)
        self.assertEqual(d.subdict.foo, "bar")
        self.assertEqual(d.sublist[1], None)

    def test_items(self):
        d = DotDict({
            "a": 1,
            "subdict": {
                "x": 0,
                "z": -1
            },
            "sublist": [{
                "y": "foo"
            }]
        })
        items = d.items()
        if six.PY2:
            self.assertIsInstance(items, list)
        elif six.PY3:
            self.assertIsInstance(items, DotDict_items)
        for k, v in items:
            if isinstance(v, dict):
                self.assertIsInstance(v, DotDict)
                v.foo = "bar"
            if isinstance(v, list):
                self.assertIsInstance(v, DotList)
                v.append(None)
        self.assertEqual(d.subdict.foo, "bar")
        self.assertEqual(d.sublist[1], None)

    @unittest.skipIf(six.PY2, "Dict view objects only exist in py3")
    def test_DotDict_view(self):
        d1 = DotDict({
            "a": 0,
            "b": 1
        })
        d2 = DotDict({
            "a": 0,
            "c": 2
        })

        items1 = d1.items()
        items2 = d2.items()
        self.assertEqual(len(items1), 2)
        self.assertIn(('a', 0), items1)
        self.assertNotIn(('c', 2), items1)
        self.assertFalse(items1.isdisjoint(items2))
        self.assertTrue(items1.isdisjoint([]))

        self.assertEqual(items1 & items2, {('a', 0)})
        self.assertEqual(items1 | items2, {('a', 0), ('b', 1), ('c', 2)})
        self.assertEqual(items1 ^ items2, {('b', 1), ('c', 2)})
        self.assertEqual(items1 - items2, {('b', 1)})
        self.assertEqual({('b', 1)} - items1, set())
        self.assertEqual(items1, items1)
        self.assertNotEqual(items1, items2)

        with self.assertRaises(TypeError):
            hash(items1)

        with self.assertRaises(AttributeError):
            items1.foo()

    def test_six_itervalues(self):
        d = DotDict({
            "subdictA": {
                "x": 0
            },
            "subdictB": {
                "x": 1
            }
        })
        iterator = six.itervalues(d)
        self.assertNotIsInstance(iterator, list)
        for v in iterator:
            self.assertIsInstance(v.x, int)
            v.foo = "bar"
        self.assertEqual(d.subdictA.foo, "bar")
        self.assertEqual(d.subdictB.foo, "bar")

    def test_values(self):
        d = DotDict({
            "subdictA": {
                "x": 0
            },
            "subdictB": {
                "x": 1
            }
        })
        values = d.values()
        if six.PY2:
            self.assertIsInstance(values, list)
        elif six.PY3:
            self.assertIsInstance(values, DotDict_values)
        for v in values:
            self.assertIsInstance(v.x, int)
            v.foo = "bar"
        self.assertEqual(d.subdictA.foo, "bar")
        self.assertEqual(d.subdictB.foo, "bar")

    def test_get(self):
        d = DotDict({
            "subdict": {
                "x": 0
            }
        })
        subdict = d.get("subdict")
        self.assertEqual(subdict.x, 0)
        subdict.foo = "bar"
        self.assertEqual(d.subdict.foo, "bar")
        default = d.get("not_here", {"foo": 1})
        self.assertEqual(default.foo, 1)

    def test_pop(self):
        d = DotDict({
            "subdict": {
                "x": 0
            }
        })
        subdict = d.pop("subdict")
        self.assertEqual(subdict.x, 0)
        default = d.pop("subdict", {"foo": 1})
        self.assertEqual(default.foo, 1)

    def test_popitem(self):
        d = DotDict({
            "subdict": {
                "x": 0
            }
        })
        k, v = d.popitem()
        self.assertEqual(k, "subdict")
        self.assertEqual(v.x, 0)
        self.assertEqual(len(d), 0)

    def test_setdefault(self):
        d = DotDict({
            "subdict": {
                "x": 0
            }
        })
        default = d.setdefault("subdict", {})
        self.assertEqual(default.x, 0)
        default.foo = "bar"
        self.assertEqual(d.subdict.foo, "bar")
        missing = d.setdefault("missing", {"foo": 1})
        self.assertEqual(missing.foo, 1)
        self.assertEqual(d.missing.foo, 1)


class TestUnbox(unittest.TestCase):
    @classmethod
    def random_container(cls, height=6):
        make_dict = random.choice((True, False, None))
        if height == 0 or make_dict is None:
            return random.randint(0, 100)
        if make_dict:
            return DotDict({i: cls.random_container(height - 1) for i in range(random.randint(1, 5))})
        else:
            return DotList(cls.random_container(height - 1) for i in range(random.randint(1, 5)))

    @classmethod
    def is_unboxed(cls, obj):
        if not isinstance(obj, (dict, list)):
            return True
        if isinstance(obj, dict):
            return type(obj) is dict and all(cls.is_unboxed(x) for x in six.itervalues(obj))
        if isinstance(obj, list):
            return type(obj) is list and all(cls.is_unboxed(x) for x in obj)

    def test_basic(self):
        d = DotDict({
            "a": 1,
            "b": 2
        })

        unboxed = d.asdict()
        self.assertEqual(type(unboxed), dict)
        self.assertEqual(unboxed, d)

        obj = DotDict(a=d)
        unboxed = obj.asdict()
        self.assertEqual(type(unboxed["a"]), dict)

        obj = DotList(DotDict(i=i) for i in range(10))
        unboxed = obj.aslist()
        self.assertEqual(type(unboxed), list)
        self.assertTrue(all(type(x) is dict for x in unboxed))

        obj = DotDict({i: DotList(range(i)) for i in range(10)})
        unboxed = obj.asdict()
        self.assertTrue(all(type(x) is list for x in six.itervalues(unboxed)))

    def test_random_nested_container(self):
        obj = 0
        while isinstance(obj, int):
            obj = self.random_container()

        unboxed = obj.asdict() if isinstance(obj, DotDict) else obj.aslist()

        self.assertFalse(self.is_unboxed(obj))
        self.assertTrue(self.is_unboxed(unboxed))
        self.assertEqual(obj, unboxed)

    @unittest.expectedFailure
    def test_dottype_within_plain(self):
        # see note in DotDict.asdict
        # unsure if this is an important case to handle
        sub = {i: DotDict(val=i) for i in range(5)}
        obj = DotDict(sub=sub)

        unboxed = obj.asdict()
        self.assertTrue(self.is_unboxed(unboxed))

    @unittest.skip("results in RuntimeError since recursive structures are not handled")
    def test_recursive_container(self):
        d = DotDict({
            "a": 1,
            "b": 2
        })

        d.loop = d
        unboxed = d.asdict()
        self.assertEqual(type(unboxed["loop"]), dict)


class TestDotList(unittest.TestCase):
    def test_from_list(self):
        template = list(range(10))
        dotlist = DotList(template)
        self.assertEqual(dotlist, template)
        self.assertIsInstance(dotlist, DotList)
        self.assertIsInstance(dotlist, list)

    def test_slice(self):
        d = DotList([
            [1, 2],
            {'foo': 'bar'},
        ])
        sliced = d[0:2]
        self.assertIsInstance(sliced, DotList)
        self.assertEqual(2, len(sliced))

    def test_iterate(self):
        d = DotList([
            {'foo': 'bar'},
            {'foo': 'baz'},
        ])
        foos = [foo.foo for foo in d]
        self.assertEqual(['bar', 'baz'], foos)

    def test_pop(self):
        d = DotList([
            {'foo': 'bar'},
            {'foo': 'baz'},
        ])
        item = d.pop()
        self.assertEqual(item.foo, "baz")


class TestIndentedRepr(unittest.TestCase):
    def test_idr_short(self):
        self.assertEqual(idr.indent, 2, 'indented repr indent has changed, other tests will fail')
        obj = [
            {
                u"key": 1.01,
                "bool": False,
                (1, (2, 3)): {"a", "b", "c"}
            },
            [4, 5, 6]
        ]
        if six.PY2:  # repr of sets and unicode changed from py2 to py3
            expected = """\
            [
              {
                'bool': False,
                (1, (2, 3)): set(['a', 'b', 'c']),
                u'key': 1.01
              },
              [4, 5, 6]
            ]"""
        else:
            expected = """\
            [
              {
                'key': 1.01,
                'bool': False,
                (1, (2, 3)): {'a', 'b', 'c'}
              },
              [4, 5, 6]
            ]"""
        expected = textwrap.dedent(expected)
        self.assertEqual(idr.repr(obj), expected)

    def test_idr_truncates_str(self):
        idr = IndentedRepr()
        idr.maxstring = 8
        s = "abcdefghijkl"
        self.assertEqual(idr.repr(s), "'a...kl'")

    def test_idr_toplevel_untruncated(self):
        idr = IndentedRepr()
        idr.maxlist = 5
        obj = list(range(10))
        expected = """\
        [
          0,
          1,
          2,
          3,
          4,
          5,
          6,
          7,
          8,
          9
        ]"""
        expected = textwrap.dedent(expected)
        self.assertEqual(idr.repr(obj), expected)

        idr.maxdict = 5
        obj = {i: i for i in range(10)}

        expected = """\
        {
          0: 0,
          1: 1,
          2: 2,
          3: 3,
          4: 4,
          5: 5,
          6: 6,
          7: 7,
          8: 8,
          9: 9
        }"""
        expected = textwrap.dedent(expected)
        self.assertEqual(idr.repr(obj), expected)

    def test_idr_truncates_list(self):
        idr = IndentedRepr()
        idr.maxlist = 5
        obj = [list(range(6))]
        expected = """\
        [
          [
            0,
            1,
            2,
            3,
            4,
            ...
          ]
        ]"""
        expected = textwrap.dedent(expected)
        self.assertEqual(idr.repr(obj), expected)

    def test_idr_doesnt_truncate_dict(self):
        obj = [{i: i for i in range(10)}]
        expected = """\
        [
          {
            0: 0,
            1: 1,
            2: 2,
            3: 3,
            4: 4,
            5: 5,
            6: 6,
            7: 7,
            8: 8,
            9: 9
          }
        ]"""
        expected = textwrap.dedent(expected)
        self.assertEqual(idr.repr(obj), expected)

    def test_idr_truncates_level(self):
        idr = IndentedRepr()
        idr.maxlevel = 3
        obj = [[[[True]]]]
        expected = "[[[[...]]]]"
        self.assertEqual(idr.repr(obj), expected)

    def test_untruncated(self):
        long_idr = IndentedRepr()
        long_idr.maxlevel = 100
        long_idr.maxdict = 100
        long_idr.maxlist = 100
        long_idr.maxtuple = 100
        long_idr.maxset = 100
        long_idr.maxfrozenset = 100
        long_idr.maxdeque = 100
        long_idr.maxarray = 100
        long_idr.maxlong = 100
        long_idr.maxstring = 100
        long_idr.maxother = 100

        # dict
        obj = [{i: i for i in range(10)}]
        self.assertEqual(untruncated_idr.repr(obj), long_idr.repr(obj))
        # list
        obj = [list(range(50))]
        self.assertEqual(untruncated_idr.repr(obj), long_idr.repr(obj))
        # tuple
        obj = [tuple(range(50))]
        self.assertEqual(untruncated_idr.repr(obj), long_idr.repr(obj))
        # set
        obj = [frozenset(range(50))]
        self.assertEqual(untruncated_idr.repr(obj), long_idr.repr(obj))
        # long
        obj = int(''.join(str(x) for x in range(50)))
        self.assertEqual(untruncated_idr.repr(obj), long_idr.repr(obj))
        # string
        obj = ''.join(random.choice(string.ascii_letters) for i in range(50))
        self.assertEqual(untruncated_idr.repr(obj), long_idr.repr(obj))


if __name__ == '__main__':
    unittest.main()
