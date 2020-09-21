import unittest
import pytest
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
        assert d == template
        assert isinstance(d, DotDict)
        assert isinstance(d, dict)

    def test_getitem_access(self):
        d = DotDict(alpha=1, beta=2)
        assert d["alpha"] == 1
        with pytest.raises(KeyError):
            d["nonexistent"]

    def test_getattr_access(self):
        d = DotDict(alpha=1, beta=2)
        assert d.alpha == 1
        with pytest.raises((KeyError, AttributeError)):
            d["nonexistent"]

    def test_setattr(self):
        d = DotDict()
        d.key = "value"
        assert d.key == "value"
        assert d["key"] == "value"

    def test_delattr(self):
        d = DotDict(delete=0)
        del d.delete
        assert "delete" not in d
        with pytest.raises(AttributeError):
            del d.delete
        pass

    def test_mutable(self):
        d = DotDict({"a": 1, "subdict": {"x": 0}})
        d.subdict.y = 4
        assert d["subdict"]["y"] == 4
        d.subdict.x = -1
        assert d["subdict"]["x"] == -1

    def test_dir(self):
        d = DotDict({"a": 1, "b": 2, "c": [0, -1]})
        _dir = dir(d)
        properDir = sorted(dir(dict) + list(d.keys()))
        assert properDir == _dir

    def test_repr(self):
        d = DotDict(long=list(range(100)))
        # long lists should be truncated with "..."
        assert "..." in repr(d)

        d = DotDict({i: i for i in range(100)})
        # a long top-level dict should not be truncated
        assert d == {i: i for i in range(100)}

        # short lists and dicts should not be truncated
        d = DotDict(short=list(range(2)), other_key=list(range(3)))
        assert d == ast.literal_eval(repr(d))

    def test_str(self):
        d = DotDict({"a": 1, "b": 2, "c": [0, -1]})
        assert ast.literal_eval(str(d)) == d

    def test_untruncated_str(self):
        d = DotDict(long=[[list(range(100))]])
        _ = d.long[0][0][0]  # force list to be converted to DotList  # noqa: F841
        assert d == ast.literal_eval(str(d))

    def test_str_none(self):
        d = DotDict({"none": None})
        assert "{\n  'none': None\n}" == str(d)

    def test_getattr_returns_dotdict(self):
        d = DotDict({"a": 1, "subdict": {"x": 0}})
        subdict = d.subdict
        assert isinstance(subdict, DotDict)
        assert subdict.x == 0

    def test_getitem_returns_dotdict(self):
        d = DotDict({"a": 1, "subdict": {"x": 0}})
        subdict = d["subdict"]
        assert isinstance(subdict, DotDict)
        assert subdict.x == 0

    def test_getattr_returns_dotlist(self):
        d = DotDict({"a": 1, "sublist": [{"x": 0}]})
        sublist = d.sublist
        assert isinstance(sublist, DotList)
        assert sublist[0].x == 0

    def test_getitem_returns_dotlist(self):
        d = DotDict({"a": 1, "sublist": [{"x": 0}]})
        sublist = d["sublist"]
        assert isinstance(sublist, DotList)
        assert sublist[0].x == 0

    def test_nested_lists(self):
        d = DotDict(x=[[{"sublist": [{"key": "value"}]}]])
        assert d.x[0][0]["sublist"][0].key == "value"

    def test_jsonable(self):
        d = DotDict(long=[[list(range(100))]])
        _ = d.long[0][0][0]  # force list to be converted to DotList  # noqa: F841
        from_json = json.loads(json.dumps(d))
        assert from_json == d

    def test_six_iteritems(self):
        d = DotDict({"a": 1, "subdict": {"x": 0, "z": -1}, "sublist": [{"y": "foo"}]})
        iterator = six.iteritems(d)
        assert not isinstance(iterator, list)
        for k, v in iterator:
            if isinstance(v, dict):
                assert isinstance(v, DotDict)
                v.foo = "bar"
            if isinstance(v, list):
                assert isinstance(v, DotList)
                v.append(None)
        assert d.subdict.foo == "bar"
        assert d.sublist[1] is None

    def test_items(self):
        d = DotDict({"a": 1, "subdict": {"x": 0, "z": -1}, "sublist": [{"y": "foo"}]})
        items = d.items()
        if six.PY2:
            assert isinstance(items, list)
        elif six.PY3:
            assert isinstance(items, DotDict_items)
        for k, v in items:
            if isinstance(v, dict):
                assert isinstance(v, DotDict)
                v.foo = "bar"
            if isinstance(v, list):
                assert isinstance(v, DotList)
                v.append(None)
        assert d.subdict.foo == "bar"
        assert d.sublist[1] is None

    @unittest.skipIf(six.PY2, "Dict view objects only exist in py3")
    def test_DotDict_view(self):
        d1 = DotDict({"a": 0, "b": 1})
        d2 = DotDict({"a": 0, "c": 2})

        items1 = d1.items()
        items2 = d2.items()
        assert len(items1) == 2
        assert ("a", 0) in items1
        assert ("c", 2) not in items1
        assert not items1.isdisjoint(items2)
        assert items1.isdisjoint([])

        assert items1 & items2 == {("a", 0)}
        assert items1 | items2 == {("a", 0), ("b", 1), ("c", 2)}
        assert items1 ^ items2 == {("b", 1), ("c", 2)}
        assert items1 - items2 == {("b", 1)}
        assert {("b", 1)} - items1 == set()
        assert items1 == items1
        assert items1 != items2

        with pytest.raises(TypeError):
            hash(items1)

        with pytest.raises(AttributeError):
            items1.foo()

    def test_six_itervalues(self):
        d = DotDict({"subdictA": {"x": 0}, "subdictB": {"x": 1}})
        iterator = six.itervalues(d)
        assert not isinstance(iterator, list)
        for v in iterator:
            assert isinstance(v.x, int)
            v.foo = "bar"
        assert d.subdictA.foo == "bar"
        assert d.subdictB.foo == "bar"

    def test_values(self):
        d = DotDict({"subdictA": {"x": 0}, "subdictB": {"x": 1}})
        values = d.values()
        if six.PY2:
            assert isinstance(values, list)
        elif six.PY3:
            assert isinstance(values, DotDict_values)
        for v in values:
            assert isinstance(v.x, int)
            v.foo = "bar"
        assert d.subdictA.foo == "bar"
        assert d.subdictB.foo == "bar"

    def test_get(self):
        d = DotDict({"subdict": {"x": 0}})
        subdict = d.get("subdict")
        assert subdict.x == 0
        subdict.foo = "bar"
        assert d.subdict.foo == "bar"
        default = d.get("not_here", {"foo": 1})
        assert default.foo == 1

    def test_pop(self):
        d = DotDict({"subdict": {"x": 0}})
        subdict = d.pop("subdict")
        assert subdict.x == 0
        default = d.pop("subdict", {"foo": 1})
        assert default.foo == 1

    def test_popitem(self):
        d = DotDict({"subdict": {"x": 0}})
        k, v = d.popitem()
        assert k == "subdict"
        assert v.x == 0
        assert len(d) == 0

    def test_setdefault(self):
        d = DotDict({"subdict": {"x": 0}})
        default = d.setdefault("subdict", {})
        assert default.x == 0
        default.foo = "bar"
        assert d.subdict.foo == "bar"
        missing = d.setdefault("missing", {"foo": 1})
        assert missing.foo == 1
        assert d.missing.foo == 1


class TestUnbox(unittest.TestCase):
    @classmethod
    def random_container(cls, height=6):
        make_dict = random.choice((True, False, None))
        if height == 0 or make_dict is None:
            return random.randint(0, 100)
        if make_dict:
            return DotDict(
                {
                    i: cls.random_container(height - 1)
                    for i in range(random.randint(1, 5))
                }
            )
        else:
            return DotList(
                cls.random_container(height - 1) for i in range(random.randint(1, 5))
            )

    @classmethod
    def is_unboxed(cls, obj):
        if not isinstance(obj, (dict, list)):
            return True
        if isinstance(obj, dict):
            return type(obj) is dict and all(
                cls.is_unboxed(x) for x in six.itervalues(obj)
            )
        if isinstance(obj, list):
            return type(obj) is list and all(cls.is_unboxed(x) for x in obj)

    def test_basic(self):
        d = DotDict({"a": 1, "b": 2})

        unboxed = d.asdict()
        assert type(unboxed) == dict
        assert unboxed == d

        obj = DotDict(a=d)
        unboxed = obj.asdict()
        assert type(unboxed["a"]) == dict

        obj = DotList(DotDict(i=i) for i in range(10))
        unboxed = obj.aslist()
        assert type(unboxed) == list
        assert all(type(x) is dict for x in unboxed)

        obj = DotDict({i: DotList(range(i)) for i in range(10)})
        unboxed = obj.asdict()
        assert all(type(x) is list for x in six.itervalues(unboxed))

    def test_random_nested_container(self):
        obj = 0
        while isinstance(obj, int):
            obj = self.random_container()

        unboxed = obj.asdict() if isinstance(obj, DotDict) else obj.aslist()

        assert not self.is_unboxed(obj)
        assert self.is_unboxed(unboxed)
        assert obj == unboxed

    @unittest.expectedFailure
    def test_dottype_within_plain(self):
        # see note in DotDict.asdict
        # unsure if this is an important case to handle
        sub = {i: DotDict(val=i) for i in range(5)}
        obj = DotDict(sub=sub)

        unboxed = obj.asdict()
        assert self.is_unboxed(unboxed)

    @unittest.skip("results in RuntimeError since recursive structures are not handled")
    def test_recursive_container(self):
        d = DotDict({"a": 1, "b": 2})

        d.loop = d
        unboxed = d.asdict()
        assert type(unboxed["loop"]) == dict


class TestDotList(unittest.TestCase):
    def test_from_list(self):
        template = list(range(10))
        dotlist = DotList(template)
        assert dotlist == template
        assert isinstance(dotlist, DotList)
        assert isinstance(dotlist, list)

    def test_slice(self):
        d = DotList([[1, 2], {"foo": "bar"}])
        sliced = d[0:2]
        assert isinstance(sliced, DotList)
        assert 2 == len(sliced)

    def test_iterate(self):
        d = DotList([{"foo": "bar"}, {"foo": "baz"}])
        foos = [foo.foo for foo in d]
        assert ["bar", "baz"] == foos

    def test_pop(self):
        d = DotList([{"foo": "bar"}, {"foo": "baz"}])
        item = d.pop()
        assert item.foo == "baz"


class TestIndentedRepr(unittest.TestCase):
    def test_idr_short(self):
        assert (
            idr.indent == 2
        ), "indented repr indent has changed, other tests will fail"
        obj = [{u"key": 1.01, "bool": False, (1, (2, 3)): {"a", "b", "c"}}, [4, 5, 6]]
        unicode_prefix = ""
        set_start = "{"
        set_end = "}"

        if six.PY2:  # repr of sets and unicode changed from py2 to py3
            unicode_prefix = "u"
            set_start = "set(["
            set_end = "])"

        output = idr.repr(obj)

        # The order is not guaranteed.  Look for individual lines.
        # Since the comma depends on the position, skip those.
        assert "[\n" in output
        assert "  {\n" in output
        assert "\n    {}'key': 1.01".format(unicode_prefix) in output
        assert "\n    'bool': False" in output
        assert (
            "\n    (1, (2, 3)): {}'a', 'b', 'c'{}".format(set_start, set_end) in output
        )
        assert "\n  }" in output
        assert "\n  [4, 5, 6]" in output
        assert "\n]" in output

    def test_idr_truncates_str(self):
        idr = IndentedRepr()
        idr.maxstring = 8
        s = "abcdefghijkl"
        assert idr.repr(s) == "'a...kl'"

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
        assert idr.repr(obj) == expected

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
        assert idr.repr(obj) == expected

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
        assert idr.repr(obj) == expected

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
        assert idr.repr(obj) == expected

    def test_idr_truncates_level(self):
        idr = IndentedRepr()
        idr.maxlevel = 3
        obj = [[[[True]]]]
        expected = "[[[[...]]]]"
        assert idr.repr(obj) == expected

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
        assert untruncated_idr.repr(obj) == long_idr.repr(obj)
        # list
        obj = [list(range(50))]
        assert untruncated_idr.repr(obj) == long_idr.repr(obj)
        # tuple
        obj = [tuple(range(50))]
        assert untruncated_idr.repr(obj) == long_idr.repr(obj)
        # set
        obj = [frozenset(range(50))]
        assert untruncated_idr.repr(obj) == long_idr.repr(obj)
        # long
        obj = int("".join(str(x) for x in range(50)))
        assert untruncated_idr.repr(obj) == long_idr.repr(obj)
        # string
        obj = "".join(random.choice(string.ascii_letters) for i in range(50))
        assert untruncated_idr.repr(obj) == long_idr.repr(obj)
