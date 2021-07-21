import reprlib
from itertools import islice


class DotDict(dict):
    """
    Subclass of dict, with "dot" (attribute) access to keys,
    a pretty-printed repr, which indents and truncates large containers,
    and a JSON repr for Jupyter Lab.

    Any dicts stored in DotDict are returned as DotDicts, to allow chained attribute access.
    Any lists stored in DotDict are returned as DotLists, which return any contained dict items as DotDicts,
    allowing chained attribute access past list indexing.

    The repr() of a DotDict is truncated for readability, but str() is not.

    Example
    -------

    >>> d = DotDict(a=1, b=[{"foo": "bar"}])
    >>> d.a
        1
    >>> d["a"]
        1
    >>> d.b
        [
          {
            'foo': 'bar'
          }
        ]
    >>> d.b[0].foo
        'bar'
    """

    __slots__ = ()  # no need for a namespace __dict__ when DotDict is a dict already

    def _repr_json_(self):
        return self, {"expanded": False}

    @classmethod
    def _box(cls, value):
        "If value is a dict or list, return it as a DotDict or DotList, otherwise return unmodified value."
        if type(value) is dict:
            return cls(value)
        elif type(value) is list:
            return DotList(value)
        else:
            return value

    def __getitem__(self, key):
        """
        x.__getitem__(y) <==> x[y]
        If x[y] is a dict or list, it is returned as a DotDict or DotList.
        """
        try:
            v = dict.__getitem__(self, key)
        except KeyError:
            raise KeyError(key) from None

        v = self._box(v)
        self[key] = v
        return v

    def __getattr__(self, attr):
        """
        self.attr <==> self[attr]
        If x[y] is a dict or list, it is returned as a DotDict or DotList.
        """
        try:
            return self[attr]
        except KeyError:
            try:
                return object.__getattribute__(self, attr)
            except AttributeError:
                raise AttributeError(attr) from None

    def __setattr__(self, attr, val):
        "self.attr = x <==> self[attr] = x"
        self[attr] = val

    def __delattr__(self, attr):
        "del self.attr <==> del self[attr]"
        try:
            del self[attr]
        except KeyError:
            raise AttributeError(attr) from None

    def __dir__(self):
        return list(self.keys()) + dir(dict)

    def __repr__(self):
        return idr.repr(self)

    def __str__(self):
        return untruncated_idr.repr(self)

    def items(self):
        """
        Equivalent to dict.items.
        Values that are plain dicts or lists are returned as DotDicts or DotLists.
        """
        return DotDict_items(self)

    def values(self):
        """
        Equivalent to dict.values.
        Values that are plain dicts or lists are returned as DotDicts or DotLists.
        """
        return DotDict_values(self)

    def get(self, key, default=None):
        """
        D.get(k[,d]) -> D[k] if k in D, else d.  d defaults to None.
        Values that are dicts or lists are cast to DotDicts and DotLists.
        """
        try:
            return self[key]
        except KeyError:
            return self._box(default)

    def pop(self, key, default=None):
        """
        D.pop(k[,d]) -> v, remove specified key and return the corresponding value.
        If key is not found, d is returned if given, otherwise KeyError is raised.
        If v is a dict or list, it is returned as a DotDict or DotList.
        """
        result = dict.pop(self, key, default)
        return self._box(result)

    def popitem(self):
        """
        D.popitem() -> (k, v), remove and return some (key, value) pair as a
        2-tuple; but raise KeyError if D is empty.
        If v is a dict or list, it is returned as a DotDict or DotList.
        """
        k, v = dict.popitem(self)
        return k, self._box(v)

    def setdefault(self, key, default=None):
        """
        D.setdefault(k[,d]) -> D.get(k,d), also set D[k]=d if k not in D
        If d is a dict or list, it is returned as a DotDict or DotList.
        """
        try:
            return self[key]
        except KeyError:
            default = self._box(default)
            self[key] = default
            return default

    def asdict(self):
        """
        D.asdict() -> a deep copy of D, where any DotDicts or DotLists contained are converted to plain types.
        Raises RuntimeError if the container is recursive (contains itself as a value).
        """
        # TODO: does not handle recursive structures

        # note: we're assuming here that any plain dict/list doesn't contain Dot-types
        # within it.  this is safe for normal usage of DotDict: any assignment to a
        # DotDict will cause all the levels in the hierarchy to be converted to
        # Dot-types However, if someone creates a plain dict, assigns DotDicts as its
        # values, then assigns *that* plain dict to a value in a DotDict, the asdict
        # of the containing DotDict will stop when it hits the plain dict.  The most
        # probable case here is assigning Dot-types as values in a dictionary or list
        # comprehension.

        unboxed = {}
        iterator = dict.items
        for k, v in iterator(self):
            if isinstance(v, DotDict):
                v = v.asdict()
            if isinstance(v, DotList):
                v = v.aslist()
            unboxed[k] = v
        return unboxed


class DotDict_view(object):
    """Wrapper around a dictionary view object that yields dicts and lists as DotDicts and DotLists when iterated."""

    __slots__ = ("_view",)

    def __init__(self, dotdict):
        self._view = dict.items(dotdict)
        self._dict = dotdict

    def __iter__(self):
        """Implement iter(self)."""
        for k, v in self._view:
            boxed = DotDict._box(v)
            if boxed is not v:
                self._dict[k] = boxed
            yield k, boxed

    def __len__(self):
        """Return len(self)."""
        return self._view.__len__()

    def __repr__(self):
        """Return repr(self)."""
        return "{}({})".format(self.__class__.__name__, list(self))


class DotDict_values(DotDict_view):
    """Wrapper around a dict_values object that yields dicts and lists as DotDicts and DotLists when iterated."""

    def __iter__(self):
        for k, v in super(DotDict_values, self).__iter__():
            yield v


class DotDict_items(DotDict_view):
    """Wrapper around a dict_values object that yields dicts and lists as DotDicts and DotLists when iterated."""

    def __and__(self, value):
        """Return self&value."""
        return self._view.__and__(value)

    def __contains__(self, key):
        """Return key in self."""
        return self._view.__contains__(key)

    def __eq__(self, value):
        """Return self==value."""
        return self._view.__eq__(value)

    def __ge__(self, value):
        """Return self>=value."""
        return self._view.__ge__(value)

    def __gt__(self, value):
        """Return self>value."""
        return self._view.__gt__(value)

    def __le__(self, value):
        """Return self<=value."""
        return self._view.__le__(value)

    def __lt__(self, value):
        """Return self<value."""
        return self._view.__lt__(value)

    def __ne__(self, value):
        """Return self!=value."""
        return self._view.__ne__(value)

    def __or__(self, value):
        """Return self|value."""
        return self._view.__or__(value)

    def __rand__(self, value):
        """Return value&self."""
        return self._view.__rand__(value)

    def __ror__(self, value):
        """Return value|self."""
        return self._view.__ror__(value)

    def __rsub__(self, value):
        """Return value-self."""
        return self._view.__rsub__(value)

    def __rxor__(self, value):
        """Return value^self."""
        return self._view.__rxor__(value)

    def __sub__(self, value):
        """Return self-value."""
        return self._view.__sub__(value)

    def __xor__(self, value):
        """Return self^value."""
        return self._view.__xor__(value)

    def isdisjoint(self, iterable):
        """Return True if the view and the given iterable have a null intersection."""
        return self._view.isdisjoint(iterable)


class DotList(list):
    """
    Returns contained dicts as DotDicts (and contained lists as DotLists),
    soley to allow attribute access past list indexing
    """

    __slots__ = ()

    def __getitem__(self, i):
        """
        x.__getitem__(y) <==> x[y]
        If x[y] is a dict or list, it is returned as a DotDict or DotList.
        """
        try:
            item = list.__getitem__(self, i)
        except IndexError:
            raise IndexError("list index out of range") from None

        item = DotDict._box(item)
        self[i] = item
        return item

    def __getslice__(self, i, j):
        return self.__getitem__(slice(i, j))

    def __iter__(self):
        for i in range(0, len(self)):
            yield self[i]

    def __repr__(self):
        return idr.repr(self)

    def __str__(self):
        return untruncated_idr.repr(self)

    def pop(self, i=-1):
        """
        L.pop([index]) -> item -- remove and return item at index (default last).
        If item is a dict or list, it is returned as a DotDict or DotList.
        Raises IndexError if list is empty or index is out of range.
        """
        result = list.pop(self, i)
        return DotDict._box(result)

    def aslist(self):
        """
        L.aslist() -> a deep copy of L, where any DotDicts or DotLists contained are converted to plain types.
        Raises RuntimeError if the container is recursive (contains itself as a value).
        """
        unboxed = list(self)
        for i, obj in enumerate(unboxed):
            if isinstance(obj, DotList):
                unboxed[i] = obj.aslist()
            elif isinstance(obj, DotDict):
                unboxed[i] = obj.asdict()
        return unboxed


def _possibly_sorted(x):
    # Since not all sequences of items can be sorted and comparison
    # functions may raise arbitrary exceptions, return an unsorted
    # sequence in that case.
    try:
        return sorted(x)
    except Exception:
        return list(x)


class IndentedRepr(reprlib.Repr, object):
    def __init__(self):
        super(IndentedRepr, self).__init__()

        self.maxstring = 90  # about the maximum width of a Jupyter Notebook
        self.maxlevel = 6
        self.maxlist = 4
        self.maxdict = None

        self.indent = 2

    def repr_DotDict(self, x, level):
        return self.repr_dict(x, level)

    def repr_DotList(self, x, level):
        return self.repr_list(x, level)

    def repr_unicode(self, x, level):
        return self.repr_str(x, level)

    def repr1(self, x, level):
        # repr1 is explicity defined rather than inherited,
        # because py2 and py3 have different implementations---py2 inlines repr_instance, basically
        typename = type(x).__name__
        if " " in typename:
            parts = typename.split()
            typename = "_".join(parts)
        if hasattr(self, "repr_" + typename):
            return getattr(self, "repr_" + typename)(x, level)
        else:
            return self.repr_instance(x, level)

    def repr_dict(self, x, level):
        n = len(x)
        if n == 0:
            return "{}"
        if self.maxlevel is not None:
            depth = self.maxlevel - level
            if level <= 0:
                return "{...}"
            newlevel = level - 1
        else:
            if level is None:
                level = 0
            depth = level
            newlevel = level + 1
        repr1 = self.repr1
        pieces = []
        for key in islice(
            _possibly_sorted(x),
            self.maxdict if self.maxdict is not None and depth > 0 else None,
        ):
            keyrepr = repr1(key, newlevel)
            valrepr = repr1(x[key], newlevel)
            pieces.append("%s: %s" % (keyrepr, valrepr))
        if self.maxdict is not None and n > self.maxdict and depth > 0:
            pieces.append("...")

        outer_indent = " " * (self.indent * depth)
        inner_indent = outer_indent + " " * self.indent
        s = (",\n%s" % inner_indent).join(pieces)
        return "{\n%s%s\n%s}" % (inner_indent, s, outer_indent)

    def _repr_iterable(self, x, level, left, right, maxiter, trail=""):
        n = len(x)
        if self.maxlevel is not None:
            depth = self.maxlevel - level
            newlevel = level - 1
        else:
            if level is None:
                level = 0
            depth = level
            newlevel = level + 1

        outer_indent = " " * (self.indent * depth)
        inner_indent = outer_indent + " " * self.indent

        if self.maxlevel is not None and level <= 0 and n:
            s = "..."
        else:
            repr1 = self.repr1
            pieces = [
                repr1(elem, newlevel)
                for elem in islice(
                    x, maxiter if maxiter is not None and depth > 0 else None
                )
            ]

            has_multiline_pieces = any("\n" in piece for piece in pieces)

            if maxiter is not None and n > maxiter and depth > 0:
                pieces.append("...")

            if has_multiline_pieces or maxiter is not None and n > maxiter:
                # multiline if long list, or components have line breaks (prevents weird closing bracket indentation)
                s = (",\n%s" % inner_indent).join(pieces)
                s = "\n%s%s\n%s" % (inner_indent, s, outer_indent)
            else:
                # single line if short
                s = ", ".join(pieces)

            if n == 1 and trail:
                right = trail + right

        return "%s%s%s" % (left, s, right)

    def repr_str(self, x, level):
        s = repr(x[: self.maxstring])
        if self.maxstring is not None:
            if len(s) > self.maxstring:
                i = max(0, (self.maxstring - 3) // 2)
                j = max(0, self.maxstring - 3 - i)
                s = repr(x[:i] + x[len(x) - j :])
                s = s[:i] + "..." + s[len(s) - j :]
        return s

    def repr_int(self, x, level):
        return self.repr_long(x, level)

    def repr_long(self, x, level):
        s = repr(x)  # XXX Hope this isn't too slow...
        if self.maxlong is not None and len(s) > self.maxlong:
            i = max(0, (self.maxlong - 3) // 2)
            j = max(0, self.maxlong - 3 - i)
            s = s[:i] + "..." + s[len(s) - j :]
        return s

    def repr_instance(self, x, level):
        try:
            s = repr(x)
            # Bugs in x.__repr__() can cause arbitrary
            # exceptions -- then make up something
        except Exception:
            return "<%s instance at %#x>" % (x.__class__.__name__, id(x))
        if self.maxother is not None and len(s) > self.maxother:
            i = max(0, (self.maxother - 3) // 2)
            j = max(0, self.maxother - 3 - i)
            s = s[:i] + "..." + s[len(s) - j :]
        return s


idr = IndentedRepr()
untruncated_idr = IndentedRepr()
untruncated_idr.maxlevel = None
untruncated_idr.maxdict = None
untruncated_idr.maxlist = None
untruncated_idr.maxtuple = None
untruncated_idr.maxset = None
untruncated_idr.maxfrozenset = None
untruncated_idr.maxdeque = None
untruncated_idr.maxarray = None
untruncated_idr.maxlong = None
untruncated_idr.maxstring = None
untruncated_idr.maxother = None
