import six
from six.moves import reprlib
from itertools import islice


class DotDict(dict):
    """
    Subclass of dict, with "dot" (attribute) access to keys,
    a pretty-printed repr, which indents and truncates long lists,
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
        [{
            'foo': 'bar'
        }]
    >>> d.b[0].foo
        'bar'
    """
    def _repr_json_(self):
        return self, {'expanded': False}

    def __getattr__(self, attr):
        try:
            v = self[attr]
        except KeyError:
            try:
                return object.__getattribute__(self, attr)
            except AttributeError:
                six.raise_from(AttributeError(attr), None)

        if type(v) is dict:
            v = DotDict(v)
            self[attr] = v
            return v
        elif type(v) is list:
            v = DotList(v)
            self[attr] = v
            return v
        else:
            return v

    def __setattr__(self, attr, val):
        self[attr] = val

    def __delattr__(self, attr):
        try:
            del self[attr]
        except KeyError:
            six.raise_from(AttributeError(attr), None)

    def __dir__(self):
        return list(self.keys()) + dir(dict)

    def __repr__(self):
        return idr.repr(self)

    def __str__(self):
        return str(dict(self))


class DotList(list):
    """
    Returns contained dicts as DotDicts (and contained lists as DotLists),
    soley to allow attribute access past list indexing
    """
    def __getitem__(self, i):
        try:
            item = list.__getitem__(self, i)
        except IndexError:
            six.raise_from(IndexError("list index out of range"), None)

        if type(item) is dict:
            item = DotDict(item)
            self[i] = item
            return item
        if type(item) is list:
            item = DotList(item)
            self[i] = item
            return item
        else:
            return item


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
        self.maxlevel = 4
        self.maxlist = 4
        self.maxdict = 40

        self.indent = 2

    def repr_DotDict(self, x, level):
        return self.repr_dict(x, level)

    def repr_DotList(self, x, level):
        return self.repr_list(x, level)

    def repr_unicode(self, x, level):
        return self.repr_str(x, level)

    def repr_dict(self, x, level):
        n = len(x)
        depth = self.maxlevel - level
        if n == 0:
            return '{}'
        if level <= 0:
            return '{...}'
        newlevel = level - 1
        repr1 = self.repr1
        pieces = []
        for key in islice(_possibly_sorted(x), self.maxdict if depth > 0 else None):
            keyrepr = repr1(key, newlevel)
            valrepr = repr1(x[key], newlevel)
            pieces.append('%s: %s' % (keyrepr, valrepr))
        if self.maxdict and n > self.maxdict and depth > 0:
            pieces.append('...')

        outer_indent = ' ' * (self.indent * depth)
        inner_indent = outer_indent + ' ' * self.indent
        s = (',\n%s' % inner_indent).join(pieces)
        return '{\n%s%s\n%s}' % (inner_indent, s, outer_indent)

    def _repr_iterable(self, x, level, left, right, maxiter, trail=''):
        n = len(x)
        depth = self.maxlevel - level
        outer_indent = ' ' * (self.indent * depth)
        inner_indent = outer_indent + ' ' * self.indent
        if level <= 0 and n:
            s = '...'
        else:
            newlevel = level - 1
            repr1 = self.repr1
            pieces = [repr1(elem, newlevel) for elem in islice(x, maxiter if depth > 0 else None)]
            if n > maxiter and depth > 0:
                pieces.append('...')

            s = (',\n%s' % inner_indent).join(pieces)
            s = '\n%s%s\n%s' % (inner_indent, s, outer_indent)

            if n == 1 and trail:
                right = trail + right

        return '%s%s%s' % (left, s, right)


idr = IndentedRepr()
