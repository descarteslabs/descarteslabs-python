import six

from ...cereal import serializable
from ..core import typecheck_promote
from .primitive import Primitive
from .bool_ import Bool
from .number import Int
from .none import NoneType


def ListType(*params):
    # necessary to delay circular imports for typechecks
    def inner():
        from ..containers import List

        return List[params]

    return inner


@serializable()
class Str(Primitive):
    "Proxy str"
    _pytype = six.string_types

    @typecheck_promote(lambda: Str)
    def __lt__(self, other):
        return Bool._from_apply("lt", self, other)

    @typecheck_promote(lambda: Str)
    def __le__(self, other):
        return Bool._from_apply("le", self, other)

    @typecheck_promote(lambda: Str)
    def __eq__(self, other):
        return Bool._from_apply("eq", self, other)

    @typecheck_promote(lambda: Str)
    def __ne__(self, other):
        return Bool._from_apply("ne", self, other)

    @typecheck_promote(lambda: Str)
    def __gt__(self, other):
        return Bool._from_apply("gt", self, other)

    @typecheck_promote(lambda: Str)
    def __ge__(self, other):
        return Bool._from_apply("ge", self, other)

    @typecheck_promote(lambda: Str)
    def __add__(self, other):
        return self._from_apply("add", self, other)

    @typecheck_promote(Int)
    def __mul__(self, other):
        return self._from_apply("mul", self, other)

    def __reversed__(self):
        return self._from_apply("reversed", self)

    def length(self):
        return Int._from_apply("length", self)

    @typecheck_promote(lambda: Str)
    def contains(self, other):
        return Bool._from_apply("contains", self, other)

    def capitalize(self):
        return self._from_apply("Str.capitalize", self)

    @typecheck_promote(Int, fillchar=lambda: Str)
    def center(self, width, fillchar=" "):
        return self._from_apply("Str.center", self, width, fillchar=fillchar)

    @typecheck_promote(lambda: Str)
    def count(self, sub):
        return Int._from_apply("Str.count", self, sub)

    def decode(self):
        raise NotImplementedError()

    def encode(self):
        raise NotImplementedError()

    @typecheck_promote(lambda: Str)
    def endswith(self, suffix):
        return Bool._from_apply("Str.endswidth", self, suffix)

    @typecheck_promote(tabsize=Int)
    def expandtabs(self, tabsize=8):
        return self._from_apply("Str.expandtabs", self, tabsize=tabsize)

    @typecheck_promote(lambda: Str)
    def find(self, sub):
        return Int._from_apply("Str.find", self, sub)

    def format(self, *args, **kwargs):
        return self._from_apply("Str.format", self, *args, **kwargs)

    @typecheck_promote(lambda: Str)
    def index(self, sub):
        return Int._from_apply("Str.index", self, sub)

    def isalnum(self):
        return Bool._from_apply("Str.isalnum", self)

    def isalpha(self):
        return Bool._from_apply("Str.isalpha", self)

    def isdigit(self):
        return Bool._from_apply("Str.isdigit", self)

    def islower(self):
        return Bool._from_apply("Str.islower", self)

    def isspace(self):
        return Bool._from_apply("Str.isspace", self)

    def istitle(self):
        return Bool._from_apply("Str.istitle", self)

    def isupper(self):
        return Bool._from_apply("Str.isupper", self)

    def join(self, iterable):
        from ..containers import List

        @typecheck_promote(List[Str])
        def _join(iterable):
            return self._from_apply("Str.join", self, iterable)

        return _join(iterable)

    @typecheck_promote(Int, fillchar=lambda: Str)
    def ljust(self, width, fillchar=" "):
        return self._from_apply("Str.ljust", self, width, fillchar=fillchar)

    def lower(self):
        return self._from_apply("Str.lower", self)

    @typecheck_promote(chars=lambda: (Str, NoneType))
    def lstrip(self, chars=None):
        return self._from_apply("Str.lstrip", self, chars)

    @typecheck_promote(lambda: Str)
    def partition(self, sep):
        from ..containers import Tuple

        return Tuple[Str, Str, Str]._from_apply("Str.partition", self, sep)

    @typecheck_promote(lambda: Str, lambda: Str, count=Int)
    def replace(self, old, new, count=-1):
        return self._from_apply("Str.replace", self, old, new, count=count)

    @typecheck_promote(lambda: Str)
    def rfind(self, sub):
        return Int._from_apply("Str.rfind", self, sub)

    @typecheck_promote(lambda: Str)
    def rindex(self, sub):
        return Int._from_apply("Str.rindex", self, sub)

    @typecheck_promote(Int, fillchar=lambda: Str)
    def rjust(self, width, fillchar=" "):
        return self._from_apply("Str.rjust", self, width, fillchar=fillchar)

    @typecheck_promote(lambda: Str)
    def rpartition(self, sep):
        from ..containers import Tuple

        return Tuple[Str, Str, Str]._from_apply("Str.rpartition", self, sep)

    @typecheck_promote(sep=lambda: (Str, NoneType), maxsplit=Int)
    def rsplit(self, sep=None, maxsplit=-1):
        from ..containers import List

        return List[Str]._from_apply("Str.rsplit", self, sep=sep, maxsplit=maxsplit)

    @typecheck_promote(chars=lambda: (Str, NoneType))
    def rstrip(self, chars=None):
        return self._from_apply("Str.rstrip", self, chars=chars)

    @typecheck_promote(sep=lambda: (Str, NoneType), maxsplit=Int)
    def split(self, sep=None, maxsplit=-1):
        from ..containers import List

        return List[Str]._from_apply("Str.split", self, sep=sep, maxsplit=maxsplit)

    def splitlines(self):
        from ..containers import List

        return List[Str]._from_apply("Str.splitlines", self)

    @typecheck_promote(lambda: Str)
    def startswith(self, prefix):
        return Bool._from_apply("Str.startswith", self, prefix)

    @typecheck_promote(chars=lambda: (Str, NoneType))
    def strip(self, chars=None):
        return self._from_apply("Str.strip", self, chars=chars)

    def swapcase(self):
        return self._from_apply("Str.swapcase", self)

    def title(self):
        return self._from_apply("Str.title", self)

    @typecheck_promote(lambda: Str, deletechars=lambda: Str)
    def translate(self, table, deletechars=""):
        return self._from_apply("Str.translate", self, table, deletechars=deletechars)

    def upper(self):
        return self._from_apply("Str.upper", self)

    @typecheck_promote(Int)
    def zfill(self, width):
        return self._from_apply("Str.upper", self, width)
