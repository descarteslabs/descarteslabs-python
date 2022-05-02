from ...cereal import serializable
from ..core import typecheck_promote
from .primitive import Primitive
from .bool_ import Bool
from .number import Int
from .none import NoneType


def ListType(param):
    # necessary to delay circular imports for typechecks
    from ..containers import List

    return List[param]


def SliceType():
    # necessary to delay circular imports for typechecks
    from ..containers import Slice

    return Slice


@serializable()
class Str(Primitive):
    """
    Proxy string.

    Supports most of the same API as Python's string type.

    Examples
    --------
    >>> from descarteslabs.workflows import Str
    >>> my_str = Str("hello")
    >>> my_str
    <descarteslabs.workflows.types.primitives.string.Str object at 0x...>
    >>> other_str = Str("world")
    >>> val = my_str + " " + other_str
    >>> val.compute() # doctest: +SKIP
    'hello world'
    >>> val.upper().compute() # doctest: +SKIP
    'HELLO WORLD'
    >>> val = val * 3
    >>> val.compute() # doctest: +SKIP
    'hello worldhello worldhello world'
    """

    _pytype = (str,)

    @typecheck_promote(lambda: Str)
    def __lt__(self, other):
        return Bool._from_apply("wf.lt", self, other)

    @typecheck_promote(lambda: Str)
    def __le__(self, other):
        return Bool._from_apply("wf.le", self, other)

    @typecheck_promote(lambda: Str)
    def __eq__(self, other):
        return Bool._from_apply("wf.eq", self, other)

    @typecheck_promote(lambda: Str)
    def __ne__(self, other):
        return Bool._from_apply("wf.ne", self, other)

    @typecheck_promote(lambda: Str)
    def __gt__(self, other):
        return Bool._from_apply("wf.gt", self, other)

    @typecheck_promote(lambda: Str)
    def __ge__(self, other):
        return Bool._from_apply("wf.ge", self, other)

    @typecheck_promote(lambda: Str)
    def __add__(self, other):
        return self._from_apply("wf.add", self, other)

    @typecheck_promote(lambda: Str)
    def __radd__(self, other):
        return self._from_apply("wf.add", other, self)

    @typecheck_promote(Int)
    def __mul__(self, other):
        return self._from_apply("wf.mul", self, other)

    @typecheck_promote(Int)
    def __rmul__(self, other):
        return self._from_apply("wf.mul", other, self)

    def __reversed__(self):
        return self._from_apply("wf.reversed", self)

    @typecheck_promote((Int, SliceType))
    def __getitem__(self, idx):
        return self._from_apply("wf.get", self, idx)

    def length(self):
        """The length of the string (returns `Int`)

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").length().compute() # doctest: +SKIP
        5
        """
        return Int._from_apply("wf.length", self)

    @typecheck_promote(lambda: Str)
    def contains(self, other):
        """Whether this string contains the given substring (returns `Bool`)

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").contains("o").compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.contains", self, other)

    def capitalize(self):
        """
        Return a capitalized version of the string.

        More specifically, make the first character have upper case and the rest lower
        case.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").capitalize().compute() # doctest: +SKIP
        'Hello'
        """
        return self._from_apply("wf.Str.capitalize", self)

    @typecheck_promote(Int, fillchar=lambda: Str)
    def center(self, width, fillchar=" "):
        """
        Return a centered string of length width.

        Padding is done using the specified fill character (default is a space).

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").center(9).compute() # doctest: +SKIP
        '  hello  '
        """
        return self._from_apply("wf.Str.center", self, width, fillchar=fillchar)

    @typecheck_promote(lambda: Str)
    def count(self, sub):
        """
        Return an `Int` of the number of non-overlapping occurrences of the substring sub in this string.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").count("l").compute() # doctest: +SKIP
        2
        """
        return Int._from_apply("wf.Str.count", self, sub)

    @typecheck_promote(lambda: Str)
    def endswith(self, suffix):
        """
        Return True if S ends with the specified suffix, False otherwise.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").endswith("o").compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.endswith", self, suffix)

    @typecheck_promote(tabsize=Int)
    def expandtabs(self, tabsize=8):
        """
        Return a copy where all tab characters are expanded using spaces.

        If tabsize is not given, a tab size of 8 characters is assumed.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello\t").expandtabs().compute() # doctest: +SKIP
        'hello   '
        """
        return self._from_apply("wf.Str.expandtabs", self, tabsize=tabsize)

    @typecheck_promote(lambda: Str)
    def find(self, sub):
        """
        Return the lowest index in S where substring sub is found in this string.

        Return -1 on failure.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").find("l").compute() # doctest: +SKIP
        2
        """
        return Int._from_apply("wf.Str.find", self, sub)

    def format(self, *args, **kwargs):
        """
        Return a formatted version of S, using substitutions from args and kwargs.
        The substitutions are identified by braces ('{' and '}').

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello {}").format("world").compute() # doctest: +SKIP
        'hello world'
        """
        return self._from_apply("wf.Str.format", self, *args, **kwargs)

    def isalnum(self):
        """
        Return True if the string is an alpha-numeric string, False otherwise.

        A string is alpha-numeric if all characters in the string are alpha-numeric and
        there is at least one character in the string.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").isalnum().compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.isalnum", self)

    def isalpha(self):
        """
        Return True if the string is an alphabetic string, False otherwise.

        A string is alphabetic if all characters in the string are alphabetic and there
        is at least one character in the string.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").isalpha().compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.isalpha", self)

    def isdigit(self):
        """
        Return True if the string is a digit string, False otherwise.

        A string is a digit string if all characters in the string are digits and there
        is at least one character in the string.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("3").isdigit().compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.isdigit", self)

    def islower(self):
        """
        Return True if the string is a lowercase string, False otherwise.

        A string is lowercase if all cased characters in the string are lowercase and
        there is at least one cased character in the string.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").islower().compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.islower", self)

    def isspace(self):
        """
        Return True if the string is a whitespace string, False otherwise.

        A string is whitespace if all characters in the string are whitespace and there
        is at least one character in the string.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str(" ").isspace().compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.isspace", self)

    def istitle(self):
        """
        Return True if the string is a title-cased string, False otherwise.

        In a title-cased string, upper- and title-case characters may only
        follow uncased characters and lowercase characters only cased ones.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("Hello World").istitle().compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.istitle", self)

    def isupper(self):
        """
        Return True if the string is an uppercase string, False otherwise.

        A string is uppercase if all cased characters in the string are uppercase and
        there is at least one cased character in the string.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("HELLO").isupper().compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.isupper", self)

    @typecheck_promote(lambda: ListType(Str))
    def join(self, strings):
        """
        Concatenate a ``List[Str]``.

        The string whose method is called is inserted in between each given string.
        The result is returned as a new string.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str(".").join(['ab', 'pq']).compute() # doctest: +SKIP
        'ab.pq'
        """
        return self._from_apply("wf.Str.join", self, strings)

    @typecheck_promote(Int, fillchar=lambda: Str)
    def ljust(self, width, fillchar=" "):
        """
        Return a left-justified string of length width.

        Padding is done using the specified fill character (default is a space).

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").ljust(6).compute() # doctest: +SKIP
        'hello '
        """
        return self._from_apply("wf.Str.ljust", self, width, fillchar=fillchar)

    def lower(self):
        """
        Return a copy of the string converted to lowercase.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("HELLO").lower().compute() # doctest: +SKIP
        'hello'
        """
        return self._from_apply("wf.Str.lower", self)

    @typecheck_promote(chars=lambda: (Str, NoneType))
    def lstrip(self, chars=None):
        """
        Return a copy of the string with leading whitespace removed.

        If chars is given and not None, remove characters in chars instead.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("  hello ").lstrip().compute() # doctest: +SKIP
        'hello '
        """
        return self._from_apply("wf.Str.lstrip", self, chars)

    @typecheck_promote(lambda: Str)
    def partition(self, sep):
        """
        Partition the string into three parts using the given separator.

        This will search for the separator in the string.  If the separator is found,
        returns a ``Tuple[Str, Str, Str]`` containing the part before the separator,
        the separator itself, and the part after it.

        If the separator is not found, returns a ``Tuple[Str, Str, Str]``
        containing the original string and two empty strings.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").partition("e").compute() # doctest: +SKIP
        ('h', 'e', 'llo')
        """
        from ..containers import Tuple

        return Tuple[Str, Str, Str]._from_apply("wf.Str.partition", self, sep)

    @typecheck_promote(lambda: Str, lambda: Str, count=Int)
    def replace(self, old, new, count=-1):
        """
        Return a copy with all occurrences of substring old replaced by new.
        If the optional argument count is given, only the first count occurrences are replaced.

        Parameters
        ----------
        old: Str
            Substring to replace
        new:
            Replacement substring
        count: Int
            Maximum number of occurrences to replace.
            -1 (the default value) means replace all occurrences.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").replace("e", "a").compute() # doctest: +SKIP
        'hallo'
        """
        return self._from_apply("wf.Str.replace", self, old, new, count=count)

    @typecheck_promote(lambda: Str)
    def rfind(self, sub):
        """
        Return the highest index in S where substring sub is found.

        Return -1 on failure.

        Parameters
        ----------
        sub: Str
            Substring to find

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").rfind("l").compute() # doctest: +SKIP
        3
        """
        return Int._from_apply("wf.Str.rfind", self, sub)

    @typecheck_promote(Int, fillchar=lambda: Str)
    def rjust(self, width, fillchar=" "):
        """
        Return a right-justified string of length width.

        Padding is done using the specified fill character (default is a space).

        Parameters
        ----------
        width: Int
            Total length of resulting padded string
        fillchar: Str
            Character to pad string with

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").rjust(9).compute() # doctest: +SKIP
        '    hello'
        """
        return self._from_apply("wf.Str.rjust", self, width, fillchar=fillchar)

    @typecheck_promote(lambda: Str)
    def rpartition(self, sep):
        """
        Partition the string into three parts using the given separator.

        This will search for the separator in the string, starting at the end. If
        the separator is found, returns a 3-tuple containing the part before the
        separator, the separator itself, and the part after it.

        If the separator is not found, returns a 3-tuple containing two empty strings
        and the original string.

        Parameters
        ----------
        sep: Str
            String to partition on

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").rpartition("l").compute() # doctest: +SKIP
        ('hel', 'l', 'o')
        """
        from ..containers import Tuple

        return Tuple[Str, Str, Str]._from_apply("wf.Str.rpartition", self, sep)

    @typecheck_promote(sep=lambda: (Str, NoneType), maxsplit=Int)
    def rsplit(self, sep=None, maxsplit=-1):
        """
        Return a list of the words in the string, using sep as the delimiter string.

        Parameters
        ----------
        sep: Str
            The delimiter according which to split the string.
            None (the default value) means split according to any whitespace,
            and discard empty strings from the result.
        maxsplit: Int
            Maximum number of splits to do. -1 (the default value) means no limit.

        Splits are done starting at the end of the string and working to the front.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").rsplit("l").compute() # doctest: +SKIP
        ['he', '', 'o']
        """
        from ..containers import List

        return List[Str]._from_apply("wf.Str.rsplit", self, sep=sep, maxsplit=maxsplit)

    @typecheck_promote(chars=lambda: (Str, NoneType))
    def rstrip(self, chars=None):
        """
        Return a copy of the string with trailing whitespace removed.

        If chars is given and not None, remove characters in chars instead.

        Parameters
        ----------
        chars: Str, optional
            Characters to remove

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str(" hello  ").rstrip().compute() # doctest: +SKIP
        ' hello'
        """
        return self._from_apply("wf.Str.rstrip", self, chars=chars)

    @typecheck_promote(sep=lambda: (Str, NoneType), maxsplit=Int)
    def split(self, sep=None, maxsplit=-1):
        """
        Return a ``List[Str]`` of the words in the string, using sep as the delimiter string.

        Parameters
        ----------
        sep: Str, optional
            The delimiter according which to split the string.
            None (the default value) means split according to any whitespace,
            and discard empty strings from the result.
        maxsplit: Int, optional
            Maximum number of splits to do. -1 (the default value) means no limit.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").split("e").compute() # doctest: +SKIP
        ['h', 'llo']
        """
        from ..containers import List

        return List[Str]._from_apply("wf.Str.split", self, sep=sep, maxsplit=maxsplit)

    def splitlines(self):
        """
        Return a ``List[Str]`` of the lines in the string, breaking at line boundaries.

        Line breaks are not included in the resulting strings.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello\\nworld").splitlines().compute() # doctest: +SKIP
        ['hello', 'world']
        """
        from ..containers import List

        return List[Str]._from_apply("wf.Str.splitlines", self)

    @typecheck_promote(lambda: Str)
    def startswith(self, prefix):
        """
        Return True if S starts with the specified prefix, False otherwise.

        Parameters
        ----------
        prefix: Str
            Prefix string

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").startswith("h").compute() # doctest: +SKIP
        True
        """
        return Bool._from_apply("wf.Str.startswith", self, prefix)

    @typecheck_promote(chars=lambda: (Str, NoneType))
    def strip(self, chars=None):
        """
        Return a copy of the string with leading and trailing whitespaces removed.

        If chars is given and not None, remove characters in chars instead.

        Parameters
        ----------
        chars: Str, optional
            Optional characters to remove

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("  hello  ").strip().compute() # doctest: +SKIP
        'hello'
        """
        return self._from_apply("wf.Str.strip", self, chars=chars)

    def swapcase(self):
        """
        Convert uppercase characters to lowercase and lowercase characters to uppercase.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").swapcase().compute() # doctest: +SKIP
        'HELLO'
        """
        return self._from_apply("wf.Str.swapcase", self)

    def title(self):
        """
        Return a version of the string where each word is titlecased.

        More specifically, words start with uppercased characters and all remaining
        cased characters have lower case.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello world").title().compute() # doctest: +SKIP
        'Hello World'
        """
        return self._from_apply("wf.Str.title", self)

    def upper(self):
        """
        Return a copy of the string converted to uppercase.

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").upper().compute() # doctest: +SKIP
        'HELLO'
        """
        return self._from_apply("wf.Str.upper", self)

    @typecheck_promote(Int)
    def zfill(self, width):
        """
        Pad a numeric string with zeros on the left, to fill a field of the given width.

        The string is never truncated.

        Parameters
        ----------
        width: Int
            Total length of resulting padded string

        Example
        -------
        >>> from descarteslabs.workflows import Str
        >>> Str("hello").zfill(9).compute() # doctest: +SKIP
        '0000hello'
        """
        return self._from_apply("wf.Str.zfill", self, width)
