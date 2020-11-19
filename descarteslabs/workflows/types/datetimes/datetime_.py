import numbers
import six

from ...cereal import serializable
from ..core import typecheck_promote
from ..primitives import Int, Float, Str, Bool, Any
from ..containers import Struct, Tuple
from .timedelta import Timedelta


def _binary_op_casts_to(a, b):
    if isinstance(a, Datetime) and isinstance(b, Timedelta):
        return Datetime
    elif isinstance(a, Datetime) and isinstance(b, Datetime):
        return Timedelta
    elif isinstance(a, Timedelta) and isinstance(b, Datetime):
        return Datetime
    else:  # pragma: no cover
        return type(a)


DatetimeStruct = Struct[
    {
        "year": Int,
        "month": Int,
        "day": Int,
        "hour": Int,
        "minute": Int,
        "second": Int,
        "microsecond": Int,
    }
]


@serializable(is_named_concrete_type=True)
class Datetime(DatetimeStruct):
    """
    Proxy Datetime object, similar to Python's datetime.

    Note: Datetimes are always in UTC.

    Examples
    --------
    >>> from descarteslabs.workflows import Datetime
    >>> my_datetime = Datetime(year=2019, month=1, day=1)
    >>> my_datetime
    <descarteslabs.workflows.types.datetimes.datetime_.Datetime object at 0x...>
    >>> my_datetime.compute() # doctest: +SKIP
    datetime.datetime(2019, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
    >>> my_datetime.year.compute() # doctest: +SKIP
    2019
    """

    _doc = {
        "year": "1 <= year <= 9999",
        "month": "1 <= month <= 12",
        "day": "1 <= day <= number of days in the given month and year",
        "hour": "0 <= hour < 24",
        "minute": "0 <= minute < 60",
        "second": "0 <= second < 60",
        "microsecond": "0 <= microsecond < 1000000",
    }
    _constructor = "wf.datetime.from_components"

    def __init__(self, year, month=1, day=1, hour=0, minute=0, second=0, microsecond=0):
        "Construct a `Datetime` from components. All parts are optional besides ``year``."
        super(Datetime, self).__init__(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            second=second,
            microsecond=microsecond,
        )

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Any):
            return obj.cast(cls)
        try:
            return cls.from_string(obj.isoformat())
        except AttributeError:
            if isinstance(obj, six.string_types):
                return cls.from_string(obj)

            if isinstance(obj, numbers.Number):
                return cls.from_timestamp(obj)

            return super(cls, cls)._promote(obj)

    @classmethod
    @typecheck_promote((Int, Float))
    def from_timestamp(cls, seconds):
        """
        Construct a Workflows Datetime from a number of seconds since the Unix epoch (January 1, 1970, 00:00:00 (UTC)).

        Parameters
        ----------
        seconds: Int or Float

        Returns
        -------
        ~descarteslabs.datetimes.Datetime

        Example
        -------
        >>> from descarteslabs.workflows import Datetime
        >>> my_datetime = Datetime.from_timestamp(1000)
        >>> my_datetime.compute() # doctest: +SKIP
        datetime.datetime(1970, 1, 1, 0, 16, 40)
        """
        return cls._from_apply("wf.datetime.from_timestamp", seconds)

    @classmethod
    @typecheck_promote(Str)
    def from_string(cls, string):
        """
        Construct a Workflows Datetime from an ISO 8601-formatted string.

        If there's no timezone offset information in the string, it's assumed to be UTC.
        If there is, it's converted to UTC.

        Parameters
        ----------
        string: Str
            An ISO 8601-formatted datetime string, such as 2018-03-22 or 2020-03-22T16:37:00Z.

        Returns
        -------
        ~descarteslabs.datetimes.Datetime

        Example
        -------
        >>> from descarteslabs.workflows import Datetime
        >>> my_datetime = Datetime.from_string("2017-12-31")
        >>> my_datetime.compute() # doctest: +SKIP
        datetime.datetime(2017, 12, 31, 0, 0, tzinfo=datetime.timezone.utc)
        """
        return cls._from_apply("wf.datetime.from_string", string)

    @typecheck_promote(Timedelta)
    def __add__(self, other):
        return self._from_apply("wf.add", self, other)

    @typecheck_promote(Timedelta)
    def __radd__(self, other):
        return self._from_apply("wf.add", other, self)

    @typecheck_promote(lambda: (Timedelta, Datetime))
    def __sub__(self, other):
        return _binary_op_casts_to(self, other)._from_apply("wf.sub", self, other)

    @typecheck_promote(Timedelta)
    def __mod__(self, other):
        """Difference (`Timedelta`) between this `Datetime` and the nearest prior interval of the given `Timedelta`.

        Example
        -------
        >>> dt = Datetime(2016, 4, 15, 18, 16, 37, 684181)  # doctest: +SKIP
        >>> td = dt % Timedelta(seconds=60)  # doctest: +SKIP
        >>> td.compute()  # doctest: +SKIP
        datetime.timedelta(0, 37, 684181)
        """
        return Timedelta._from_apply("wf.mod", self, other)

    @typecheck_promote(Timedelta)
    def __floordiv__(self, other):
        """This `Datetime`, floored to the nearest prior interval of the given `Timedelta`.

        Example
        -------
        >>> dt = Datetime(2016, 4, 15, 18, 16, 37, 684181)  # doctest: +SKIP
        >>> dt_quotient = dt // Timedelta(seconds=60)  # doctest: +SKIP
        >>> dt_quotient.compute()  # doctest: +SKIP
        datetime.datetime(2016, 4, 15, 18, 16)
        """
        return self._from_apply("wf.floordiv", self, other)

    @typecheck_promote(Timedelta)
    def __divmod__(self, other):
        return Tuple[type(self), Timedelta]._from_apply("wf.divmod", self, other)

    @typecheck_promote(lambda: Datetime)
    def __eq__(self, other):
        return Bool._from_apply("wf.eq", self, other)

    @typecheck_promote(lambda: Datetime)
    def __ge__(self, other):
        return Bool._from_apply("wf.ge", self, other)

    @typecheck_promote(lambda: Datetime)
    def __gt__(self, other):
        return Bool._from_apply("wf.gt", self, other)

    @typecheck_promote(lambda: Datetime)
    def __le__(self, other):
        return Bool._from_apply("wf.le", self, other)

    @typecheck_promote(lambda: Datetime)
    def __lt__(self, other):
        return Bool._from_apply("wf.lt", self, other)

    @typecheck_promote(lambda: Datetime)
    def __ne__(self, other):
        return Bool._from_apply("wf.ne", self, other)

    @typecheck_promote(lambda: Datetime, lambda: Datetime, inclusive=Bool)
    def is_between(self, start, end, inclusive=True):
        """
        Whether the datetime is between these ``start`` and ``end`` dates.

        Parameters
        ----------
        start: Datetime
            Start date
        end: Datetime
            End date
        inclusive: Bool, optional, default True
            If True, equivalent to ``start <= self <= end``

            If False, equivalent to ``start < self < end``

        Returns
        -------
        Bool

        Example
        -------
        >>> import descarteslabs.workflows as wf
        >>> dt = wf.Datetime(2019, 6, 1)
        >>> dt.is_between("2019-01-01", "2020-01-01").compute()  # doctest: +SKIP
        True
        >>> dt.is_between("2019-06-01", "2020-07-01").compute()  # doctest: +SKIP
        True
        >>> dt.is_between("2019-06-01", "2020-07-01", inclusive=False).compute()  # doctest: +SKIP
        False
        """
        return Bool._from_apply(
            "wf.datetime.is_between", self, start, end, inclusive=inclusive
        )
