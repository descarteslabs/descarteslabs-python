from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import typecheck_promote
from ..containers import Struct
from ..primitives import Bool, Float, Int, Number, Any

TimedeltaStruct = Struct[{"days": Int, "seconds": Int, "microseconds": Int}]


@serializable(is_named_concrete_type=True)
class Timedelta(TimedeltaStruct):
    """Proxy Timedelta object, similar to Python's timedelta.

    Examples
    --------
    >>> from descarteslabs.workflows import Timedelta
    >>> my_timedelta = Timedelta(days=10, minutes=100)
    >>> my_timedelta
    <descarteslabs.workflows.types.datetimes.timedelta.Timedelta object at 0x...>
    >>> my_timedelta.compute() # doctest: +SKIP
    datetime.timedelta(days=10, seconds=6000)
    >>> my_timedelta.total_seconds().compute() # doctest: +SKIP
    870000.0
    """

    _doc = {
        "days": "-999999999 <= days <= 999999999",
        "seconds": "0 <= seconds < 3600*24 (the number of seconds in one day)",
        "microseconds": "0 <= microseconds < 1000000",
    }

    @typecheck_promote(
        days=(Int, Number),
        seconds=(Int, Number),
        microseconds=(Int, Number),
        milliseconds=(Int, Number),
        minutes=(Int, Number),
        hours=(Int, Number),
        weeks=(Int, Number),
    )
    def __init__(
        self,
        days=0,
        seconds=0,
        microseconds=0,
        milliseconds=0,
        minutes=0,
        hours=0,
        weeks=0,
    ):
        self.graft = client.apply_graft(
            "wf.timedelta.from_components",
            days=days,
            seconds=seconds,
            microseconds=microseconds,
            milliseconds=milliseconds,
            minutes=minutes,
            hours=hours,
            weeks=weeks,
        )

    @classmethod
    def _promote(cls, obj):
        if isinstance(obj, cls):
            return obj
        if isinstance(obj, Any):
            return obj.cast(cls)
        try:
            return cls(
                days=obj.days, seconds=obj.seconds, microseconds=obj.microseconds
            )
        except AttributeError:
            return super(cls, cls)._promote(obj)

    def __abs__(self):
        return self._from_apply("wf.abs", self)

    def __add__(self, other):
        from .datetime_ import Datetime, _binary_op_casts_to

        @typecheck_promote(lambda: (Timedelta, Datetime))
        def add(other):
            return _binary_op_casts_to(self, other)._from_apply("wf.add", self, other)

        return add(other)

    @typecheck_promote(lambda: Timedelta)
    def __eq__(self, other):
        return Bool._from_apply("wf.eq", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __ge__(self, other):
        return Bool._from_apply("wf.ge", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __gt__(self, other):
        return Bool._from_apply("wf.gt", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __le__(self, other):
        return Bool._from_apply("wf.le", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __lt__(self, other):
        return Bool._from_apply("wf.lt", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __ne__(self, other):
        return Bool._from_apply("wf.ne", self, other)

    def __neg__(self):
        return self._from_apply("wf.neg", self)

    def __pos__(self):
        return self._from_apply("wf.pos", self)

    def __radd__(self, other):
        from .datetime_ import Datetime, _binary_op_casts_to

        @typecheck_promote((Timedelta, Datetime))
        def radd(other):
            return _binary_op_casts_to(self, other)._from_apply("wf.add", other, self)

        return radd(other)

    @typecheck_promote(lambda: Timedelta)
    def __rsub__(self, other):
        return self._from_apply("wf.sub", other, self)

    @typecheck_promote(lambda: Timedelta)
    def __sub__(self, other):
        return self._from_apply("wf.sub", self, other)

    def total_seconds(self):
        """The total number of seconds contained in the duration.

        Example
        -------
        >>> from descarteslabs.workflows import Timedelta
        >>> my_timedelta = Timedelta(minutes=30)
        >>> my_timedelta.total_seconds().compute() # doctest: +SKIP
        1800.0
        """
        return Float._from_apply("wf.timedelta.total_seconds", self)
