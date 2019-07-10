from descarteslabs.common.graft import client
from ...cereal import serializable
from ..core import typecheck_promote
from ..containers import Struct
from ..primitives import Bool, Float, Int, Number

TimedeltaStruct = Struct[{"days": Int, "seconds": Int, "microseconds": Int}]


@serializable(is_named_concrete_type=True)
class Timedelta(TimedeltaStruct):
    @typecheck_promote(
        weeks=(Int, Number),
        days=(Int, Number),
        hours=(Int, Number),
        minutes=(Int, Number),
        seconds=(Int, Number),
        microseconds=(Int, Number),
        milliseconds=(Int, Number),
    )
    def __init__(
        self,
        weeks=0,
        days=0,
        hours=0,
        minutes=0,
        seconds=0,
        microseconds=0,
        milliseconds=0,
    ):
        self.graft = client.apply_graft(
            "timedelta.from_components",
            weeks=weeks,
            days=days,
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            microseconds=microseconds,
            milliseconds=milliseconds,
        )

    @classmethod
    def _promote(cls, obj):
        try:
            return cls(seconds=obj.seconds, microseconds=obj.microseconds)
        except AttributeError:
            return super(cls, cls)._promote(obj)

    def __abs__(self):
        return self._from_apply("abs", self)

    def __add__(self, other):
        from .datetime_ import Datetime, _binary_op_casts_to

        @typecheck_promote(lambda: (Timedelta, Datetime))
        def add(other):
            return _binary_op_casts_to(self, other)._from_apply("add", self, other)

        return add(other)

    @typecheck_promote(lambda: Timedelta)
    def __eq__(self, other):
        return Bool._from_apply("eq", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __ge__(self, other):
        return Bool._from_apply("ge", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __gt__(self, other):
        return Bool._from_apply("gt", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __le__(self, other):
        return Bool._from_apply("le", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __lt__(self, other):
        return Bool._from_apply("lt", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __ne__(self, other):
        return Bool._from_apply("ne", self, other)

    def __neg__(self):
        return self._from_apply("neg", self)

    def __pos__(self):
        return self._from_apply("pos", self)

    def __radd__(self, other):
        from .datetime_ import Datetime, _binary_op_casts_to

        @typecheck_promote((Timedelta, Datetime))
        def radd(other):
            return _binary_op_casts_to(self, other)._from_apply("radd", self, other)

        return radd(other)

    @typecheck_promote(lambda: Timedelta)
    def __rsub__(self, other):
        return self._from_apply("rsub", self, other)

    @typecheck_promote(lambda: Timedelta)
    def __sub__(self, other):
        return self._from_apply("sub", self, other)

    def total_seconds(self):
        return Float._from_apply("timedelta.total_seconds", self)
