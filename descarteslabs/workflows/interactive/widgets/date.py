from typing import Union
import datetime

from ._ipywidgets import ipywidgets

from ....common.proto.widgets import widgets_pb2
from ...types import Datetime
from ...types.widget import Widget


class Date(Widget, Datetime):
    _proto_type = widgets_pb2.Date

    def __init__(self, name: str, default: datetime.datetime, label=""):
        assert (
            default.tzinfo is not None
        ), f"default datetime must be timezone-aware: {default!r}"
        super().__init__(name, default, label)

        # ipywidgets `DatePicker` only picks dates, not timestamps, so both HH:MM:SS and
        # timezone information of `default` is lost.
        # We ensure `default` is in UTC first, since Workflows will ultimately interpret the
        # dates the widget produces as UTC (because they `.isoformat()` without timezone info,
        # and `wf.datetime.from_string` assumes any naive strings it receives are UTC).
        utc_default = default.astimezone(datetime.timezone.utc)
        self.widget = ipywidgets.DatePicker(value=utc_default)
        self.widget._label = label

    def _to_proto_set_widget_msg(self, widget_msg: widgets_pb2.Date):
        timestamp = self._default.timestamp()
        seconds = int(timestamp)
        widget_msg.default.seconds = seconds
        widget_msg.default.nanos = int((timestamp - seconds) * 10 ** 9)

    @classmethod
    def _from_proto_init_from_widget_msg(
        cls, name: str, label: str, widget_msg: widgets_pb2.Date
    ):
        timestamp = widget_msg.default.seconds + widget_msg.default.nanos / 10 ** 9
        return cls(
            name,
            datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc),
            label,
        )


def date(
    name: str,
    default: Union[str, datetime.date, datetime.datetime],
    label: str = "",
):
    """
    A date-picker widget, which acts as a `.Datetime` parameter.

    The value of the widget and the date displayed on it are always in UTC.

    Example
    -------
    >>> import descarteslabs.workflows as wf
    >>> wf.widgets.date("param_name", default="2020-11-03", label="The date")  # doctest: +SKIP

    >>> s2 = wf.ImageCollection.from_id(
    ...     "sentinel-2:L1C",
    ...     start_datetime=wf.widgets.date("start", default="2020-11-03"),
    ...     end_datetime=wf.widgets.date("end", default="2020-12-03"),
    ... ).pick_bands("red green blue")
    >>> s2.visualize("Sentinel-2", scales=[[0, 0.4], [0, 0.4], [0, 0.4]], reduction="median")  # doctest: +SKIP
    >>> # ^ when you call .visualize, the two `date` widgets will automatically show up below

    Selecting different dates will change the date range for the imagery.
    (If you haven't already, run ``wf.map`` in another notebook cell to see your layer.)

    Parameters
    ----------
    name: str
        The name of the parameter.
    default: Union[str, datetime.date, datetime.datetime]
        The default value for the widget, as an ISO-formatted timestamp string (like ``"2020-01-01"``),
        or a Python date or datetime object.

        If given as a string without a timezone offset, or a Python `datetime.date` object,
        it's assumed to be in UTC (consistent with `.Datetime.from_string`).

        Otherwise, if given as a Python `datetime.datetime` object without a timezone, it's assumed
        to be in system local time, and converted to UTC with ``default.astimezone(datetime.timezone.utc)``
    label: str, default ""
        The longform label to display next to the widget.
        If not given, the widget will display as ``name``.

    Returns
    -------
    widget: Date
        A Widget object that acts just like a Workflows `.Datetime`, and displays as a date picker.
    """
    if isinstance(default, str):
        default = datetime.datetime.fromisoformat(default)
        if default.tzinfo is None:
            # `wf.Datetime.from_string` treats naive timestamps as UTC,
            # so we do the same to strings here for interoperability
            default = default.replace(tzinfo=datetime.timezone.utc)

    elif isinstance(default, datetime.date):
        # NOTE: `datetime.datetime` is a subclass of `datetime.date`
        if isinstance(default, datetime.datetime):
            # if given as an actual Python datetime, assume naive datetimes
            # are in system time, since that appears to be the convention of the
            # `datetime` module.
            if default.tzinfo is None:
                default = default.astimezone(datetime.timezone.utc)
                # ^ "If called without arguments (or with tz=None) the system local timezone is assumed for
                # the target timezone. The .tzinfo attribute of the converted datetime instance will be set
                # to an instance of timezone with the zone name and offset obtained from the OS."
                # https://docs.python.org/3/library/datetime.html#datetime.datetime.astimezone
        else:
            # otherwise, `datetime.date`s become 00:00:00 UTC, since again, that's what
            # `wf.Datetime.from_string` will do to the TZ-less ISO-formatted string
            default = datetime.datetime(
                default.year, default.month, default.day, tzinfo=datetime.timezone.utc
            )
    else:
        raise TypeError(
            f"Default date must be a str, datetime.date, or datetime.datetime, not type {type(default)}: {default!r}"
        )

    return Date(name, default, label)
