import datetime


def pb_milliseconds_to_datetime(ms):
    "datetime.datetime from the number of milliseconds, in UTC, since the UNIX epoch"
    if ms is None or ms == 0:
        return None
    return datetime.datetime.fromtimestamp(ms / 1e3, tz=datetime.timezone.utc)


def pb_datetime_to_milliseconds(dt):
    "milliseconds since the UNIX epoch for a tz-aware datetime.datetime"
    if dt.tzinfo is None:
        raise ValueError("datetime must have tzinfo specified: {!r}".format(dt))
    return int(dt.timestamp() * 1000)


def in_notebook():
    try:
        from IPython import get_ipython

        return "IPKernelApp" in get_ipython().config
    except Exception:
        return False
