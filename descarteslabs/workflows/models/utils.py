import datetime


def pb_milliseconds_to_datetime(ms):
    if ms is None or ms == 0:
        return None
    return datetime.datetime.utcfromtimestamp(ms / 1e3)


def pb_datetime_to_milliseconds(dt):
    try:
        # py3
        return int(dt.timestamp() * 1000)
    except AttributeError:
        return int((dt - datetime.datetime.utcfromtimestamp(0)).total_seconds() * 1000)


def in_notebook():
    try:
        from IPython import get_ipython

        return "IPKernelApp" in get_ipython().config
    except Exception:
        return False
