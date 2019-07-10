import datetime


def pb_milliseconds_to_datetime(ms):
    if ms is None or ms == 0:
        return None
    return datetime.datetime.utcfromtimestamp(ms / 1e3)


def in_notebook():
    try:
        from IPython import get_ipython

        return "IPKernelApp" in get_ipython().config
    except Exception:
        return False
