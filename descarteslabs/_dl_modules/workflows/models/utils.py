import datetime
import logging

from ...common.proto.logging import logging_pb2


LOG_LEVEL_TO_PROTO_LOG_LEVEL = {
    logging.NOTSET: logging_pb2.LogRecord.Level.DEBUG,
    logging.DEBUG: logging_pb2.LogRecord.Level.DEBUG,
    logging.INFO: logging_pb2.LogRecord.Level.INFO,
    logging.WARNING: logging_pb2.LogRecord.Level.WARNING,
    logging.ERROR: logging_pb2.LogRecord.Level.ERROR,
    logging.CRITICAL: logging_pb2.LogRecord.Level.ERROR,
}


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


def pb_timestamp_to_datetime(ts):
    "datetme.datetime from a protobuf.Timestamp"
    if ts is None:
        return None
    return ts.ToDatetime()


def in_notebook():
    try:
        from IPython import get_ipython

        return "IPKernelApp" in get_ipython().config
    except Exception:
        return False


def py_log_level_to_proto_log_level(level):
    try:
        return LOG_LEVEL_TO_PROTO_LOG_LEVEL[level]
    except KeyError as e:
        valid_log_levels = ", ".join(LOG_LEVEL_TO_PROTO_LOG_LEVEL.keys())
        raise ValueError(
            f"Provided log level {e!s} not in set of valid log levels: "
            f"{valid_log_levels}. Please see "
            "https://docs.python.org/3/library/logging.html#logging-levels for the "
            "logging module constants that correspond to these valid log levels."
        )
