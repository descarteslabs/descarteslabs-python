import datetime

from ..utils import pb_milliseconds_to_datetime


def test_pb_milliseconds_to_datetime():
    assert pb_milliseconds_to_datetime(0) is None
    assert pb_milliseconds_to_datetime(1) == datetime.datetime(
        1970, 1, 1, 0, 0, 0, 1000
    )

    now = datetime.datetime.utcnow()
    now_timestamp = (now - datetime.datetime(1970, 1, 1)).total_seconds()
    now_timestamp_ms = now_timestamp * 1e3
    assert pb_milliseconds_to_datetime(now_timestamp_ms) == now
