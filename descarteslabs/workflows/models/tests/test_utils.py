import datetime

from ..utils import pb_milliseconds_to_datetime, pb_datetime_to_milliseconds


def test_pb_milliseconds_to_datetime_helpers():
    assert pb_milliseconds_to_datetime(0) is None
    assert pb_datetime_to_milliseconds(pb_milliseconds_to_datetime(1)) == 1

    milliseconds = 123
    now = datetime.datetime.utcnow().replace(microsecond=milliseconds*1000)

    assert pb_milliseconds_to_datetime(pb_datetime_to_milliseconds(now)) == now
    assert pb_datetime_to_milliseconds(now) % 1000 == milliseconds
