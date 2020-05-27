import datetime

import pytest

from ..utils import pb_milliseconds_to_datetime, pb_datetime_to_milliseconds


def test_pb_milliseconds_to_datetime_helpers():
    assert pb_milliseconds_to_datetime(0) is None
    assert pb_datetime_to_milliseconds(pb_milliseconds_to_datetime(1e9)) == 1e9

    milliseconds = 123
    now = datetime.datetime.now(datetime.timezone.utc).replace(
        microsecond=milliseconds * 1000
    )

    assert pb_milliseconds_to_datetime(pb_datetime_to_milliseconds(now)) == now
    assert pb_datetime_to_milliseconds(now) % 1000 == milliseconds

    with pytest.raises(ValueError, match="must have tzinfo specified"):
        pb_datetime_to_milliseconds(datetime.datetime.now())
