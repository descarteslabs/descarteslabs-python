import pytest

import datetime
import json
import functools
from urllib.parse import urlencode, parse_qs

from ....common.graft import client as graft_client

from ... import types
from .. import tile_url


def test_url():
    base = "foo"
    base_q = base + "?"

    url = functools.partial(tile_url.tile_url, base, types.Image.from_id(""))

    assert url() == base
    assert url(session_id="foo") == base_q + urlencode({"session_id": "foo"})
    assert url(colormap="foo") == base_q + urlencode({"colormap": "foo"})
    assert url(colormap="") == base_q + urlencode({"colormap": ""})
    assert url(reduction="mean") == base_q + urlencode({"reduction": "mean"})
    assert url(checkerboard=True) == base_q + urlencode({"checkerboard": "true"})
    assert url(checkerboard=False) == base_q + urlencode({"checkerboard": "false"})

    assert url(bands=["red"]) == base_q + urlencode({"band": "red"})
    assert url(bands=["red", "green"]) == base_q + urlencode(
        {"band": ["red", "green"]}, doseq=True
    )
    with pytest.raises(ValueError, match="Up to 3 bands may be specified, not 4"):
        url(bands=["a", "b", "c", "d"])

    # 1-band scales are normalized
    assert url(scales=[0, 1]) == base_q + urlencode({"scales": "[[0.0, 1.0]]"})
    # If all none scales, not included
    assert url(scales=[None, None]) == base_q + urlencode({"scales": "null"})

    # test everything gets added together correctly
    got_base, params = url(
        session_id="foo", colormap="bar", bands=["red", "green"]
    ).split("?")
    assert got_base == base
    query = parse_qs(params, strict_parsing=True, keep_blank_values=True)
    assert query == {
        # `parse_qs` returns all values wrapped in lists
        "session_id": ["foo"],
        "colormap": ["bar"],
        "band": ["red", "green"],
    }


@pytest.mark.parametrize(
    "args",
    [
        {
            "p1": "2021-01-20",
            "p2": 2.2,
            "p3": 1,
        },
        {
            "p1": datetime.datetime(2020, 1, 20),
            "p2": types.Float(1.1) + 1,
            "p3": 1,
        },
        {
            "p1": types.Datetime(2021, 1, 20),
            "p2": types.Float(1.1) + 1,
            "p3": types.Int(1),
        },
    ],
)
def test_url_arguments(args):
    func = types.Function[
        dict(p1=types.Datetime, p2=types.Float, p3=types.Int), types.Image
    ]("x")
    base = "http://base.net"
    url = functools.partial(tile_url.tile_url, base, func)

    with pytest.raises(TypeError, match="missing a required argument"):
        url()
    with pytest.raises(TypeError, match="got an unexpected keyword argument 'blah'"):
        url(**args, blah="bad")

    with graft_client.consistent_guid():
        got_base, params = url(**args).split("?")

    assert got_base == base
    query = parse_qs(params, strict_parsing=True, keep_blank_values=True)
    assert query.keys() == args.keys()

    with graft_client.consistent_guid():
        p1_graft = types.Datetime._promote(args["p1"]).graft
    assert query["p1"] == [json.dumps(p1_graft)]

    if isinstance(args["p2"], float):
        assert query["p2"] == ["2.2"]
    else:
        assert query["p2"] == [json.dumps(args["p2"].graft)]

    assert query["p3"] == ["1"]


def test_no_url_for_positional_only_function():
    with pytest.raises(
        TypeError, match="cannot use Functions with positional-only arguments"
    ):
        tile_url.tile_url("", types.Function[types.Int, {}, types.Image]("x"))


def test_validate_scales():
    assert tile_url.validate_scales([[0.0, 1.0], [0.0, 2.0], [-1.0, 1.0]]) == [
        [0.0, 1.0],
        [0.0, 2.0],
        [-1.0, 1.0],
    ]
    assert tile_url.validate_scales([[0.0, 1.0]]) == [[0.0, 1.0]]
    # ints -> floats
    assert tile_url.validate_scales([[0, 1]]) == [[0.0, 1.0]]
    # 1-band convenience
    assert tile_url.validate_scales([0, 1]) == [[0.0, 1.0]]
    # no scalings
    assert tile_url.validate_scales(None) == []
    assert tile_url.validate_scales([]) == []

    with pytest.raises(TypeError, match="Expected a list or tuple of scales"):
        tile_url.validate_scales(0)
    with pytest.raises(TypeError, match="Expected a list or tuple of scales"):
        tile_url.validate_scales("foo")
    with pytest.raises(TypeError, match="Scaling 0: expected a 2-item list or tuple"):
        tile_url.validate_scales([1, 2, 3])
    with pytest.raises(TypeError, match="Scaling 0: items in scaling must be numbers"):
        tile_url.validate_scales([1, "foo"])
    with pytest.raises(ValueError, match="expected up to 3 scales, but got 4"):
        tile_url.validate_scales([[0.0, 1.0], [0.0, 1.0], [0.0, 1.0], [0.0, 1.0]])
    with pytest.raises(ValueError, match="but length was 3"):
        tile_url.validate_scales([[0.0, 1.0, 2.0]])
    with pytest.raises(ValueError, match="but length was 1"):
        tile_url.validate_scales([[0.0]])
    with pytest.raises(ValueError, match="one number and one None in scales"):
        tile_url.validate_scales([[None, 1.0]])
