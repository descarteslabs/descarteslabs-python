import mock

import json
from ....common.graft import client as graft_client
from ... import cereal, types, client


class MockedClient(client.Client):
    def _open_channel(self):
        return mock.MagicMock()


def json_normalize(x):
    return json.loads(json.dumps(x))


def assert_graft_is_scope_isolated_equvalent(isolated, orig):
    # TODO(gabe): replace with tokenization once that exists;
    # this is a workaround for GUIDs being, well, GUIDs.
    assert len(isolated) == 3
    orig_key = isolated[isolated["returns"]][0]
    assert json_normalize(isolated[orig_key]) == json_normalize(orig)


@cereal.serializable()
class Foo(types.Proxytype):
    def __init__(self, x):
        self.graft = graft_client.apply_graft("foo", x=x)
        self.params = getattr(x, "params", ())


@cereal.serializable()
class Bar(types.Proxytype):
    def __init__(self, x):
        self.graft = graft_client.apply_graft("bar", x=x)
        self.params = getattr(x, "params", ())
