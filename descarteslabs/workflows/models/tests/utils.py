import json
from ... import cereal, types
from descarteslabs.common.graft import client


def json_normalize(x):
    return json.loads(json.dumps(x))


@cereal.serializable()
class Foo(types.Proxytype):
    def __init__(self, x):
        self.graft = client.apply_graft("foo", x=x)


@cereal.serializable()
class Bar(types.Proxytype):
    def __init__(self, x):
        self.graft = client.apply_graft("bar", x=x)
