import unittest

import ibis.expr.types

from descarteslabs.common.proto.ibis import ibis_pb2
from descarteslabs.common.ibis.client.api import compile


class ApiTestCase(unittest.TestCase):
    def test_compile(self):
        expr = ibis.literal("123")
        res = compile(expr)
        assert type(res) == ibis_pb2.Literal
