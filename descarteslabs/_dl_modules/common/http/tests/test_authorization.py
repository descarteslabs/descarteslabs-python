import unittest

from .. import authorization


class TestAuthorization(unittest.TestCase):
    def test_add_bearer(self):
        assert authorization.add_bearer(u"foo") == u"Bearer foo"
        assert authorization.add_bearer("foo") == "Bearer foo"
        assert authorization.add_bearer(b"foo") == b"Bearer foo"

    def test_remove_bearer(self):
        assert authorization.remove_bearer(u"Bearer foo") == u"foo"
        assert authorization.remove_bearer("Bearer foo") == "foo"
        assert authorization.remove_bearer(b"Bearer foo") == b"foo"
