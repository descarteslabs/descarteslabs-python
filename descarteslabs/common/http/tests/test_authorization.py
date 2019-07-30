import unittest

from .. import authorization


class TestAuthorization(unittest.TestCase):
    def test_add_bearer(self):
        self.assertEqual(authorization.add_bearer(u"foo"), u"Bearer foo")
        self.assertEqual(authorization.add_bearer("foo"), "Bearer foo")
        self.assertEqual(authorization.add_bearer(b"foo"), b"Bearer foo")

    def test_remove_bearer(self):
        self.assertEqual(authorization.remove_bearer(u"Bearer foo"), u"foo")
        self.assertEqual(authorization.remove_bearer("Bearer foo"), "foo")
        self.assertEqual(authorization.remove_bearer(b"Bearer foo"), b"foo")
