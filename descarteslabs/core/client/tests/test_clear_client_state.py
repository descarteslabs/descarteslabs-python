import unittest

from .. import clear_client_state
from ...auth import Auth
from ..services.raster import Raster


class ClientStateTests(unittest.TestCase):
    def test_clear_client_state(self):
        clear_client_state()

        auth = Auth.get_default_auth()
        assert auth
        assert Auth._instance is auth

        raster = Raster.get_default_client()
        assert raster
        assert Raster._instance is raster

        clear_client_state()

        assert Auth._instance is None
        assert Raster._instance is None
