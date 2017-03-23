# flake8: noqa

__version__ = "0.1.0"
from .auth import Auth
descartes_auth = Auth()

from .services.metadata import Metadata
from .services.places import Places
from .services.raster import Raster

metadata = Metadata()
places = Places()
raster = Raster()
