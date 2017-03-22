__version__ = "0.1.0"

from .services.metadata import Metadata
from .services.places import Places
from .services.raster import Raster


metadata = Metadata()
places = Places()
raster = Raster()
