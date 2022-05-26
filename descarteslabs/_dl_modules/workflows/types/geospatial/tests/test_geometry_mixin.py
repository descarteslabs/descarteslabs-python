from ..geometry import Geometry
from ..geometrycollection import GeometryCollection


def test_buffer():
    geom = Geometry(type="Point", coordinates=[0, 0])
    col = GeometryCollection(type="GeometryCollection", geometries=[geom, geom])
    assert isinstance(geom.buffer(0.0), Geometry)
    assert isinstance(col.buffer(0.0), Geometry)
