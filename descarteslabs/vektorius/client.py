"""
Example:
    In [1]: from descarteslabs.vektorius import vector
       ...: import ibis
       ...: import shapely.geometry
       ...: s = shapely.geometry.shape({
       ...:          "type": "Polygon",
       ...:          "coordinates": [
       ...:            [
       ...:              [
       ...:                -70.32142639160156,
       ...:                43.63657210502274
       ...:              ],
       ...:              [
       ...:                -70.23405075073242,
       ...:                43.63657210502274
       ...:              ],
       ...:              [
       ...:                -70.23405075073242,
       ...:                43.691956147956965
       ...:              ],
       ...:              [
       ...:                -70.32142639160156,
       ...:                43.691956147956965
       ...:              ],
       ...:              [
       ...:                -70.32142639160156,
       ...:                43.63657210502274
       ...:              ]
       ...:            ]
       ...:          ]
       ...: })
       ...: t = vector.table("whosonfirst_v2")
       ...: q = t[t.wof_name, t.geom]
       ...: q = q.filter(q.geom.set_srid(4326).intersects(ibis.literal(s)))
       ...: q = q.limit(10)
       ...: df = q.execute()
       ...: df
       ...:
    Out[1]:
                             geom         wof_name
    0  POINT (-70.29025 43.65688)        Libbytown
    1  POINT (-70.31338 43.67381)    Nasons Corner
    2  POINT (-70.29063 43.66753)        Highlands
    3  POINT (-70.29609 43.66824)  Brighton Corner
    4  POINT (-70.25659 43.68287)     East Deering
    5  POINT (-70.24676 43.66755)         East End
    6  POINT (-70.29654 43.67430)   Deering Center
    7  POINT (-70.27895 43.67926)        Back Cove
    8  POINT (-70.25539 43.66633)     East Bayside
    9  POINT (-70.26962 43.65628)         Parkside

"""

from descarteslabs.client.auth import Auth
from descarteslabs.common.ibis.client import api as serializer


class Vector(object):
    def __init__(
        self,
        host="platform.descarteslabs.com",
        port=443,
        auth=None,
        **grpc_client_kwargs
    ):
        """
        :param str host: Service hostname.
        :param int port: Service port.
        :param Auth auth: A custom user authentication (defaults to the user
            authenticated locally by token information on disk or by environment
            variables)
        """
        if auth is None:
            auth = Auth()

        self._conn = serializer.connect(
            host=host, port=port, auth=auth, **grpc_client_kwargs
        )

    @property
    def connection(self):
        return self._conn

    @property
    def dialect(self):
        return self.connection.dialect

    def table(self, name, database=None):
        return self.connection.table(name, database=database)

    def compile(self, expr):
        stmt = expr.compile()
        return stmt


vector = Vector()
