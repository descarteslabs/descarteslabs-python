import copy

from shapely.geometry import shape

from descarteslabs.common.dotdict import DotDict


class Feature(object):
    """
    An object matching the format of a GeoJSON Feature with geometry and properties.

    Attributes
    ----------
    geometry : shapely.geometry.Polygon or dict
        If the Shapely package is installed, it will be a shapely shape,
        otherwise a dict of a simple GeoJSON geometry type.
        The geometry must be one of these GeoJSON types:

        * Point
        * MultiPoint
        * Polygon
        * MultiPolygon
        * LineString
        * MultiLineString
        * GeometryCollection

    properties : DotDict
        Fields describing the geometry.
        Values can be strings up to 256 characters, numeric types, or ``None``.
    """

    geojson_type = "Feature"

    def __init__(self, geometry, properties, id=None):
        """
        Example
        -------
        >>> polygon = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]]}
        >>> properties = {"temperature": 70.13, "size": "large"}
        >>> Feature(geometry=polygon, properties=properties)  # doctest: +SKIP
        Feature({
          'geometry': {
            'coordinates': (((-95.0, 42.0), (-93.0, 42.0), (-93.0, 40.0), (-95.0, 41.0), (-95.0, 42.0)),),
            'type': 'Polygon'
          },
          'id': None,
          'properties': {
            'size': 'large',
            'temperature': 70.13
          }
        })
        """
        if geometry is None:
            raise ValueError("geometry should not be None")

        self.geometry = shape(geometry)

        self.properties = DotDict(properties)
        self.id = id

    @classmethod
    def _create_from_jsonapi(cls, response):
        geometry = response.attributes.get("geometry")
        properties = response.attributes.get("properties")

        self = cls(geometry=geometry, properties=properties, id=response.id)

        return self

    @property
    def geojson(self):
        """
        Returns the ``Feature`` as a GeoJSON dict.

        Returns
        -------
        dict
            GeoJSON Feature as a dict with the following keys:

                .. highlight:: none

                ::

                    geometry:   GeoJSON geometry object. A dict with the following
                                keys:

                        coordinates: Coordinates of the GeoJSON object. A list,
                                     list(list) or list(list(list)) depending on
                                     the type of the geometry object.
                        type:        GeoJSON object type.

                    properties: A dict with feature properties.
                    type:       "Feature"


        Example
        -------
        >>> polygon = {
        ...    'type': 'Polygon',
        ...    'coordinates': [[[-95, 42], [-93, 42], [-93, 40], [-95, 41], [-95, 42]]]}
        >>> properties = {"temperature": 70.13, "size": "large", "tags": None}
        >>> feature = Feature(geometry=polygon, properties=properties)
        >>> feature.geojson  # doctest: +SKIP
        {'geometry': {'coordinates': (((-95.0, 42.0),
            (-93.0, 42.0),
            (-93.0, 40.0),
            (-95.0, 41.0),
            (-95.0, 42.0)),),
         'type': 'Polygon'},
         'id': None,
         'properties': {
           'size': 'large',
           'temperature': 70.13
         },
         'type': 'Feature'
        }
        """
        properties = copy.deepcopy(self.properties)

        geometry = self.geometry.__geo_interface__

        return dict(
            properties=properties, geometry=geometry, id=self.id, type=self.geojson_type
        )

    @property
    def __geo_interface__(self):
        return self.geojson["geometry"]

    def _repr_json_(self):
        return DotDict(self.geojson)

    def __repr__(self):
        return "Feature({})".format(repr(DotDict(self.geojson)))
