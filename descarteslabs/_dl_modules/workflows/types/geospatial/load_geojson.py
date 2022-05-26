import json
import os

from .geometry import Geometry
from .geometrycollection import GeometryCollection
from .feature import Feature
from .featurecollection import FeatureCollection

geojson_type_to_wf_type = {
    "point": Geometry,
    "multipoint": Geometry,
    "linestring": Geometry,
    "multilinestring": Geometry,
    "polygon": Geometry,
    "multipolygon": Geometry,
    "geometrycollection": GeometryCollection,
    "feature": Feature,
    "featurecollection": FeatureCollection,
}


def load_geojson(geojson):
    """
    Create a Workflows `Geometry`, `GeometryCollection`, `Feature`, or `FeatureCollection` from a GeoJSON mapping.

    Parameters
    ----------
    geojson: dict
        Dict-like object representing a GeoJSON Geometry, Feature, FeatureCollection.

    Returns
    -------
    geometry: `Geometry`, `GeometryCollection`, `Feature`, or `FeatureCollection`

    Example
    -------
    >>> from descarteslabs.workflows import load_geojson
    >>> # type will be inferred based on the contents of the GeoJSON
    >>> # geometry
    >>> geojson = {"type": "Point", "coordinates": [1, 2]}
    >>> load_geojson(geojson)
    <descarteslabs.workflows.types.geospatial.geometry.Geometry object at 0x...>
    >>> # geometry collection
    >>> geojson = {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [1, 2]}]}
    >>> load_geojson(geojson)
    <descarteslabs.workflows.types.geospatial.geometrycollection.GeometryCollection object at 0x...>
    >>> # feature
    >>> geojson = {"type": "Feature",
    ...            "geometry": {"type": "Point", "coordinates": [1, 2]},
    ...            "properties": {"foo": "bar"}}
    >>> load_geojson(geojson)
    <descarteslabs.workflows.types.geospatial.feature.Feature object at 0x...>
    >>> # feature collection
    >>> geojson = {"type": "FeatureCollection",
    ...            "features": [{"type": "Feature",
    ...              "geometry": {"type": "Point", "coordinates": [1, 2]},
    ...              "properties": {"foo": "bar"}}]}
    >>> load_geojson(geojson)
    <descarteslabs.workflows.types.geospatial.featurecollection.FeatureCollection object at 0x...>
    """
    try:
        geojson = geojson.__geo_interface__
    except AttributeError:
        pass

    try:
        geojson_type = geojson["type"]
    except KeyError:
        raise ValueError("Invalid GeoJSON: missing 'type' field: {}".format(geojson))
    except TypeError:
        raise TypeError(
            "Expected a mapping type or object with __geo_interface__ for geojson, but got {}".format(
                geojson_type
            )
        )

    try:
        wf_type = geojson_type_to_wf_type[geojson_type.lower()]
    except KeyError:
        raise ValueError("Unsupported GeoJSON type {!r}".format(geojson_type))

    return wf_type.from_geojson(geojson)


def load_geojson_file(path):
    """
    Create a Workflows `Geometry`, `GeometryCollection`, `Feature`, or `FeatureCollection` from a GeoJSON file.

    Parameters
    ----------
    path: str or path-like object
        Path to a file on disk containing GeoJSON data for a Geometry, Feature, or FeatureCollection.

        Files of more than ~1MiB are not recommended. For larger amounts of data,
        try the Vector service and `FeatureCollection.from_vector_id`.

    Returns
    -------
    geometry: `Geometry`, `GeometryCollection`, `Feature`, or `FeatureCollection`

    Example
    -------
    >>> from descarteslabs.workflows import load_geojson_file
    >>> # type will be inferred based on the contents of the GeoJSON in the file
    >>> obj = load_geojson_file("path/to/geojson/file") # doctest: +SKIP
    """
    path = os.path.expanduser(path)
    with open(path) as f:
        geojson = json.load(f)
    return load_geojson(geojson)
