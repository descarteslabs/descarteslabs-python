from shapely.geometry import mapping


def shapely_to_geojson(geometry):
    """Converts a Shapely Shape geometry to a GeoJSON geometry"""
    if hasattr(geometry, "__geo_interface__"):
        geometry = mapping(geometry)
    return geometry
