import six

import cachetools


def polygon_from_bounds(bounds):
    "Return a GeoJSON Polygon dict from a (minx, miny, maxx, maxy) tuple"
    return {
        "type": "Polygon",
        "coordinates": (
            (
                (bounds[2], bounds[1]),
                (bounds[2], bounds[3]),
                (bounds[0], bounds[3]),
                (bounds[0], bounds[1]),
                (bounds[2], bounds[1]),
            ),
        ),
    }


def valid_latlon_bounds(bounds):
    "Return whether bounds fall within [-180, 180] for x and [-90, 90] for y"
    return (
        -180 <= bounds[0] <= 180
        and -90 <= bounds[1] <= 90
        and -180 <= bounds[2] <= 180
        and -90 <= bounds[3] <= 90
    )


def is_geographic_crs(crs):
    if not isinstance(crs, six.string_types):
        return False
    lower_crs = crs.lower()
    return (
        lower_crs
        == "epsg:4326"  # WGS84 geodetic CRS. Other geodetic EPSG codes (e.g. NAD27) are incorrectly rejected.
        or lower_crs.startswith("+proj=longlat")  # PROJ.4
        # OGC WKT
        or lower_crs.startswith("geogcs[")  # deprecated
        or lower_crs.startswith("geodcrs[")
        or lower_crs.startswith("geodeticcrs[")
    )


def is_wgs84_crs(crs):
    if not isinstance(crs, six.string_types):
        return False
    lower_crs = crs.lower()
    return (
        lower_crs == "epsg:4326"  # WGS84 geodetic CRS
        or lower_crs.startswith("+proj=longlat +datum=wgs84")  # PROJ.4
        # OGC WKT
        # NOTE: this is a totally heuristic, non-robust guess at whether a WKT string is WGS84.
        # The correct way would be to parse out the spheroid, prime meridian, and unit parameters
        # and check their values. However, parsing WKT is outside the scope of this client,
        # and this method is used only to provide more helpful error messages, so accuracy isn't essential.
        # Here, we'll hope that anyone using a WGS 84 WKT generated it with a tool that sensibly
        # named the CRS as "WGS 84", or with a tool that sensibly *didn't* name a non-WGS84 CRS as "WGS 84".
        or lower_crs.startswith('geogcs["wgs 84"')  # deprecated
        or lower_crs.startswith('geodcrs["wgs 84"')
        or lower_crs.startswith('geodeticcrs["wgs 84"')
    )


@cachetools.cached(cachetools.TTLCache(maxsize=256, ttl=600), key=lambda p, c: p)
def cached_bands_by_product(product_id, metadata_client):
    return metadata_client.get_bands_by_product(product_id)
