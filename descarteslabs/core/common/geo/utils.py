# Copyright 2018-2023 Descartes Labs.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from math import floor


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


def valid_latlon_bounds(bounds, tol=0.001, lon_wrap=0):
    "Return whether bounds fall within [-180, 180] for x and [-90, 90] for y"
    return (
        (-180 + lon_wrap) <= round_if_close_to_int(bounds[0], tol) <= (180 + lon_wrap)
        and -90 <= round_if_close_to_int(bounds[1], tol) <= 90
        and (-180 + lon_wrap)
        <= round_if_close_to_int(bounds[2], tol)
        <= (180 + lon_wrap)
        and -90 <= round_if_close_to_int(bounds[3], tol) <= 90
    )


def is_geographic_crs(crs, with_lon_wrap=False):
    is_geographic = False
    lon_wrap = 0
    if isinstance(crs, str):
        crs = crs.lower()
        if crs == "epsg:4326":
            # WGS84 geodetic CRS. Other geodetic EPSG codes (e.g. NAD27) are incorrectly rejected.
            is_geographic = True
        elif crs.startswith("+proj=longlat"):
            # PROJ.4
            is_geographic = True
            for word in crs.split():
                if word.startswith("+lon_wrap"):
                    try:
                        lon_wrap = float(word.split("=", 1)[1])
                    except Exception:
                        pass
                    break
        elif (
            crs.startswith("geogcs[")  # deprecated
            or crs.startswith("geodcrs[")
            or crs.startswith("geodeticcrs[")
        ):
            is_geographic = True
    if with_lon_wrap:
        return is_geographic, lon_wrap
    else:
        return is_geographic


def is_wgs84_crs(crs):
    if not isinstance(crs, str):
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


def round_if_close_to_int(value, tol=0.001):
    closest_int = int(floor(value + 0.5))

    if abs(value - closest_int) < tol:
        return closest_int
    else:
        return value
