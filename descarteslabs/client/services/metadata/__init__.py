# Copyright 2018 Descartes Labs.
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

from .metadata import Metadata
from .metadata_filtering import Properties

properties = Properties("absolute_orbit", "acquired", "archived", "area", "azimuth_angle",
                        "bright_fraction", "cirrus_fraction", "cloud_fraction", "cloud_fraction_0",
                        "cloud_fraction_1", "descartes_version", "earth_sun_distance",
                        "fill_fraction", "geolocation_accuracy", "opaque_fraction", "product", "processed",
                        "published", "relative_orbit", "roll_angle", "sat_id", "solar_azimuth_angle",
                        "solar_azimuth_angle_0", "solar_elevation_angle", "solar_elevation_angle_0",
                        "sw_version", "terrain_correction", "tile_id", "view_angle", "view_angle_0",
                        "view_angle_1")

__all__ = ["Metadata", "properties"]
