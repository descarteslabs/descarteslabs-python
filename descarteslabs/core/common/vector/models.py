# © 2025 EarthDaily Analytics Corp.
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

from typing import Any, Dict
from uuid import uuid4

from pydantic import BaseModel, Field


class VectorBaseModel(BaseModel):
    uuid: str = Field(
        default_factory=uuid4,
        json_schema_extra={"primary_key": True},
    )


class PointBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "POINT"})


class LineBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "LINESTRING"})


class PolygonBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "POLYGON"})


class MultiPointBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "MULTIPOINT"})


class MultiLineBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "MULTILINESTRING"})


class MultiPolygonBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "MULTIPOLYGON"})


class GenericFeatureBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "GEOMETRY"})
    properties: Dict[str, Any] = {}
