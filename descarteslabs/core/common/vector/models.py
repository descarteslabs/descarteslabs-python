# Copyright 2018-2024 Descartes Labs.

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
