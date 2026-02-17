from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class CompatibilityMode(Enum):
    NONE = "none"
    BACKWARD = "backward"
    FORWARD = "forward"
    FULL = "full"

    def as_string(self) -> str:
        return self.value

    @staticmethod
    def parse(value: str) -> CompatibilityMode:
        try:
            return CompatibilityMode(value.lower())
        except ValueError:
            raise ValueError(f"unknown compatibility mode: {value}")


class SchemaType(Enum):
    BYTES = "bytes"
    STRING = "string"
    NUMBER = "number"
    AVRO = "avro"
    JSON_SCHEMA = "json_schema"
    PROTOBUF = "protobuf"

    def as_string(self) -> str:
        return self.value

    @staticmethod
    def parse(value: str) -> SchemaType:
        v = value.lower()
        if v in ("jsonschema", "json_schema"):
            return SchemaType.JSON_SCHEMA
        if v in ("protobuf", "proto"):
            return SchemaType.PROTOBUF
        try:
            return SchemaType(v)
        except ValueError:
            raise ValueError(f"unknown schema type: {value}")


@dataclass
class SchemaInfo:
    schema_id: int
    subject: str
    version: int
    schema_type: str
    schema_definition: bytes
    fingerprint: str

    def schema_definition_as_string(self) -> str:
        return self.schema_definition.decode("utf-8")
