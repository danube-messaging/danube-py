from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from danube.proto import SchemaRegistry_pb2, SchemaRegistry_pb2_grpc
from danube.schema import SchemaInfo, SchemaType, CompatibilityMode

if TYPE_CHECKING:
    from danube.auth_service import AuthService
    from danube.connection_manager import ConnectionManager


class SchemaRegistryClient:
    """Client for schema registry operations."""

    def __init__(self, cnx_manager: ConnectionManager, auth_service: AuthService, uri: str) -> None:
        self._cnx_manager = cnx_manager
        self._auth_service = auth_service
        self._uri = uri

    async def _prepare(self, addr: str):
        conn = await self._cnx_manager.get_connection(addr, addr)
        metadata = await self._auth_service.attach_token_if_needed(addr)
        stub = SchemaRegistry_pb2_grpc.SchemaRegistryStub(conn.channel)
        return metadata, stub

    def register_schema(self, subject: str) -> SchemaRegistrationBuilder:
        return SchemaRegistrationBuilder(self, subject)

    async def get_schema_by_id(self, schema_id: int) -> SchemaInfo:
        return await self.get_schema_version(schema_id, None)

    async def get_schema_version(self, schema_id: int, version: Optional[int] = None) -> SchemaInfo:
        metadata, stub = await self._prepare(self._uri)
        request = SchemaRegistry_pb2.GetSchemaRequest(schema_id=schema_id)
        if version is not None:
            request.version = version
        resp = await stub.GetSchema(request, metadata=metadata)
        return _schema_info_from_proto(resp)

    async def get_latest_schema(self, subject: str) -> SchemaInfo:
        metadata, stub = await self._prepare(self._uri)
        resp = await stub.GetLatestSchema(
            SchemaRegistry_pb2.GetLatestSchemaRequest(subject=subject),
            metadata=metadata,
        )
        return _schema_info_from_proto(resp)

    async def list_versions(self, subject: str) -> list[int]:
        metadata, stub = await self._prepare(self._uri)
        resp = await stub.ListVersions(
            SchemaRegistry_pb2.ListVersionsRequest(subject=subject),
            metadata=metadata,
        )
        return [v.version for v in resp.versions]

    async def check_compatibility(
        self,
        subject: str,
        schema_data: bytes,
        schema_type: SchemaType,
        mode: Optional[CompatibilityMode] = None,
    ) -> tuple[bool, list[str]]:
        metadata, stub = await self._prepare(self._uri)
        request = SchemaRegistry_pb2.CheckCompatibilityRequest(
            subject=subject,
            new_schema_definition=schema_data,
            schema_type=schema_type.as_string(),
        )
        if mode is not None:
            request.compatibility_mode = mode.as_string()
        resp = await stub.CheckCompatibility(request, metadata=metadata)
        return resp.is_compatible, list(resp.errors)

    async def set_compatibility_mode(self, subject: str, mode: CompatibilityMode) -> tuple[bool, str]:
        metadata, stub = await self._prepare(self._uri)
        resp = await stub.SetCompatibilityMode(
            SchemaRegistry_pb2.SetCompatibilityModeRequest(
                subject=subject,
                compatibility_mode=mode.as_string(),
            ),
            metadata=metadata,
        )
        return resp.success, resp.message

    async def delete_schema_version(self, subject: str, version: int) -> tuple[bool, str]:
        metadata, stub = await self._prepare(self._uri)
        resp = await stub.DeleteSchemaVersion(
            SchemaRegistry_pb2.DeleteSchemaVersionRequest(
                subject=subject,
                version=version,
            ),
            metadata=metadata,
        )
        return resp.success, resp.message

    async def configure_topic_schema(
        self,
        topic_name: str,
        schema_subject: str,
        validation_policy: str = "none",
        enable_payload_validation: bool = False,
    ) -> tuple[bool, str]:
        metadata, stub = await self._prepare(self._uri)
        resp = await stub.ConfigureTopicSchema(
            SchemaRegistry_pb2.ConfigureTopicSchemaRequest(
                topic_name=topic_name,
                schema_subject=schema_subject,
                validation_policy=validation_policy,
                enable_payload_validation=enable_payload_validation,
            ),
            metadata=metadata,
        )
        return resp.success, resp.message

    async def update_topic_validation_policy(
        self,
        topic_name: str,
        validation_policy: str,
        enable_payload_validation: bool = False,
    ) -> tuple[bool, str]:
        metadata, stub = await self._prepare(self._uri)
        resp = await stub.UpdateTopicValidationPolicy(
            SchemaRegistry_pb2.UpdateTopicValidationPolicyRequest(
                topic_name=topic_name,
                validation_policy=validation_policy,
                enable_payload_validation=enable_payload_validation,
            ),
            metadata=metadata,
        )
        return resp.success, resp.message

    async def get_topic_schema_config(self, topic_name: str):
        metadata, stub = await self._prepare(self._uri)
        return await stub.GetTopicSchemaConfig(
            SchemaRegistry_pb2.GetTopicSchemaConfigRequest(topic_name=topic_name),
            metadata=metadata,
        )


class SchemaRegistrationBuilder:
    """Fluent builder for schema registration."""

    def __init__(self, client: SchemaRegistryClient, subject: str) -> None:
        self._client = client
        self._subject = subject
        self._schema_type: Optional[SchemaType] = None
        self._schema_data: bytes = b""
        self._description: str = ""
        self._created_by: str = ""
        self._tags: list[str] = []

    def with_type(self, schema_type: SchemaType) -> SchemaRegistrationBuilder:
        self._schema_type = schema_type
        return self

    def with_schema_data(self, data: bytes) -> SchemaRegistrationBuilder:
        self._schema_data = data
        return self

    def with_description(self, description: str) -> SchemaRegistrationBuilder:
        self._description = description
        return self

    def with_created_by(self, created_by: str) -> SchemaRegistrationBuilder:
        self._created_by = created_by
        return self

    def with_tags(self, tags: list[str]) -> SchemaRegistrationBuilder:
        self._tags = tags
        return self

    async def execute(self) -> int:
        if self._schema_type is None:
            raise ValueError("schema type is required")
        if not self._schema_data:
            raise ValueError("schema data is required")

        created_by = self._created_by or "danube-py"
        metadata, stub = await self._client._prepare(self._client._uri)

        resp = await stub.RegisterSchema(
            SchemaRegistry_pb2.RegisterSchemaRequest(
                subject=self._subject,
                schema_type=self._schema_type.as_string(),
                schema_definition=self._schema_data,
                description=self._description,
                created_by=created_by,
                tags=self._tags,
            ),
            metadata=metadata,
        )
        return resp.schema_id


def _schema_info_from_proto(resp) -> SchemaInfo:
    return SchemaInfo(
        schema_id=resp.schema_id,
        subject=resp.subject,
        version=resp.version,
        schema_type=resp.schema_type,
        schema_definition=resp.schema_definition,
        fingerprint=resp.fingerprint,
    )
