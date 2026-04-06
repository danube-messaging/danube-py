from __future__ import annotations

from typing import Callable, Optional

import grpc

from danube.auth_service import AuthService
from danube.connection_manager import ConnectionManager, ConnectionOptions
from danube.consumer import ConsumerBuilder
from danube.health_check import HealthCheckService
from danube.lookup_service import LookupService
from danube.producer import ProducerBuilder
from danube.schema_registry_client import SchemaRegistryClient


class DanubeClient:
    """Main client for interacting with the Danube messaging system."""

    def __init__(
        self,
        uri: str,
        connection_manager: ConnectionManager,
        lookup_service: LookupService,
        health_check_service: HealthCheckService,
        auth_service: AuthService,
    ) -> None:
        self.uri = uri
        self.connection_manager = connection_manager
        self.lookup_service = lookup_service
        self.health_check_service = health_check_service
        self.auth_service = auth_service

    def new_producer(self) -> ProducerBuilder:
        return ProducerBuilder(self)

    def new_consumer(self) -> ConsumerBuilder:
        return ConsumerBuilder(self)

    def schema(self) -> SchemaRegistryClient:
        return SchemaRegistryClient(
            self.connection_manager, self.auth_service, self.uri
        )


class DanubeClientBuilder:
    """Builder for configuring and creating a DanubeClient."""

    def __init__(self) -> None:
        self._uri: str = ""
        self._options = ConnectionOptions()

    def service_url(self, url: str) -> DanubeClientBuilder:
        self._uri = url
        return self

    def with_tls(self, ca_cert_path: str) -> DanubeClientBuilder:
        with open(ca_cert_path, "rb") as f:
            ca_data = f.read()
        self._options.tls_credentials = grpc.ssl_channel_credentials(
            root_certificates=ca_data
        )
        self._options.use_tls = True
        return self

    def with_mtls(
        self, ca_cert_path: str, client_cert_path: str, client_key_path: str
    ) -> DanubeClientBuilder:
        with open(ca_cert_path, "rb") as f:
            ca_data = f.read()
        with open(client_cert_path, "rb") as f:
            cert_data = f.read()
        with open(client_key_path, "rb") as f:
            key_data = f.read()
        self._options.tls_credentials = grpc.ssl_channel_credentials(
            root_certificates=ca_data,
            private_key=key_data,
            certificate_chain=cert_data,
        )
        self._options.use_tls = True
        return self

    def with_token(self, token: str) -> DanubeClientBuilder:
        """Set the authentication token (JWT) for the client.

        Use ``danube-admin security tokens create`` to generate a token.
        Automatically enables TLS. If no TLS config has been set via
        ``with_tls()`` or ``with_mtls()``, a default TLS config using system
        root certificates is applied.

        For tokens that expire, consider :meth:`with_token_supplier` instead,
        which allows runtime token refresh.
        """
        self._options.token = token
        self._options.use_tls = True
        if self._options.tls_credentials is None:
            self._options.tls_credentials = grpc.ssl_channel_credentials()
        return self

    def with_token_supplier(
        self, supplier: Callable[[], str]
    ) -> DanubeClientBuilder:
        """Set a dynamic token supplier for the client.

        The supplier function is called on **every gRPC request** to obtain the
        current token, enabling runtime token refresh without restarting the
        client. Useful for:

        - File-based tokens updated by infrastructure (K8s projected volumes)
        - Environment-based tokens
        - Custom refresh logic

        Automatically enables TLS (same as ``with_token``).
        """
        self._options.token_supplier = supplier
        self._options.use_tls = True
        if self._options.tls_credentials is None:
            self._options.tls_credentials = grpc.ssl_channel_credentials()
        return self

    async def build(self) -> DanubeClient:
        cnx_manager = ConnectionManager(self._options)
        auth_service = AuthService(cnx_manager)

        lookup_service = LookupService(cnx_manager, auth_service)
        health_check_service = HealthCheckService(cnx_manager, auth_service)

        return DanubeClient(
            uri=self._uri,
            connection_manager=cnx_manager,
            lookup_service=lookup_service,
            health_check_service=health_check_service,
            auth_service=auth_service,
        )
