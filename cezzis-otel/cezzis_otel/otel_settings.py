import socket
from typing import Optional


class OTelSettings:
    """OpenTelemetry settings for configuring tracing and logging.

    Attributes:
        service_name (str): The name of the service.
        service_namespace (str): The namespace of the service.
        service_version (str): The version of the service.
        otlp_exporter_endpoint (str): The OTLP exporter endpoint URL.
        otlp_exporter_auth_header (str): The authorization header for the OTLP exporter.
        environment (str): The environment in which the service is running.
        instance_id (str): The unique identifier for the service instance.
        enable_logging (bool): Flag to enable logging.
        enable_tracing (bool): Flag to enable tracing.
        certificate_file (Optional[str]): Path to the CA certificate file for secure connections.
        client_certificate_file (Optional[str]): Path to the client certificate file for mutual TLS authentication.
        client_key_file (Optional[str]): Path to the client key file for mutual TLS authentication
        enable_console_logging (Optional[bool]): Flag to enable console logging output.

    Methods:
        __init__: Initializes the OTelSettings with provided values.
    """

    def __init__(
        self,
        service_name: str,
        service_namespace: str,
        service_version: str,
        otlp_exporter_endpoint: str,
        otlp_exporter_auth_header: str,
        environment: str,
        instance_id: str,
        enable_logging: bool = True,
        enable_tracing: bool = True,
        certificate_file: Optional[str] = None,
        client_certificate_file: Optional[str] = None,
        client_key_file: Optional[str] = None,
        enable_console_logging: Optional[bool] = False,
    ) -> None:
        """Initialize OTelSettings with provided values.

        Args:
            service_name (str): The name of the service.
            service_namespace (str): The namespace of the service.
            service_version (str): The version of the service.
            otlp_exporter_endpoint (str): The OTLP exporter endpoint URL.
            otlp_exporter_auth_header (str): The authorization header for the OTLP exporter.
            environment (str): The environment in which the service is running.
            instance_id (str): The unique identifier for the service instance.
            enable_logging (bool): Flag to enable logging. Defaults to True.
            enable_tracing (bool): Flag to enable tracing. Defaults to True.
            certificate_file (str | None): Path to the CA certificate file for secure connections. Defaults to None.
            client_certificate_file (str | None): Path to the client certificate file for mutual TLS authentication. Defaults to None.
            client_key_file (str | None): Path to the client key file
            enable_console_logging (bool | None): Flag to enable console logging output. Defaults to False.


        Returns:
            None
        """

        self.service_name = service_name
        if self.service_name is None or self.service_name.strip() == "":
            self.service_name = "unknown"

        self.service_namespace = service_namespace
        if self.service_namespace is None or self.service_namespace.strip() == "":
            self.service_namespace = "unknown"

        self.service_version = service_version
        if self.service_version is None or self.service_version.strip() == "":
            self.service_version = "unknown"

        self.environment = environment
        if self.environment is None or self.environment.strip() == "":
            self.environment = "unknown"

        self.instance_id = instance_id
        if self.instance_id is None or self.instance_id.strip() == "":
            self.instance_id = socket.gethostname()

        self.otlp_exporter_endpoint = otlp_exporter_endpoint
        self.otlp_exporter_auth_header = otlp_exporter_auth_header
        self.enable_logging = enable_logging
        self.enable_tracing = enable_tracing
        self.certificate_file = certificate_file
        self.client_certificate_file = client_certificate_file
        self.client_key_file = client_key_file
        self.enable_console_logging = enable_console_logging
