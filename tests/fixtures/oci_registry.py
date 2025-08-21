"""OCI Registry fixtures for testing using testcontainers."""
import pytest
from testcontainers.core.container import DockerContainer


@pytest.fixture(scope="session")
def oci_registry():
    """
    Provides a real OCI registry for testing.
    
    Spins up a registry:2 container and returns the registry URL.
    The fixture is session-scoped for performance - the same registry
    is reused across all tests in the session.
    
    Returns:
        str: Registry URL in format "host:port"
    """
    with DockerContainer("registry:2") as container:
        container.with_exposed_ports(5000)
        container.start()
        
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5000)
        
        registry_url = f"{host}:{port}"
        
        yield registry_url