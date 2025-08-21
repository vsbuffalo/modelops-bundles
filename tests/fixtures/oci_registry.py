"""OCI Registry fixtures for testing using testcontainers."""
import pytest

try:
    from testcontainers.core.container import DockerContainer
except ImportError:
    DockerContainer = None


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
    if DockerContainer is None:
        pytest.skip("testcontainers not available")
    
    try:
        container = DockerContainer("registry:2")
    except Exception as e:
        pytest.skip(f"Docker not available: {e}")
        
    with container:
        container.with_exposed_ports(5000)
        container.start()
        
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5000)
        
        registry_url = f"{host}:{port}"
        
        yield registry_url