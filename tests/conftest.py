"""Root pytest configuration for modelops-bundles tests."""
import pytest
from testcontainers.core.container import DockerContainer


# Configure pytest markers
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", 
        "integration: mark test as integration test (requires Docker)"
    )
    config.addinivalue_line(
        "markers",
        "slow: mark test as slow (may take significant time)"
    )


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
    container = DockerContainer("registry:2")
    container.with_exposed_ports(5000)
    container.start()
    
    try:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5000)
        
        registry_url = f"{host}:{port}"
        
        yield registry_url
    finally:
        container.stop()