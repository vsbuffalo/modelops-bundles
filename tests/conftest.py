"""Root pytest configuration for modelops-bundles tests."""
import pytest

# Import fixtures to make them available
from .fixtures.oci_registry import oci_registry


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