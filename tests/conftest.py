"""Root pytest configuration for modelops-bundles tests."""
import pytest

from modelops_bundles.settings import Settings
from modelops_bundles.providers.bundle_content import BundleContentProvider
from .storage.fakes.fake_oci_registry import FakeOciRegistry
from .storage.fakes.fake_external import FakeExternalStore

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


# Set up test environment variables
@pytest.fixture(autouse=True)
def test_env(monkeypatch):
    """Automatically set up test environment variables."""
    monkeypatch.setenv("MODELOPS_REGISTRY_URL", "http://localhost:5000")
    monkeypatch.setenv("MODELOPS_REGISTRY_REPO", "testns")
    monkeypatch.setenv("MODELOPS_REGISTRY_INSECURE", "true")


# Standardized test fixtures
@pytest.fixture
def settings():
    """Standard test settings."""
    return Settings(
        registry_url="http://localhost:5000",
        registry_repo="testns",
        registry_insecure=True
    )


@pytest.fixture
def registry():
    """Standard fake OCI registry for testing."""
    return FakeOciRegistry()


@pytest.fixture
def external():
    """Standard fake external store for testing."""
    return FakeExternalStore()


@pytest.fixture
def provider(settings, registry, external):
    """Standard bundle content provider for testing."""
    return BundleContentProvider(
        registry=registry,
        external=external,
        settings=settings
    )