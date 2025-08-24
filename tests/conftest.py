"""Root pytest configuration for modelops-bundles tests."""
import pytest

from modelops_bundles.settings import Settings
from modelops_bundles.providers.bundle_content import BundleContentProvider
# Removed FakeOciRegistry - now using FakeOrasBundleRegistry
from .storage.fakes.fake_oras_bundle_registry import FakeOrasBundleRegistry
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
    """Standard fake registry for testing (now ORAS-based)."""
    return FakeOrasBundleRegistry()


@pytest.fixture
def oras_registry():
    """Alias for registry fixture - both are ORAS-based now."""
    return FakeOrasBundleRegistry()


@pytest.fixture
def oras_provider(settings, oras_registry, external):
    """Bundle content provider using ORAS registry for testing."""
    return BundleContentProvider(
        registry=oras_registry,
        external=external,
        settings=settings
    )


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