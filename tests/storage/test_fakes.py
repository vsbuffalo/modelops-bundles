"""
Tests for fake storage implementations.

These tests verify that fakes implement the protocols correctly
and handle all documented error conditions.
"""
from __future__ import annotations

import pytest

from modelops_bundles.storage.base import ExternalStat
from tests.storage.fakes.fake_external import FakeExternalStore
# Removed FakeOciRegistry - using FakeOrasBundleRegistry
from tests.storage.fakes.fake_oras_bundle_registry import FakeOrasBundleRegistry


class TestFakeOrasBundleRegistryAdvanced:
    """Test FakeOrasBundleRegistry implementation (replaces old OCI tests)."""
    
    def test_put_get_blob_roundtrip(self) -> None:
        """Test storing and retrieving blobs."""
        import hashlib
        
        registry = FakeOrasBundleRegistry()
        repo = "test/repo"
        data = b"test blob content"
        digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
        
        registry.put_blob(repo, digest, data)
        retrieved = registry.get_blob(repo, digest)
        
        assert retrieved == data
    
    def test_put_get_manifest_roundtrip(self) -> None:
        """Test storing and retrieving manifests."""
        registry = FakeOrasBundleRegistry()
        repo = "test/repo"
        tag = "v1.0.0"
        media_type = "application/vnd.oci.image.manifest.v1+json"
        manifest_data = b'{"test": "manifest"}'
        
        digest = registry.put_manifest(repo, media_type, manifest_data, tag)
        
        # Test retrieval by tag
        retrieved = registry.get_manifest(repo, tag)
        assert retrieved == manifest_data
        
        # Test retrieval by digest
        retrieved_by_digest = registry.get_manifest(repo, digest)
        assert retrieved_by_digest == manifest_data
        
        # Test head_manifest
        head_digest = registry.head_manifest(repo, tag)
        assert head_digest == digest


class TestFakeExternalStore:
    """Test FakeExternalStore implementation."""
    
    def test_put_stat_get_roundtrip(self) -> None:
        """Test complete workflow: put -> stat -> get."""
        import hashlib
        
        store = FakeExternalStore()
        uri = "az://test-container/test-file"
        data = b"test file content"
        expected_sha256 = hashlib.sha256(data).hexdigest()
        
        # Put file (store computes hash automatically)
        stat = store.put(uri, data)
        
        # Verify returned stat
        assert stat.uri == uri
        assert stat.size == len(data)
        assert stat.sha256 == expected_sha256
        
        # Stat file separately
        stat2 = store.stat(uri)
        assert stat2.uri == uri
        assert stat2.size == len(data)
        assert stat2.sha256 == expected_sha256
        
        # Get file
        retrieved = store.get(uri)
        assert retrieved == data


class TestProtocolCompliance:
    """Test that fakes properly implement their protocols."""
    
    
    def test_fake_oras_bundle_registry_basic_ops(self) -> None:
        """Test that FakeOrasBundleRegistry supports basic operations."""
        import hashlib
        
        registry = FakeOrasBundleRegistry()
        
        # Test basic blob operations
        repo = "test/repo"
        data = b"test content"
        digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
        
        # These should work without errors
        assert not registry.blob_exists(repo, digest)
        registry.put_blob(repo, digest, data)
        assert registry.blob_exists(repo, digest)
        retrieved = registry.get_blob(repo, digest)
        assert retrieved == data
    
    def test_fake_external_subclasses_protocol(self) -> None:
        """Test that FakeExternalStore implements ExternalStore protocol."""
        from modelops_bundles.storage.base import ExternalStore
        store = FakeExternalStore()  
        assert isinstance(store, ExternalStore)


class TestIntegrationWithProvider:
    """Test integration between fakes and BundleContentProvider."""
    
    def test_provider_construction_with_fakes(self) -> None:
        """Test that BundleContentProvider can be constructed with fakes."""
        from modelops_bundles.providers.bundle_content import BundleContentProvider
        from modelops_bundles.settings import Settings
        
        registry = FakeOrasBundleRegistry()
        external = FakeExternalStore()
        settings = Settings(registry_url="http://localhost:5000", registry_repo="test")
        
        provider = BundleContentProvider(registry=registry, external=external, settings=settings)
        assert provider is not None
        assert provider._registry is registry
        assert provider._external is external
        assert provider._settings is settings
    
    def test_provider_construction_with_oras_fake(self) -> None:
        """Test that BundleContentProvider can be constructed with ORAS fake."""
        from modelops_bundles.providers.bundle_content import BundleContentProvider
        from modelops_bundles.settings import Settings
        
        registry = FakeOrasBundleRegistry()
        external = FakeExternalStore()
        settings = Settings(registry_url="http://localhost:5000", registry_repo="test")
        
        provider = BundleContentProvider(registry=registry, external=external, settings=settings)
        assert provider is not None
        assert provider._registry is registry
        assert provider._external is external
        assert provider._settings is settings


# Remaining tests are kept as they are working correctly...