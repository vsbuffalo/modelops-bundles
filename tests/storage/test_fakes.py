"""
Tests for fake storage implementations.

These tests verify that fakes implement the protocols correctly
and handle all documented error conditions.
"""
from __future__ import annotations

import pytest

from modelops_bundles.storage.base import ExternalStat
from modelops_bundles.storage.fakes.fake_external import FakeExternalStore
from modelops_bundles.storage.fakes.fake_oras import FakeOrasStore


class TestFakeOrasStore:
    """Test FakeOrasStore implementation."""
    
    def test_put_get_blob_roundtrip(self) -> None:
        """Test storing and retrieving blobs."""
        store = FakeOrasStore()
        digest = "sha256:a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"
        data = b"test blob content"
        
        # Put blob
        store.put_blob(digest, data)
        
        # Check exists
        assert store.blob_exists(digest)
        
        # Get blob
        retrieved = store.get_blob(digest)
        assert retrieved == data
    
    def test_put_get_manifest_roundtrip(self) -> None:
        """Test storing and retrieving manifests."""
        store = FakeOrasStore()
        media_type = "application/vnd.oci.image.manifest.v1+json"
        payload = b'{"test": "manifest"}'
        
        # Put manifest returns computed digest
        digest = store.put_manifest(media_type, payload)
        assert digest.startswith("sha256:")
        assert len(digest) == 71  # "sha256:" + 64 hex chars
        
        # Get manifest
        retrieved = store.get_manifest(digest)
        assert retrieved == payload
    
    def test_blob_exists_false_for_missing(self) -> None:
        """Test blob_exists returns False for missing blobs."""
        store = FakeOrasStore()
        digest = "sha256:a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"
        
        assert not store.blob_exists(digest)
    
    def test_get_blob_raises_keyerror_for_missing(self) -> None:
        """Test get_blob raises KeyError for missing blobs."""
        store = FakeOrasStore()
        digest = "sha256:a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"
        
        with pytest.raises(KeyError, match=digest):
            store.get_blob(digest)
    
    def test_get_manifest_raises_keyerror_for_missing(self) -> None:
        """Test get_manifest raises KeyError for missing manifests."""
        store = FakeOrasStore()
        digest_or_ref = "sha256:missing"
        
        with pytest.raises(KeyError, match=digest_or_ref):
            store.get_manifest(digest_or_ref)
    
    def test_put_blob_invalid_digest_raises_valueerror(self) -> None:
        """Test put_blob raises ValueError for invalid digest format."""
        store = FakeOrasStore()
        data = b"test data"
        
        # Test various invalid formats
        invalid_digests = [
            "not-a-digest",
            "sha256:too-short",
            "sha256:UPPERCASE",
            "sha256:with-invalid-chars!",
            "md5:a1b2c3d4",  # wrong algorithm
        ]
        
        for invalid_digest in invalid_digests:
            with pytest.raises(ValueError, match="invalid digest format"):
                store.put_blob(invalid_digest, data)
    
    def test_clear_removes_all_data(self) -> None:
        """Test clear utility method."""
        store = FakeOrasStore()
        digest = "sha256:a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"
        data = b"test data"
        
        store.put_blob(digest, data)
        manifest_digest = store.put_manifest("test/type", b"manifest")
        
        assert store.blob_exists(digest)
        assert store.get_manifest(manifest_digest)
        
        store.clear()
        
        assert not store.blob_exists(digest)
        with pytest.raises(KeyError):
            store.get_manifest(manifest_digest)


class TestFakeExternalStore:
    """Test FakeExternalStore implementation."""
    
    def test_put_stat_get_roundtrip(self) -> None:
        """Test storing, stating, and retrieving objects."""
        store = FakeExternalStore()
        uri = "az://test-bucket/path/to/object"
        data = b"test external content"
        
        # Put object
        stat = store.put(uri, data, tier="hot")
        
        # Verify returned stat
        assert stat.uri == uri
        assert stat.size == len(data)
        assert len(stat.sha256) == 64  # SHA256 hex length
        assert stat.tier == "hot"
        
        # Stat object
        retrieved_stat = store.stat(uri)
        assert retrieved_stat == stat
        
        # Get object
        retrieved_data = store.get(uri)
        assert retrieved_data == data
    
    def test_put_sha256_mismatch_raises_valueerror(self) -> None:
        """Test put raises ValueError when provided SHA256 doesn't match."""
        store = FakeExternalStore()
        uri = "az://test-bucket/object"
        data = b"test data"
        wrong_hash = "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"
        
        with pytest.raises(ValueError, match="sha256 mismatch"):
            store.put(uri, data, sha256=wrong_hash)
    
    def test_put_with_correct_sha256_succeeds(self) -> None:
        """Test put succeeds when provided SHA256 matches."""
        import hashlib
        
        store = FakeExternalStore()
        uri = "az://test-bucket/object"
        data = b"test data"
        correct_hash = hashlib.sha256(data).hexdigest()
        
        stat = store.put(uri, data, sha256=correct_hash)
        assert stat.sha256 == correct_hash
    
    def test_stat_missing_raises_filenotfounderror(self) -> None:
        """Test stat raises FileNotFoundError for missing objects."""
        store = FakeExternalStore()
        uri = "az://test-bucket/missing"
        
        with pytest.raises(FileNotFoundError, match=uri):
            store.stat(uri)
    
    def test_get_missing_raises_filenotfounderror(self) -> None:
        """Test get raises FileNotFoundError for missing objects."""
        store = FakeExternalStore()
        uri = "az://test-bucket/missing"
        
        with pytest.raises(FileNotFoundError, match=uri):
            store.get(uri)
    
    def test_consistent_hash_on_stat(self) -> None:
        """Test that stat returns the same hash computed during put."""
        store = FakeExternalStore()
        uri = "az://test-bucket/object"
        data = b"test consistency"
        
        # Put object and get initial stat
        put_stat = store.put(uri, data)
        
        # Stat the same object
        stat_result = store.stat(uri)
        
        # Hashes should be identical (not recomputed)
        assert put_stat.sha256 == stat_result.sha256
        assert put_stat.size == stat_result.size
    
    def test_put_without_tier(self) -> None:
        """Test put works without tier specification."""
        store = FakeExternalStore()
        uri = "az://test-bucket/no-tier"
        data = b"no tier data"
        
        stat = store.put(uri, data)
        assert stat.tier is None
        
        retrieved_stat = store.stat(uri)
        assert retrieved_stat.tier is None
    
    def test_clear_removes_all_data(self) -> None:
        """Test clear utility method."""
        store = FakeExternalStore()
        uri = "az://test-bucket/object"
        data = b"test data"
        
        store.put(uri, data)
        assert store.get(uri) == data
        
        store.clear()
        
        with pytest.raises(FileNotFoundError):
            store.stat(uri)
        with pytest.raises(FileNotFoundError):
            store.get(uri)


class TestProtocolCompliance:
    """Test that fakes properly implement their protocols."""
    
    def test_fake_oras_subclasses_protocol(self) -> None:
        """Verify FakeOrasStore subclasses OrasStore for drift control."""
        from modelops_bundles.storage.base import OrasStore
        
        store = FakeOrasStore()
        assert isinstance(store, OrasStore)
    
    def test_fake_external_subclasses_protocol(self) -> None:
        """Verify FakeExternalStore subclasses ExternalStore for drift control."""
        from modelops_bundles.storage.base import ExternalStore
        
        store = FakeExternalStore()
        assert isinstance(store, ExternalStore)


class TestIntegrationWithProvider:
    """Test that providers can be constructed with fakes."""
    
    def test_provider_construction_with_fakes(self) -> None:
        """Test OrasExternalProvider can be constructed with fakes."""
        from modelops_bundles.providers.oras_external import OrasExternalProvider
        
        oras = FakeOrasStore()
        external = FakeExternalStore()
        
        # Should construct without errors
        provider = OrasExternalProvider(oras=oras, external=external)
        
        # Verify stores are accessible (implementation detail, but good for testing)
        assert provider._oras is oras
        assert provider._external is external
    
    def test_provider_fetch_external_uses_store(self) -> None:
        """Test that provider.fetch_external delegates to external store."""
        from modelops_bundles.providers.oras_external import OrasExternalProvider
        from modelops_bundles.runtime_types import MatEntry
        
        # Set up fake store with data
        external = FakeExternalStore()
        uri = "az://test-bucket/data.bin"
        data = b"external test data"
        external.put(uri, data)
        
        # Create provider
        oras = FakeOrasStore()
        provider = OrasExternalProvider(oras=oras, external=external)
        
        # Create MatEntry for external content (use proper SHA256 format)
        import hashlib
        proper_hash = hashlib.sha256(data).hexdigest()
        entry = MatEntry(
            path="data.bin",
            layer="test",
            kind="external",
            content=None,
            uri=uri,
            sha256=proper_hash,
            size=len(data),
            tier=None
        )
        
        # Fetch should delegate to external store
        fetched = provider.fetch_external(entry)
        assert fetched == data
    
    def test_provider_fetch_external_missing_uri_raises(self) -> None:
        """Test fetch_external raises ValueError for entry without URI."""
        from modelops_bundles.providers.oras_external import OrasExternalProvider
        from modelops_bundles.runtime_types import MatEntry
        
        oras = FakeOrasStore()
        external = FakeExternalStore()
        provider = OrasExternalProvider(oras=oras, external=external)
        
        # Since MatEntry validation prevents creating entries with missing URI,
        # we'll create a valid entry and then manually set URI to None
        proper_hash = "a1b2c3d4e5f67890abcdef1234567890abcdef1234567890abcdef1234567890"
        entry = MatEntry(
            path="data.bin",
            layer="test",
            kind="external",
            content=None,
            uri="temp://valid",  # Temporary valid URI
            sha256=proper_hash,
            size=100,
            tier=None
        )
        
        # Manually set URI to None to test provider validation
        # This bypasses dataclass validation but tests provider logic
        object.__setattr__(entry, 'uri', None)
        
        with pytest.raises(ValueError, match="external entry missing uri"):
            provider.fetch_external(entry)