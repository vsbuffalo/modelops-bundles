"""
Tests for SHA256 integrity verification during external content prefetch.

Verifies that prefetch_external validates content hashes and raises
appropriate conflicts when data corruption is detected.
"""
from __future__ import annotations

import hashlib
import pytest
from pathlib import Path
from unittest.mock import patch

from modelops_contracts.artifacts import BundleRef, ResolvedBundle
from modelops_bundles.runtime import materialize, WorkdirConflict
from modelops_bundles.runtime_types import MatEntry
from modelops_bundles.runtime_types import ContentProvider
from tests.storage.fakes.fake_oras import FakeBundleRegistryStore


class TestPrefetchIntegrity:
    """Test SHA256 verification during external content prefetch."""
    
    def _create_test_setup(self):
        """Create test registry and mock resolved bundle."""
        registry = FakeBundleRegistryStore()
        repository = "test/repo"
        
        # Create a mock resolved bundle
        resolved = ResolvedBundle(
            ref=BundleRef(digest="sha256:" + "a" * 64),
            manifest_digest="sha256:" + "b" * 64,
            roles={"default": ["data"]},
            layers=["data"],
            external_index_present=True,
            total_size=100,
            cache_dir=None,
            layer_indexes={"data": "sha256:" + "c" * 64}
        )
        
        return registry, repository, resolved
    
    def test_prefetch_sha256_mismatch_raises_conflict(self, tmp_path):
        """Test that SHA256 mismatch during prefetch raises WorkdirConflict."""
        # Setup test data
        good_data = b"correct content"
        corrupt_data = b"corrupt content"
        expected_sha = hashlib.sha256(good_data).hexdigest()
        
        class CorruptProvider(ContentProvider):
            def iter_entries(self, resolved, layers):
                yield MatEntry(
                    path="data/corrupt.txt",
                    layer="data", 
                    kind="external",
                    content=None,
                    uri="az://test/data.txt",
                    sha256=expected_sha,  # Correct expected hash
                    size=len(corrupt_data)
                )
            
            def fetch_external(self, entry):
                return corrupt_data  # Returns corrupt data that doesn't match SHA
        
        provider = CorruptProvider()
        registry, repository, resolved = self._create_test_setup()
        
        ref = BundleRef(digest="sha256:" + "a" * 64)
        
        # Mock resolve to return our test resolved bundle
        with patch('modelops_bundles.runtime.resolve', return_value=resolved):
            # Materialize with prefetch should detect SHA mismatch and raise conflict
            with pytest.raises(WorkdirConflict) as exc_info:
                materialize(
                    ref=ref,
                    dest=str(tmp_path),
                    role="default",
                    provider=provider,
                    prefetch_external=True,
                    registry=registry,
                    repository=repository
                )
        
        # Verify the conflict details
        error = exc_info.value
        assert len(error.conflicts) == 1
        conflict = error.conflicts[0]
        assert conflict["path"] == "data/corrupt.txt"
        assert conflict["expected_sha256"] == expected_sha
        assert conflict["actual_sha256"] == hashlib.sha256(corrupt_data).hexdigest()
    
    def test_prefetch_sha256_match_succeeds(self, tmp_path):
        """Test that correct SHA256 during prefetch succeeds."""
        # Setup test data
        correct_data = b"correct content for testing"
        expected_sha = hashlib.sha256(correct_data).hexdigest()
        
        class GoodProvider(ContentProvider):
            def iter_entries(self, resolved, layers):
                yield MatEntry(
                    path="data/correct.txt",
                    layer="data",
                    kind="external", 
                    content=None,
                    uri="az://test/data.txt",
                    sha256=expected_sha,
                    size=len(correct_data)
                )
            
            def fetch_external(self, entry):
                return correct_data  # Returns correct data that matches SHA
        
        provider = GoodProvider()
        registry, repository, resolved = self._create_test_setup()
        
        ref = BundleRef(digest="sha256:" + "b" * 64)
        
        # Mock resolve to return our test resolved bundle
        with patch('modelops_bundles.runtime.resolve', return_value=resolved):
            # Should succeed without raising WorkdirConflict
            result = materialize(
                ref=ref,
                dest=str(tmp_path),
                role="default",
                provider=provider,
                prefetch_external=True,
                registry=registry,
                repository=repository
            )
        
        # Verify data was written correctly
        data_path = tmp_path / "data" / "correct.txt"
        assert data_path.exists()
        assert data_path.read_bytes() == correct_data
        
        # Verify pointer file was created and marked as fulfilled
        pointer_path = tmp_path / ".mops" / "ptr" / "data" / "correct.txt.json"
        assert pointer_path.exists()
        
        import json
        with open(pointer_path, 'r') as f:
            pointer_data = json.load(f)
        
        assert pointer_data["fulfilled"] is True
        assert pointer_data["sha256"] == expected_sha
        assert pointer_data["local_path"] == "data/correct.txt"
    
    def test_prefetch_disabled_skips_integrity_check(self, tmp_path):
        """Test that integrity check is skipped when prefetch_external=False."""
        # Setup test data - intentionally mismatched SHA and data
        corrupt_data = b"corrupt content"
        expected_sha = hashlib.sha256(b"different content").hexdigest()
        
        class PointerOnlyProvider(ContentProvider):
            def iter_entries(self, resolved, layers):
                yield MatEntry(
                    path="data/pointer_only.txt",
                    layer="data",
                    kind="external",
                    content=None,
                    uri="az://test/data.txt", 
                    sha256=expected_sha,
                    size=len(corrupt_data)
                )
            
            def fetch_external(self, entry):
                return corrupt_data  # This shouldn't be called with prefetch=False
        
        provider = PointerOnlyProvider()
        registry, repository, resolved = self._create_test_setup()
        
        ref = BundleRef(digest="sha256:" + "c" * 64)
        
        # Mock resolve to return our test resolved bundle
        with patch('modelops_bundles.runtime.resolve', return_value=resolved):
            # Should succeed because prefetch is disabled (no integrity check)
            result = materialize(
                ref=ref,
                dest=str(tmp_path),
                role="default",
                provider=provider,
                prefetch_external=False,  # Only create pointer, don't fetch data
                registry=registry,
                repository=repository
            )
        
        # Verify actual data file was NOT created
        data_path = tmp_path / "data" / "pointer_only.txt"
        assert not data_path.exists()
        
        # Verify pointer file was created but not fulfilled
        pointer_path = tmp_path / ".mops" / "ptr" / "data" / "pointer_only.txt.json"
        assert pointer_path.exists()
        
        import json
        with open(pointer_path, 'r') as f:
            pointer_data = json.load(f)
        
        assert pointer_data["fulfilled"] is False
        assert pointer_data["local_path"] is None
        assert pointer_data["sha256"] == expected_sha  # Still records expected hash
    
    def test_multiple_files_some_corrupt_partial_conflict(self, tmp_path):
        """Test mixed scenario: some files have correct hash, others corrupt."""
        # Setup mixed data: one good, one corrupt
        good_data = b"good file content"
        corrupt_data = b"corrupt file content"
        good_sha = hashlib.sha256(good_data).hexdigest()
        expected_corrupt_sha = hashlib.sha256(b"expected but different").hexdigest()
        
        class MixedProvider(ContentProvider):
            def iter_entries(self, resolved, layers):
                yield MatEntry(
                    path="data/good.txt",
                    layer="data",
                    kind="external",
                    content=None,
                    uri="az://test/good.txt",
                    sha256=good_sha,
                    size=len(good_data)
                )
                yield MatEntry(
                    path="data/corrupt.txt", 
                    layer="data",
                    kind="external",
                    content=None,
                    uri="az://test/corrupt.txt",
                    sha256=expected_corrupt_sha,  # Expected hash doesn't match corrupt data
                    size=len(corrupt_data)
                )
            
            def fetch_external(self, entry):
                if "good.txt" in entry.uri:
                    return good_data
                else:
                    return corrupt_data  # Wrong data for expected hash
        
        provider = MixedProvider()
        registry, repository, resolved = self._create_test_setup()
        
        ref = BundleRef(digest="sha256:" + "d" * 64)
        
        # Mock resolve to return our test resolved bundle  
        with patch('modelops_bundles.runtime.resolve', return_value=resolved):
            # Should raise conflict due to corrupt file
            with pytest.raises(WorkdirConflict) as exc_info:
                materialize(
                    ref=ref,
                    dest=str(tmp_path),
                    role="default", 
                    provider=provider,
                    prefetch_external=True,
                    registry=registry,
                    repository=repository
                )
        
        # Verify only the corrupt file is in conflicts
        error = exc_info.value
        assert len(error.conflicts) == 1
        conflict = error.conflicts[0]
        assert conflict["path"] == "data/corrupt.txt"
        assert conflict["expected_sha256"] == expected_corrupt_sha
        assert conflict["actual_sha256"] == hashlib.sha256(corrupt_data).hexdigest()
        
        # Good file should have been written (it had correct SHA256)
        # Only the corrupt file should have been prevented from writing
        good_path = tmp_path / "data" / "good.txt"
        corrupt_path = tmp_path / "data" / "corrupt.txt"
        assert good_path.exists(), "Good file should have been written successfully"
        assert not corrupt_path.exists(), "Corrupt file should not have been written"