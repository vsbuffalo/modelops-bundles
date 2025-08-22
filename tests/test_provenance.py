"""
Tests for provenance file writing during materialization.

Verifies that .mops/.mops-manifest.json is created with correct metadata
including manifest digest, selected role, layer indexes, and original ref.
"""
from __future__ import annotations

import json
import pytest
from pathlib import Path

from modelops_contracts.artifacts import BundleRef, ResolvedBundle
from modelops_bundles.runtime import materialize
from modelops_bundles.runtime_types import ContentProvider, MatEntry
from collections.abc import Iterable


class TestProvenanceFile:
    """Test provenance file creation and content."""
    
    def test_provenance_file_exists_after_materialize(self, tmp_path):
        """Test that provenance file is created at expected location."""
        # Create a simple test provider that yields no entries
        class EmptyProvider(ContentProvider):
            def iter_entries(self, resolved: ResolvedBundle, layers: list[str]) -> Iterable[MatEntry]:
                return []  # No entries to materialize
            
            def fetch_external(self, entry: MatEntry) -> bytes:
                return b"test"
        
        provider = EmptyProvider()
        
        # Create a simple test bundle ref
        ref = BundleRef(digest="sha256:" + "a" * 64)
        
        # Materialize bundle
        result = materialize(
            ref=ref,
            dest=str(tmp_path),
            role="default",
            provider=provider
        )
        
        # Check that provenance file exists
        provenance_path = tmp_path / ".mops" / ".mops-manifest.json"
        assert provenance_path.exists(), "Provenance file should be created"
        
        # Verify it's valid JSON
        with open(provenance_path, 'r') as f:
            provenance_data = json.load(f)
        
        # Basic structure verification
        assert isinstance(provenance_data, dict)
        assert "manifest_digest" in provenance_data
        assert "role" in provenance_data
        assert "roles" in provenance_data
        assert "layer_indexes" in provenance_data
        assert "ref" in provenance_data
    
    def test_provenance_contains_correct_role_and_layer_indexes(self, tmp_path):
        """Test that provenance contains selected role and layer index mapping."""
        class EmptyProvider(ContentProvider):
            def iter_entries(self, resolved: ResolvedBundle, layers: list[str]) -> Iterable[MatEntry]:
                return []
            
            def fetch_external(self, entry: MatEntry) -> bytes:
                return b"test"
        
        provider = EmptyProvider()
        
        ref = BundleRef(name="test-bundle", version="v1.0.0")
        
        # Materialize with specific role
        result = materialize(
            ref=ref,
            dest=str(tmp_path),
            role="training",
            provider=provider
        )
        
        # Read provenance file
        provenance_path = tmp_path / ".mops" / ".mops-manifest.json"
        with open(provenance_path, 'r') as f:
            provenance_data = json.load(f)
        
        # Verify selected role
        assert provenance_data["role"] == "training"
        
        # Verify layer indexes are present and match resolved bundle
        assert "layer_indexes" in provenance_data
        assert isinstance(provenance_data["layer_indexes"], dict)
        assert provenance_data["layer_indexes"] == result.layer_indexes
        
        # Verify roles mapping
        assert provenance_data["roles"] == result.roles
        
        # Verify manifest digest
        assert provenance_data["manifest_digest"] == result.manifest_digest
        
        # Verify original ref is preserved
        assert "ref" in provenance_data
        ref_data = provenance_data["ref"]
        assert ref_data["name"] == "test-bundle"
        assert ref_data["version"] == "v1.0.0"
    
    def test_provenance_deterministic_json_format(self, tmp_path):
        """Test that provenance JSON is deterministic (sorted keys, compact)."""
        class EmptyProvider(ContentProvider):
            def iter_entries(self, resolved: ResolvedBundle, layers: list[str]) -> Iterable[MatEntry]:
                return []
            
            def fetch_external(self, entry: MatEntry) -> bytes:
                return b"test"
        
        provider = EmptyProvider()
        
        ref = BundleRef(local_path="/fake/path")
        
        materialize(
            ref=ref,
            dest=str(tmp_path),
            role="default",
            provider=provider
        )
        
        # Read provenance file content as text
        provenance_path = tmp_path / ".mops" / ".mops-manifest.json"
        content = provenance_path.read_text(encoding='utf-8')
        
        # Verify it's compact JSON (no spaces around separators)
        assert ", " not in content, "Should use compact separators"
        assert ": " not in content, "Should use compact separators"
        
        # Verify it can be parsed back to consistent dict
        data = json.loads(content)
        
        # Re-serialize with same formatting and compare
        expected = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        assert content == expected, "Should be deterministically formatted"
    
    def test_provenance_overwrite_on_multiple_materializations(self, tmp_path):
        """Test that provenance file is overwritten on subsequent materializations."""
        class EmptyProvider(ContentProvider):
            def iter_entries(self, resolved: ResolvedBundle, layers: list[str]) -> Iterable[MatEntry]:
                return []
            
            def fetch_external(self, entry: MatEntry) -> bytes:
                return b"test"
        
        provider = EmptyProvider()
        
        ref1 = BundleRef(digest="sha256:" + "a" * 64)
        ref2 = BundleRef(digest="sha256:" + "b" * 64)
        
        # First materialization with default role
        materialize(
            ref=ref1,
            dest=str(tmp_path),
            role="default", 
            provider=provider
        )
        
        provenance_path = tmp_path / ".mops" / ".mops-manifest.json"
        
        # Read first provenance
        with open(provenance_path, 'r') as f:
            first_data = json.load(f)
        
        # Second materialization with different role
        materialize(
            ref=ref2,
            dest=str(tmp_path),
            role="training",
            provider=provider,
            overwrite=True
        )
        
        # Read updated provenance
        with open(provenance_path, 'r') as f:
            second_data = json.load(f)
        
        # Verify provenance was updated
        assert first_data["manifest_digest"] != second_data["manifest_digest"]
        assert first_data["role"] != second_data["role"]
        assert second_data["role"] == "training"