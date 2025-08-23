"""
Tests for BundleContentProvider.

These tests verify that the provider correctly converts bundle metadata from 
bundle registry layer indexes into MatEntry objects, with comprehensive error handling.
"""
from __future__ import annotations

import json
import hashlib
import pytest

from modelops_contracts.artifacts import ResolvedBundle, BundleRef, LAYER_INDEX
from modelops_bundles.providers.bundle_content import BundleContentProvider
from tests.storage.fakes.fake_oci_registry import FakeOciRegistry
from modelops_bundles.settings import Settings
from tests.storage.fakes.fake_external import FakeExternalStore
from modelops_bundles.runtime_types import MatEntry


def _layer_index_doc(entries: list[dict]) -> bytes:
    """Create a layer index document with correct mediaType."""
    return json.dumps({
        "mediaType": LAYER_INDEX,
        "entries": entries
    }, separators=(",", ":"), sort_keys=True).encode()


def _digest_for_content(b: bytes) -> str:
    """Compute SHA256 digest for content."""
    return "sha256:" + hashlib.sha256(b).hexdigest()


def _put_layer_index_as_blob(registry, repo: str, payload: bytes) -> str:
    """Store layer index as blob and return digest."""
    digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
    registry.put_blob(repo, digest, payload)
    return digest


def _mk_resolved_with_indexes(code_idx: str | None = None, data_idx: str | None = None) -> ResolvedBundle:
    """Create ResolvedBundle with layer_indexes."""
    ref = BundleRef(name="test-bundle", version="1.0.0")
    layer_indexes = {}
    layers = []

    if code_idx:
        layer_indexes["code"] = code_idx
        layers.append("code")
    if data_idx:
        layer_indexes["data"] = data_idx
        layers.append("data")

    return ResolvedBundle(
        ref=ref,
        manifest_digest="sha256:" + "a"*64,
        roles={"runtime": ["code"], "training": ["code", "data"], "default": ["code"]},
        layers=layers,
        external_index_present=True,
        total_size=123,
        cache_dir=None,
        layer_indexes=layer_indexes,
    )


class TestIterEntries:
    """Test BundleContentProvider.iter_entries method."""
    
    def test_emits_oras_and_external_entries(self):
        """Test happy path: yields both ORAS and external entries."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        # Seed ORAS blobs for code files
        code_file_1 = b"print('hello')\n"
        code_file_2 = b"# utils\n"
        d1 = _digest_for_content(code_file_1)
        d2 = _digest_for_content(code_file_2)
        oras.put_blob("testns/bundles/test-bundle", d1, code_file_1)
        oras.put_blob("testns/bundles/test-bundle", d2, code_file_2)

        # Build code layer index (ORAS entries)
        code_idx_payload = _layer_index_doc([
            {"path": "src/utils.py", "digest": d2, "layer": "code"},
            {"path": "src/model.py", "digest": d1, "layer": "code"},
        ])
        code_idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", code_idx_payload)

        # Build data layer index (external entries)
        train_sha = hashlib.sha256(b"ext-train").hexdigest()
        test_sha = hashlib.sha256(b"ext-test").hexdigest()
        data_idx_payload = _layer_index_doc([
            {"path": "data/test.csv", "external": {"uri": "az://bucket/test.csv", "sha256": test_sha, "size": 3}, "layer": "data"},
            {"path": "data/train.csv", "external": {"uri": "az://bucket/train.csv", "sha256": train_sha, "size": 9, "tier": "cool"}, "layer": "data"},
        ])
        data_idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", data_idx_payload)

        resolved = _mk_resolved_with_indexes(code_idx_digest, data_idx_digest)

        # Get entries (provider doesn't sort, but we can sort for testing)
        entries = sorted(provider.iter_entries(resolved, ["code", "data"]), key=lambda e: e.path)

        # Should contain both kinds
        assert len(entries) == 4
        assert [e.path for e in entries] == [
            "data/test.csv", "data/train.csv", "src/model.py", "src/utils.py"
        ]
        
        # Check external entries
        ext_entries = [e for e in entries if e.kind == "external"]
        assert len(ext_entries) == 2
        assert all(e.content is None for e in ext_entries)
        
        train_entry = next(e for e in ext_entries if "train" in e.path)
        assert train_entry.uri == "az://bucket/train.csv"
        assert train_entry.size == 9
        assert train_entry.tier == "cool"
        
        # Check ORAS entries
        oras_entries = [e for e in entries if e.kind == "oras"]
        assert len(oras_entries) == 2
        assert all(e.content is not None for e in oras_entries)
        
        model_entry = next(e for e in oras_entries if "model" in e.path)
        assert model_entry.content == code_file_1

    def test_missing_layer_index_raises(self):
        """Test that missing layer in layer_indexes raises clear error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        # Only code index exists; 'data' is missing from layer_indexes
        code_idx_payload = _layer_index_doc([])
        code_idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", code_idx_payload)
        resolved = _mk_resolved_with_indexes(code_idx_digest, None)

        with pytest.raises(ValueError, match="resolved missing index for layer 'data'"):
            list(provider.iter_entries(resolved, ["data"]))

    def test_missing_index_manifest_raises(self):
        """Test that missing index manifest in ORAS raises clear error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))
        
        # Reference a digest that doesn't exist in ORAS
        fake_digest = "sha256:" + "b" * 64
        resolved = _mk_resolved_with_indexes(fake_digest, None)
        
        with pytest.raises(ValueError, match="missing index manifest sha256:bbbbbbbbbbb\\.\\.\\. for layer 'code'"):
            list(provider.iter_entries(resolved, ["code"]))

    def test_invalid_media_type_raises(self):
        """Test that wrong mediaType in index raises clear error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        # Create an index with wrong mediaType
        bad_payload = json.dumps({
            "mediaType": "application/vnd.wrong+json", 
            "entries": []
        }).encode()
        bad_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", bad_payload)
        resolved = _mk_resolved_with_indexes(bad_digest, None)

        with pytest.raises(ValueError, match="invalid mediaType for layer 'code': expected"):
            list(provider.iter_entries(resolved, ["code"]))

    def test_invalid_json_raises(self):
        """Test that malformed JSON in index raises clear error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()  
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))
        
        # Store malformed JSON as blob
        bad_json = b"{ invalid json }"
        # Manually store in fake (bypassing put_blob validation)
        fake_digest = "sha256:" + hashlib.sha256(bad_json).hexdigest()
        oras._blobs.setdefault("testns/bundles/test-bundle", {})[fake_digest] = bad_json
        
        resolved = _mk_resolved_with_indexes(fake_digest, None)
        
        with pytest.raises(ValueError, match="invalid JSON in index for layer 'code'"):
            list(provider.iter_entries(resolved, ["code"]))

    def test_entry_missing_path_raises(self):
        """Test that entry without path raises clear error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        malformed_payload = _layer_index_doc([
            {"digest": "sha256:" + "c"*64, "layer": "code"}  # missing path
        ])
        idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", malformed_payload)
        resolved = _mk_resolved_with_indexes(idx_digest, None)

        with pytest.raises(ValueError, match="entry missing 'path' in layer 'code'"):
            list(provider.iter_entries(resolved, ["code"]))

    def test_entry_layer_mismatch_raises(self):
        """Test that entry with wrong layer field raises error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        malformed_payload = _layer_index_doc([
            {"path": "src/model.py", "digest": "sha256:" + "c"*64, "layer": "wrong"}
        ])
        idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", malformed_payload)
        resolved = _mk_resolved_with_indexes(idx_digest, None)

        with pytest.raises(ValueError, match="entry layer mismatch in 'code': entry says 'wrong'"):
            list(provider.iter_entries(resolved, ["code"]))

    def test_entry_missing_both_digest_and_external_raises(self):
        """Test that entry missing both digest and external raises error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        malformed_payload = _layer_index_doc([
            {"path": "oops/no_source.txt", "layer": "code"}
        ])
        idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", malformed_payload)
        resolved = _mk_resolved_with_indexes(idx_digest, None)

        with pytest.raises(ValueError, match="entry must have exactly one of 'digest' or 'external' for path 'oops/no_source.txt'"):
            list(provider.iter_entries(resolved, ["code"]))

    def test_entry_has_both_digest_and_external_raises(self):
        """Test that entry with both digest and external raises error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        malformed_payload = _layer_index_doc([
            {
                "path": "conflicted/file.txt", 
                "layer": "code",
                "digest": "sha256:" + "d"*64,
                "external": {"uri": "az://bucket/file.txt", "sha256": "c"*64, "size": 100}
            }
        ])
        idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", malformed_payload)
        resolved = _mk_resolved_with_indexes(idx_digest, None)

        with pytest.raises(ValueError, match="entry must have exactly one of 'digest' or 'external' for path 'conflicted/file.txt'"):
            list(provider.iter_entries(resolved, ["code"]))

    def test_missing_oras_blob_raises(self):
        """Test that missing ORAS blob raises friendly error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        # Reference a digest that doesn't exist in ORAS blobs
        missing_digest = "sha256:" + "f"*64
        idx_payload = _layer_index_doc([
            {"path": "src/missing.py", "digest": missing_digest, "layer": "code"}
        ])
        idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", idx_payload)
        resolved = _mk_resolved_with_indexes(idx_digest, None)

        with pytest.raises(ValueError, match="missing blob sha256:fffffffffff\\.\\.\\. for layer 'code' path 'src/missing.py'"):
            list(provider.iter_entries(resolved, ["code"]))

    def test_external_entry_missing_required_fields_raises(self):
        """Test that external entry missing uri/sha256/size raises error."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        # Missing uri and size
        malformed_payload = _layer_index_doc([
            {
                "path": "data/incomplete.csv", 
                "layer": "data",
                "external": {"sha256": "b"*64}  # missing uri and size
            }
        ])
        idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", malformed_payload)
        resolved = _mk_resolved_with_indexes(None, idx_digest)

        with pytest.raises(ValueError, match="external entry missing fields \\['uri', 'size'\\] for path 'data/incomplete.csv' in layer 'data'"):
            list(provider.iter_entries(resolved, ["data"]))

    def test_external_tier_optional(self):
        """Test that external tier field is optional."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))

        # External without tier
        payload = _layer_index_doc([
            {
                "path": "data/no_tier.csv",
                "layer": "data", 
                "external": {"uri": "az://bucket/no_tier.csv", "sha256": "a"*64, "size": 50}
            }
        ])
        idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", payload)
        resolved = _mk_resolved_with_indexes(None, idx_digest)

        entries = list(provider.iter_entries(resolved, ["data"]))
        assert len(entries) == 1
        assert entries[0].tier is None

    def test_external_sha_format_enforced_by_matentry(self):
        """Test that provider propagates bad SHA256 that gets caught by MatEntry validation."""
        oras = FakeOciRegistry()
        external = FakeExternalStore()
        provider = BundleContentProvider(registry=oras, external=external, settings=Settings(registry_url="http://localhost:5000", registry_repo="testns"))
        
        # Invalid SHA256 (not hex)
        bad_sha = "zzzz" * 16  # 64 chars but not hex
        payload = _layer_index_doc([
            {
                "path": "data/bad.csv", 
                "layer": "data",
                "external": {"uri": "az://bucket/bad.csv", "sha256": bad_sha, "size": 1}
            }
        ])
        idx_digest = _put_layer_index_as_blob(oras, "testns/bundles/test-bundle", payload)
        resolved = _mk_resolved_with_indexes(None, idx_digest)
        
        with pytest.raises(ValueError, match="External sha256 must be 64 hex chars"):
            list(provider.iter_entries(resolved, ["data"]))