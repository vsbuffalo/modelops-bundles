"""
Integration tests for runtime with OrasExternalProvider.

These tests verify the full end-to-end workflow from ResolvedBundle with
layer_indexes through to materialized files and pointer files.
"""
from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

from modelops_contracts.artifacts import ResolvedBundle, BundleRef, LAYER_INDEX
from modelops_bundles.providers.oras_external import OrasExternalProvider
from modelops_bundles.storage.fakes.fake_oras import FakeOrasStore
from modelops_bundles.storage.fakes.fake_external import FakeExternalStore
from modelops_bundles.runtime import materialize, WorkdirConflict


def _layer_index_doc(entries):
    """Create layer index document with correct format."""
    return json.dumps({
        "mediaType": LAYER_INDEX, 
        "entries": entries
    }, separators=(",", ":"), sort_keys=True).encode()


def _mk_resolved(ref_name="stage3", roles=None, layers=None, layer_indexes=None) -> ResolvedBundle:
    """Create ResolvedBundle for testing."""
    ref = BundleRef(name=ref_name, version="1.0.0")
    return ResolvedBundle(
        ref=ref,
        manifest_digest="sha256:" + "b"*64,
        roles=roles or {"runtime": ["code", "config"], "training": ["code", "config", "data"], "default": ["code"]},
        layers=layers or ["code", "config", "data"],
        external_index_present=True,
        total_size=42,
        cache_dir=None,
        layer_indexes=layer_indexes or {},
    )


class TestRuntimeWithOrasExternal:
    """Test full integration of runtime with OrasExternalProvider."""
    
    def test_materialize_with_real_provider_oras_and_external(self, tmp_path):
        """Test full materialize workflow with ORAS files and external pointers."""
        oras = FakeOrasStore()
        external = FakeExternalStore()
        provider = OrasExternalProvider(oras=oras, external=external)

        # Seed ORAS blobs + indexes
        code_py = b"# Python model code\nprint('hello world')\n"
        cfg_yaml = b"model:\n  type: test\n  version: 1.0\n"
        d_code = "sha256:" + hashlib.sha256(code_py).hexdigest()
        d_cfg = "sha256:" + hashlib.sha256(cfg_yaml).hexdigest()
        oras.put_blob(d_code, code_py)
        oras.put_blob(d_cfg, cfg_yaml)

        # Create layer indexes
        code_idx = oras.put_manifest(LAYER_INDEX, _layer_index_doc([
            {"path": "src/model.py", "digest": d_code, "layer": "code"},
        ]))
        config_idx = oras.put_manifest(LAYER_INDEX, _layer_index_doc([
            {"path": "configs/base.yaml", "digest": d_cfg, "layer": "config"},
        ]))

        # External data files
        train_sha = hashlib.sha256(b"train-data-bytes").hexdigest()
        test_sha = hashlib.sha256(b"test-data-bytes").hexdigest()
        data_idx = oras.put_manifest(LAYER_INDEX, _layer_index_doc([
            {
                "path": "data/train.csv", 
                "external": {
                    "uri": "az://container/train.csv", 
                    "sha256": train_sha, 
                    "size": 17,  # len(b"train-data-bytes")
                    "tier": "cool"
                }, 
                "layer": "data"
            },
            {
                "path": "data/test.csv", 
                "external": {
                    "uri": "az://container/test.csv",  
                    "sha256": test_sha, 
                    "size": 16,  # len(b"test-data-bytes") 
                }, 
                "layer": "data"
            },
        ]))

        resolved = _mk_resolved(
            roles={"runtime": ["code", "config"], "training": ["code", "config", "data"]},
            layers=["code", "config", "data"],
            layer_indexes={"code": code_idx, "config": config_idx, "data": data_idx},
        )

        # Mock resolve to return our resolved bundle
        import modelops_bundles.runtime as rt
        original_resolve = rt.resolve
        rt.resolve = lambda ref, cache=True: resolved
        
        try:
            # Materialize training role (includes data -> creates pointers)
            dest = str(tmp_path / "training_workspace")
            result = materialize(
                BundleRef(name="stage3", version="1.0.0"), 
                dest, 
                role="training", 
                provider=provider, 
                prefetch_external=False
            )

            # Check that resolve result is returned
            assert result == resolved

            # ORAS files should be written
            dest_path = Path(dest)
            assert (dest_path / "src/model.py").read_bytes() == code_py
            assert (dest_path / "configs/base.yaml").read_bytes() == cfg_yaml

            # Pointer files should exist and be unfulfilled
            ptr_train = dest_path / ".mops/ptr/data/train.csv.json"
            ptr_test = dest_path / ".mops/ptr/data/test.csv.json"
            assert ptr_train.exists()
            assert ptr_test.exists()

            # Check pointer file content
            train_pointer = json.loads(ptr_train.read_text())
            assert train_pointer["fulfilled"] is False
            assert train_pointer["original_path"] == "data/train.csv"
            assert train_pointer["layer"] == "data"
            assert train_pointer["uri"] == "az://container/train.csv"
            assert train_pointer["sha256"] == train_sha
            assert train_pointer["size"] == 17
            assert train_pointer["tier"] == "cool"
            assert train_pointer["local_path"] is None

            test_pointer = json.loads(ptr_test.read_text())
            assert test_pointer["fulfilled"] is False
            assert test_pointer.get("tier") is None  # No tier specified

        finally:
            rt.resolve = original_resolve

    def test_materialize_runtime_role_excludes_data(self, tmp_path):
        """Test that runtime role only gets code + config, no data pointers."""
        oras = FakeOrasStore()
        external = FakeExternalStore()
        provider = OrasExternalProvider(oras=oras, external=external)

        # Simple setup with just code layer
        code_py = b"# runtime code only\n"
        d_code = "sha256:" + hashlib.sha256(code_py).hexdigest()
        oras.put_blob(d_code, code_py)

        code_idx = oras.put_manifest(LAYER_INDEX, _layer_index_doc([
            {"path": "src/main.py", "digest": d_code, "layer": "code"},
        ]))

        resolved = _mk_resolved(
            roles={"runtime": ["code"], "training": ["code", "data"]},
            layers=["code", "data"],
            layer_indexes={"code": code_idx, "data": "sha256:" + "unused"*8},  # data index not used
        )

        import modelops_bundles.runtime as rt
        original_resolve = rt.resolve
        rt.resolve = lambda ref, cache=True: resolved
        
        try:
            dest = str(tmp_path / "runtime_workspace")
            materialize(
                BundleRef(name="stage3", version="1.0.0"), 
                dest, 
                role="runtime", 
                provider=provider
            )

            # Should have code file
            dest_path = Path(dest)
            assert (dest_path / "src/main.py").read_bytes() == code_py

            # Should have provenance file but NO pointer files (no data layer requested)
            assert (dest_path / ".mops" / ".mops-manifest.json").exists()
            ptr_dir = dest_path / ".mops" / "ptr"
            assert not ptr_dir.exists() or len(list(ptr_dir.rglob("*.json"))) == 0

        finally:
            rt.resolve = original_resolve

    def test_materialize_prefetch_external_with_conflicts(self, tmp_path):
        """Test prefetch_external=True with conflict detection."""
        oras = FakeOrasStore()
        external = FakeExternalStore()
        provider = OrasExternalProvider(oras=oras, external=external)

        # External file that can be fetched
        external_content = b"actual-external-data"
        external_uri = "az://container/file.bin"
        external_sha = hashlib.sha256(external_content).hexdigest()
        external.put(external_uri, external_content)

        # Index with single external file
        data_idx = oras.put_manifest(LAYER_INDEX, _layer_index_doc([
            {
                "path": "data/file.bin", 
                "external": {
                    "uri": external_uri, 
                    "sha256": external_sha, 
                    "size": len(external_content)
                }, 
                "layer": "data"
            }
        ]))

        resolved = _mk_resolved(
            roles={"runtime": ["data"]},
            layers=["data"],
            layer_indexes={"data": data_idx},
        )

        # Create conflicting existing file
        dest = tmp_path / "prefetch_workspace"
        dest.mkdir()
        (dest / "data").mkdir()
        (dest / "data/file.bin").write_text("conflicting-content")

        import modelops_bundles.runtime as rt
        original_resolve = rt.resolve
        rt.resolve = lambda ref, cache=True: resolved
        
        try:
            # Without overwrite -> should raise WorkdirConflict
            with pytest.raises(WorkdirConflict):
                materialize(
                    BundleRef(name="stage3", version="1.0.0"), 
                    str(dest), 
                    role="runtime",
                    provider=provider, 
                    prefetch_external=True, 
                    overwrite=False
                )

            # With overwrite -> should replace file and set pointer fulfilled
            result = materialize(
                BundleRef(name="stage3", version="1.0.0"), 
                str(dest), 
                role="runtime",
                provider=provider, 
                prefetch_external=True, 
                overwrite=True
            )

            # Check file was replaced
            assert (dest / "data/file.bin").read_bytes() == external_content

            # Check pointer shows fulfilled
            pointer_path = dest / ".mops/ptr/data/file.bin.json"
            assert pointer_path.exists()
            pointer = json.loads(pointer_path.read_text())
            assert pointer["fulfilled"] is True
            assert pointer["local_path"] == "data/file.bin"
            assert pointer["sha256"] == external_sha

        finally:
            rt.resolve = original_resolve

    def test_deterministic_materialization(self, tmp_path):
        """Test that repeated materialization is deterministic and idempotent."""
        oras = FakeOrasStore()
        external = FakeExternalStore() 
        provider = OrasExternalProvider(oras=oras, external=external)

        # Simple mixed content
        code_content = b"# deterministic test\nprint('consistent')\n"
        code_digest = "sha256:" + hashlib.sha256(code_content).hexdigest()
        oras.put_blob(code_digest, code_content)

        code_idx = oras.put_manifest(LAYER_INDEX, _layer_index_doc([
            {"path": "src/app.py", "digest": code_digest, "layer": "code"},
        ]))

        ext_sha = hashlib.sha256(b"external-deterministic").hexdigest()
        data_idx = oras.put_manifest(LAYER_INDEX, _layer_index_doc([
            {
                "path": "data/sample.csv",
                "external": {
                    "uri": "az://bucket/sample.csv",
                    "sha256": ext_sha,
                    "size": 20
                },
                "layer": "data"
            }
        ]))

        resolved = _mk_resolved(
            roles={"test": ["code", "data"]},
            layers=["code", "data"],
            layer_indexes={"code": code_idx, "data": data_idx},
        )

        import modelops_bundles.runtime as rt
        original_resolve = rt.resolve
        rt.resolve = lambda ref, cache=True: resolved

        try:
            dest = str(tmp_path / "deterministic_test")
            
            # First materialization
            result1 = materialize(
                BundleRef(name="stage3", version="1.0.0"),
                dest,
                role="test", 
                provider=provider
            )
            
            # Read results after first run
            code_path = Path(dest) / "src/app.py"
            pointer_path = Path(dest) / ".mops/ptr/data/sample.csv.json"
            
            first_code = code_path.read_bytes()
            first_pointer = pointer_path.read_text()
            
            # Second materialization (should be idempotent)
            result2 = materialize(
                BundleRef(name="stage3", version="1.0.0"),
                dest,
                role="test",
                provider=provider  
            )
            
            # Results should be identical
            assert result1 == result2
            assert code_path.read_bytes() == first_code
            
            # JSON content should be parseable and stable (excluding timestamp)
            pointer1 = json.loads(first_pointer)
            pointer2 = json.loads(pointer_path.read_text())
            
            # Compare all fields except created_at (which will differ)
            for key in pointer1:
                if key != "created_at":
                    assert pointer1[key] == pointer2[key], f"Field '{key}' differs"
                    
            # Both should have created_at field (just different times)
            assert "created_at" in pointer1
            assert "created_at" in pointer2
            
        finally:
            rt.resolve = original_resolve

    def test_reserved_prefix_via_provider_rejected(self, tmp_path):
        """Test that .mops/ path from provider gets rejected by runtime."""
        oras = FakeOrasStore()
        external = FakeExternalStore()
        provider = OrasExternalProvider(oras=oras, external=external)

        # Create malicious index with .mops/ path
        evil_content = b"should not be written to reserved location"
        evil_digest = "sha256:" + hashlib.sha256(evil_content).hexdigest()
        oras.put_blob(evil_digest, evil_content)

        evil_idx = oras.put_manifest(LAYER_INDEX, _layer_index_doc([
            {"path": ".mops/evil.txt", "digest": evil_digest, "layer": "code"},
        ]))

        resolved = _mk_resolved(
            roles={"runtime": ["code"]},
            layers=["code"],
            layer_indexes={"code": evil_idx},
        )

        import modelops_bundles.runtime as rt
        original_resolve = rt.resolve
        rt.resolve = lambda ref, cache=True: resolved
        
        try:
            dest = str(tmp_path / "evil_workspace")
            
            # Should raise ValueError for unsafe path
            with pytest.raises(ValueError, match="unsafe path"):
                materialize(
                    BundleRef(name="evil-bundle", version="1.0.0"), 
                    dest, 
                    role="runtime", 
                    provider=provider
                )

            # Verify no files were created at all
            dest_path = Path(dest)
            if dest_path.exists():
                # If directory was created, it should be empty
                assert not any(dest_path.rglob("*"))

        finally:
            rt.resolve = original_resolve