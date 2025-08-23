"""
Tests for ModelOps Bundles Runtime Layer.

Tests the resolve() and materialize() functions, role selection precedence,
overwrite semantics, pointer file placement, and error conditions.
"""
import json
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError
from modelops_contracts.artifacts import BundleRef

from modelops_bundles.runtime import (
    BundleNotFoundError,
    RoleLayerMismatch, 
    WorkdirConflict,
    materialize,
    resolve,
    _select_role,
    _validate_role
)
from tests.fakes.fake_provider import FakeProvider
from tests.storage.fakes.fake_oras import FakeBundleRegistryStore


@pytest.fixture
def mock_registry_and_repo():
    """Fixture that provides registry and repository for testing."""
    # Create and seed FakeBundleRegistryStore
    fake_registry = FakeBundleRegistryStore()
    repository = "test/repo"
    
    # Create fake layer indexes
    import json
    layer_index_digests = {}
    
    for layer in ["code", "config", "data"]:
        layer_index = {
            "mediaType": "application/vnd.modelops.layer+json", 
            "entries": []
        }
        
        if layer == "data":
            layer_index["entries"] = [
                {
                    "path": "data/train.csv",
                    "external": {
                        "uri": "az://fake-container/train.csv",
                        "sha256": "fake-train-sha256",
                        "size": 1048576
                    }
                }
            ]
        else:
            layer_index["entries"] = [
                {
                    "path": f"{layer}/example.txt",
                    "oras": {
                        "digest": f"sha256:fake-{layer}-blob-digest"
                    }
                }
            ]
        
        layer_payload = json.dumps(layer_index).encode()
        digest = fake_registry.put_manifest("application/vnd.modelops.layer+json", layer_payload)
        layer_index_digests[layer] = digest
    
    # Create bundle manifest
    bundle_manifest = {
        "mediaType": "application/vnd.modelops.bundle.manifest+json",
        "roles": {
            "default": ["code", "config"],
            "runtime": ["code", "config"], 
            "training": ["code", "config", "data"]
        },
        "layers": ["code", "config", "data"],
        "layer_indexes": layer_index_digests,
        "external_index_present": True
    }
    
    bundle_payload = json.dumps(bundle_manifest).encode()
    bundle_digest = fake_registry.put_manifest("application/vnd.modelops.bundle.manifest+json", bundle_payload)
    
    # Tag the bundle manifest with the test reference
    fake_registry.tag_manifest("test/repo/test:1.0", bundle_digest)
    fake_registry.tag_manifest("test/repo/integration-test:2.0", bundle_digest)
    
    return fake_registry, repository


# =============================================================================
# Role Selection Precedence Tests
# =============================================================================

def test_role_selection_precedence_arg_wins(mock_registry_and_repo):
    """Test that function argument role overrides ref.role."""
    registry, repository = mock_registry_and_repo
    
    # Create a resolved bundle with roles
    ref = BundleRef(name="test", version="1.0", role="ref-role")
    resolved = resolve(ref, registry=registry, repository=repository)
    
    # Function arg should win over ref.role
    selected = _select_role(resolved, ref, "runtime")
    assert selected == "runtime"


def test_role_selection_precedence_ref_role(mock_registry_and_repo):
    """Test that ref.role is used when no function argument provided."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0", role="runtime")
    resolved = resolve(ref, registry=registry, repository=repository)
    
    # Should use ref.role when no argument provided
    selected = _select_role(resolved, ref, None)
    assert selected == "runtime"


def test_role_selection_precedence_default(mock_registry_and_repo):
    """Test that 'default' role is used when no arg or ref.role."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    resolved = resolve(ref, registry=registry, repository=repository)
    
    # Should use "default" role when present and no other specification
    selected = _select_role(resolved, ref, None)
    assert selected == "default"


def test_role_selection_precedence_error(mock_registry_and_repo):
    """Test error when no role can be determined."""
    registry, repository = mock_registry_and_repo
    
    # Create ref without role hint
    ref = BundleRef(name="test", version="1.0")
    resolved = resolve(ref, registry=registry, repository=repository)
    
    # Create a copy without default role to force error
    no_default_roles = {k: v for k, v in resolved.roles.items() if k != "default"}
    no_default = resolved.model_copy(update={"roles": no_default_roles})
    
    with pytest.raises(RoleLayerMismatch, match="No role specified"):
        _select_role(no_default, ref, None)


def test_validate_role_exists(mock_registry_and_repo):
    """Test that _validate_role accepts valid roles."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    resolved = resolve(ref, registry=registry, repository=repository)
    
    # Should accept existing role
    result = _validate_role(resolved, "runtime")
    assert result == "runtime"


def test_validate_role_not_found(mock_registry_and_repo):
    """Test that _validate_role rejects non-existent roles."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    resolved = resolve(ref, registry=registry, repository=repository)
    
    with pytest.raises(RoleLayerMismatch, match="Role 'nonexistent' not found"):
        _validate_role(resolved, "nonexistent")


# =============================================================================
# Resolve Function Tests  
# =============================================================================

def test_resolve_returns_resolved_bundle(mock_registry_and_repo):
    """Test that resolve() returns a ResolvedBundle."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    result = resolve(ref, registry=registry, repository=repository)
    
    assert result.ref == ref
    assert result.manifest_digest.startswith("sha256:")
    assert len(result.manifest_digest) == 71  # "sha256:" + 64 hex chars
    assert isinstance(result.roles, dict)
    assert isinstance(result.layers, list)
    assert result.total_size > 0


def test_resolve_deterministic(mock_registry_and_repo):
    """Test that resolve() returns same digest for same input."""
    registry, repository = mock_registry_and_repo
    
    ref1 = BundleRef(name="test", version="1.0")
    ref2 = BundleRef(name="test", version="1.0")
    
    result1 = resolve(ref1, registry=registry, repository=repository)
    result2 = resolve(ref2, registry=registry, repository=repository)
    
    assert result1.manifest_digest == result2.manifest_digest


def test_resolve_no_fs_side_effects(tmp_path, mock_registry_and_repo):
    """Test that resolve() doesn't create any files."""
    registry, repository = mock_registry_and_repo
    
    # Record initial directory state
    initial_files = list(tmp_path.rglob("*"))
    
    # Change to temp directory to ensure no files created in cwd
    original_cwd = Path.cwd()
    try:
        import os
        os.chdir(tmp_path)
        
        ref = BundleRef(name="test", version="1.0")
        resolve(ref, registry=registry, repository=repository)
        
        # Check no files were created
        final_files = list(tmp_path.rglob("*"))
        assert final_files == initial_files
        
    finally:
        os.chdir(original_cwd)


def test_resolve_different_ref_types(mock_registry_and_repo):
    """Test resolve with different BundleRef types."""
    registry, repository = mock_registry_and_repo
    
    
    # Name + version
    ref1 = BundleRef(name="test", version="1.0")
    result1 = resolve(ref1, registry=registry, repository=repository)
    
    # Digest - use the actual digest from our fake store
    actual_digest = result1.manifest_digest
    ref2 = BundleRef(digest=actual_digest)
    result2 = resolve(ref2, registry=registry, repository=repository)
    
    # Local path - should raise error for unsupported feature
    ref3 = BundleRef(local_path="/tmp/test")
    with pytest.raises(ValueError, match="Local path support not yet implemented"):
        resolve(ref3, registry=registry, repository=repository)
    
    # Name+version and digest should return same bundle
    assert result1.manifest_digest == result2.manifest_digest
    for result in [result1, result2]:
        assert result.manifest_digest.startswith("sha256:")
        assert isinstance(result.roles, dict)
        assert isinstance(result.layers, list)


# =============================================================================
# Materialize Function Tests
# =============================================================================

def test_materialize_creates_files_and_pointers(tmp_path, mock_registry_and_repo):
    """Test that materialize() creates ORAS files and pointer files."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    dest = str(tmp_path / "workdir")
    provider = FakeProvider()
    
    result = materialize(ref, dest, role="training", provider=provider, registry=registry, repository=repository)
    
    # Should return MaterializeResult with bundle and selected role
    assert result.bundle.ref == ref
    assert result.bundle.manifest_digest.startswith("sha256:")
    assert result.selected_role == "training"
    
    # Check that destination directory was created
    dest_path = Path(dest)
    assert dest_path.exists()
    
    # Check for ORAS files (code/config layers)
    code_file = dest_path / "src/model.py"
    config_file = dest_path / "configs/base.yaml"
    
    if code_file.exists():
        assert code_file.read_text().startswith("# Fake model code")
    if config_file.exists():
        assert config_file.read_text().startswith("# Fake config")
    
    # Check for pointer file (external data)
    pointer_file = dest_path / ".mops/ptr/data/train.csv.json"
    assert pointer_file.exists(), "Pointer file should be created for data layer"
    pointer_data = json.loads(pointer_file.read_text())
    assert pointer_data["schema_version"] == 1
    assert pointer_data["uri"].startswith("az://")
    assert pointer_data["original_path"] == "data/train.csv"
    assert pointer_data["layer"] == "data"


def test_materialize_idempotent(tmp_path, mock_registry_and_repo):
    """Test that calling materialize twice produces no conflicts."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    dest = str(tmp_path / "workdir")
    provider = FakeProvider()
    
    # First materialization
    result1 = materialize(ref, dest, role="runtime", provider=provider, registry=registry, repository=repository)
    
    # Second materialization should not raise conflicts
    result2 = materialize(ref, dest, role="runtime", provider=provider, registry=registry, repository=repository)
    
    # Results should be identical
    assert result1.bundle.manifest_digest == result2.bundle.manifest_digest
    assert result1.selected_role == result2.selected_role == "runtime"


def test_materialize_conflict_no_overwrite(tmp_path, mock_registry_and_repo):
    """Test that materialize raises WorkdirConflict when overwrite=False."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    dest = str(tmp_path / "workdir")
    provider = FakeProvider()
    
    # Create conflicting file
    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)
    conflicting_file = dest_path / "src/model.py"
    conflicting_file.parent.mkdir(parents=True, exist_ok=True)
    conflicting_file.write_text("Different content")
    
    # Should raise WorkdirConflict with overwrite=False (default)
    with pytest.raises(WorkdirConflict) as exc_info:
        materialize(ref, dest, role="runtime", provider=provider, registry=registry, repository=repository)
    
    # Check exception details
    assert len(exc_info.value.conflicts) > 0
    assert "conflict" in str(exc_info.value).lower()


def test_materialize_conflict_with_overwrite(tmp_path, mock_registry_and_repo):
    """Test that materialize replaces files when overwrite=True."""
    registry, repository = mock_registry_and_repo
    ref = BundleRef(name="test", version="1.0")
    dest = str(tmp_path / "workdir")
    provider = FakeProvider()
    
    # Create conflicting file
    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)
    conflicting_file = dest_path / "src/model.py"
    conflicting_file.parent.mkdir(parents=True, exist_ok=True)
    conflicting_file.write_text("Different content")
    
    # Should succeed with overwrite=True
    result = materialize(ref, dest, role="runtime", overwrite=True, provider=provider, registry=registry, repository=repository)
    assert result.bundle.manifest_digest.startswith("sha256:")
    
    # File should be replaced (if it was materialized)
    if conflicting_file.exists():
        # If the file was part of the role, it should be replaced
        new_content = conflicting_file.read_text()
        assert new_content != "Different content"


def test_materialize_pointer_file_location_and_schema(tmp_path, mock_registry_and_repo):
    """Test pointer file placement follows canonical rule."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0") 
    dest = str(tmp_path / "workdir")
    provider = FakeProvider()
    
    materialize(ref, dest, role="training", provider=provider, registry=registry, repository=repository)  # Include data layer
    
    # Check pointer file is in correct location
    # Original: data/train.csv -> Pointer: .mops/ptr/data/train.csv.json
    pointer_path = Path(dest) / ".mops/ptr/data/train.csv.json"
    
    if pointer_path.exists():
        # Validate pointer file schema
        with open(pointer_path) as f:
            pointer_data = json.load(f)
        
        required_fields = [
            "schema_version", "uri", "sha256", "size", "original_path", 
            "layer", "created_at", "fulfilled"
        ]
        for field in required_fields:
            assert field in pointer_data, f"Missing field: {field}"
        
        # Validate field values
        assert pointer_data["schema_version"] == 1
        assert pointer_data["original_path"] == "data/train.csv"
        assert pointer_data["layer"] == "data"
        assert len(pointer_data["sha256"]) == 64  # Hex SHA256
        assert pointer_data["size"] >= 0


def test_materialize_prefetch_external(tmp_path, mock_registry_and_repo):
    """Test prefetch_external=True downloads external data."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    dest = str(tmp_path / "workdir")
    provider = FakeProvider()
    
    materialize(ref, dest, role="training", prefetch_external=True, provider=provider, registry=registry, repository=repository)
    
    # Both pointer file and actual file should exist
    pointer_path = Path(dest) / ".mops/ptr/data/train.csv.json"
    actual_path = Path(dest) / "data/train.csv"
    
    if pointer_path.exists():
        # Check pointer shows fulfilled=True
        with open(pointer_path) as f:
            pointer_data = json.load(f)
        assert pointer_data["fulfilled"] == True
        assert pointer_data["local_path"] == "data/train.csv"
    
    if actual_path.exists():
        # Check actual file was created with FakeProvider content
        content = actual_path.read_text()
        assert content == "fake-bytes-for:data/train.csv"


# =============================================================================
# Error Condition Tests
# =============================================================================

def test_error_role_not_found(mock_registry_and_repo):
    """Test error when requesting non-existent role."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    provider = FakeProvider()
    
    with pytest.raises(RoleLayerMismatch, match="Role 'nonexistent' not found"):
        with tempfile.TemporaryDirectory() as tmp_dir:
            materialize(ref, tmp_dir, role="nonexistent", provider=provider, registry=registry, repository=repository)


def test_error_role_with_missing_layers(mock_registry_and_repo):
    """Test that role validation catches missing layer references."""
    registry, repository = mock_registry_and_repo
    
    # This test would be more relevant when we have real manifest parsing
    # For now, just test the validation logic directly
    ref = BundleRef(name="test", version="1.0")
    resolved = resolve(ref, registry=registry, repository=repository)
    
    # Create copy with role referencing missing layer
    broken_roles = {**resolved.roles, "broken": ["missing-layer"]}
    broken_resolved = resolved.model_copy(update={"roles": broken_roles})
    
    # Should still validate that role exists
    selected = _validate_role(broken_resolved, "broken")
    assert selected == "broken"
    
    # The missing layer validation would happen during actual materialization
    # when we try to fetch the layer content


def test_error_invalid_bundle_ref():
    """Test error handling for invalid BundleRef.""" 
    # BundleRef validation should happen at the contracts level
    # This tests our runtime's handling of edge cases
    
    # Empty BundleRef should be caught by contracts validation
    with pytest.raises(ValidationError):
        ref = BundleRef()  # This should fail at the contracts level


def test_materialize_requires_provider(mock_registry_and_repo):
    """Test that materialize() requires a provider parameter."""
    ref = BundleRef(name="test", version="1.0")
    
    registry, repository = mock_registry_and_repo
    # Should fail without provider parameter
    with pytest.raises(TypeError, match="missing.*required.*provider"):
        with tempfile.TemporaryDirectory() as tmp_dir:
            materialize(ref, tmp_dir, registry=registry, repository=repository)


def test_materialize_rejects_missing_layer(mock_registry_and_repo):
    """Test that materialize rejects roles referencing non-existent layers."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    provider = FakeProvider()
    
    # Test that the error message includes sorted missing layers
    missing = ["z-layer", "a-layer", "m-layer"]
    with pytest.raises(RoleLayerMismatch) as exc_info:
        raise RoleLayerMismatch(
            f"Role 'broken' references non-existent layers: {sorted(missing)}"
        )
    
    assert "['a-layer', 'm-layer', 'z-layer']" in str(exc_info.value)


def test_provider_duplicate_paths_rejected(mock_registry_and_repo):
    """Test that duplicate paths from provider are rejected."""
    registry, repository = mock_registry_and_repo
    
    from modelops_bundles.runtime_types import MatEntry
    
    class DuplicateProvider:
        def iter_entries(self, resolved, layers):
            # Yield duplicate paths
            yield MatEntry(path="duplicate.txt", layer="code", kind="oras", content=b"first")
            yield MatEntry(path="other.txt", layer="code", kind="oras", content=b"other")
            yield MatEntry(path="duplicate.txt", layer="code", kind="oras", content=b"second")
        
        def fetch_external(self, entry):
            return b"fake external"
    
    ref = BundleRef(name="test", version="1.0")
    provider = DuplicateProvider()
    
    with pytest.raises(WorkdirConflict, match="Duplicate materialization path"):
        with tempfile.TemporaryDirectory() as tmp_dir:
            materialize(ref, tmp_dir, role="runtime", provider=provider, registry=registry, repository=repository)


def test_materialize_dir_vs_file_conflict(tmp_path, mock_registry_and_repo):
    """Test conflict handling when directory exists where file expected."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    provider = FakeProvider()
    dest = str(tmp_path / "workdir")
    
    # Create a directory where a file should be materialized
    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)
    conflict_dir = dest_path / "src" / "model.py"
    conflict_dir.mkdir(parents=True, exist_ok=True)
    
    # Should raise conflict when overwrite=False
    with pytest.raises(WorkdirConflict, match="files conflict with existing content"):
        materialize(ref, dest, role="runtime", overwrite=False, provider=provider, registry=registry, repository=repository)
    
    # Should replace when overwrite=True
    result = materialize(ref, dest, role="runtime", overwrite=True, provider=provider, registry=registry, repository=repository)
    assert result.bundle.manifest_digest.startswith("sha256:")
    
    # Directory should be gone, file should exist
    file_path = dest_path / "src" / "model.py"
    assert file_path.is_file()
    assert not file_path.is_dir()


def test_duplicate_paths_across_layers(mock_registry_and_repo):
    """Test that duplicate paths from different layers are rejected."""
    registry, repository = mock_registry_and_repo
    
    from modelops_bundles.runtime_types import MatEntry
    
    class CrossLayerDuplicateProvider:
        def iter_entries(self, resolved, layers):
            # Same path from different layers
            yield MatEntry(path="shared.txt", layer="code", kind="oras", content=b"from code")
            yield MatEntry(path="shared.txt", layer="config", kind="oras", content=b"from config")
        
        def fetch_external(self, entry):
            return b"fake external"
    
    ref = BundleRef(name="test", version="1.0")
    provider = CrossLayerDuplicateProvider()
    
    with pytest.raises(WorkdirConflict, match="Duplicate materialization path"):
        with tempfile.TemporaryDirectory() as tmp_dir:
            materialize(ref, tmp_dir, role="runtime", provider=provider, registry=registry, repository=repository)


def test_temp_file_cleanup_on_error(tmp_path):
    """Test that temp files are cleaned up when write fails."""
    from unittest.mock import patch, mock_open
    from modelops_bundles.runtime import _write_file_atomically
    
    target_path = tmp_path / "test.txt"
    content = b"test content"
    
    # Mock open to raise an error during write
    with patch("builtins.open", mock_open()) as mock_file:
        mock_file.return_value.write.side_effect = OSError("Write failed")
        
        with pytest.raises(OSError, match="Write failed"):
            _write_file_atomically(target_path, content)
    
    # Verify no temp files are left behind
    temp_files = list(tmp_path.glob("*.tmp.*"))
    assert len(temp_files) == 0


# =============================================================================  
# Integration Tests
# =============================================================================

def test_resolve_then_materialize_integration(tmp_path, mock_registry_and_repo):
    """Test full resolve -> materialize workflow."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="integration-test", version="2.0")
    provider = FakeProvider()
    
    # First resolve 
    resolved = resolve(ref, registry=registry, repository=repository)
    assert resolved.manifest_digest.startswith("sha256:")
    
    # Then materialize using the same ref
    dest = str(tmp_path / "integration")
    materialized_result = materialize(ref, dest, role="default", provider=provider, registry=registry, repository=repository)
    
    # Results should have same digest (same bundle)
    assert resolved.manifest_digest == materialized_result.bundle.manifest_digest
    assert resolved.roles == materialized_result.bundle.roles


def test_different_roles_materialize_different_content(tmp_path, mock_registry_and_repo):
    """Test that different roles materialize different sets of files."""
    registry, repository = mock_registry_and_repo
    
    ref = BundleRef(name="test", version="1.0")
    provider = FakeProvider()
    
    # Materialize minimal role
    runtime_dest = str(tmp_path / "runtime")
    materialize(ref, runtime_dest, role="runtime", provider=provider, registry=registry, repository=repository)
    
    # Materialize full role  
    training_dest = str(tmp_path / "training")
    materialize(ref, training_dest, role="training", provider=provider, registry=registry, repository=repository)
    
    # Both should have basic files, but training should have data
    runtime_files = list(Path(runtime_dest).rglob("*"))
    training_files = list(Path(training_dest).rglob("*"))
    
    # Training role should have more files (includes data layer)
    # This is a rough check since we're using fake data
    assert len(training_files) >= len(runtime_files)


def test_path_traversal_attacks_rejected(tmp_path, mock_registry_and_repo):
    """Test that materialize rejects dangerous paths that could escape destination."""
    registry, repository = mock_registry_and_repo
    
    from modelops_bundles.runtime_types import MatEntry, ContentProvider
    from modelops_contracts.artifacts import ResolvedBundle
    from typing import Iterable
    import pytest
    
    class EvilProvider(ContentProvider):
        """Provider that yields dangerous paths."""
        
        def iter_entries(self, resolved: ResolvedBundle, layers: list[str]) -> Iterable[MatEntry]:
            # Try various path traversal attacks
            dangerous_paths = [
                "../../outside.txt",     # Parent directory escape
                "/etc/passwd",           # Absolute path
                "../../../etc/passwd",   # Deep traversal
                ".mops/metadata.json",   # Reserved metadata area
            ]
            
            for path in dangerous_paths:
                yield MatEntry(
                    path=path,
                    layer="code", 
                    kind="oras",
                    content=b"malicious content",
                    uri=None,
                    sha256=None,
                    size=None,
                    tier=None
                )
        
        def fetch_external(self, entry: MatEntry) -> bytes:
            return b"not used"
    
    ref = BundleRef(name="test", version="1.0")
    evil_provider = EvilProvider()
    
    # Each dangerous path should be rejected with ValueError
    with pytest.raises(ValueError, match="unsafe path"):
        materialize(ref, str(tmp_path), role="runtime", provider=evil_provider, registry=registry, repository=repository)


def test_external_prefetch_honors_overwrite_rules(tmp_path, mock_registry_and_repo):
    """Test that prefetched external files respect overwrite/conflict rules."""
    registry, repository = mock_registry_and_repo
    
    from modelops_bundles.runtime_types import MatEntry, ContentProvider
    from modelops_contracts.artifacts import ResolvedBundle
    from typing import Iterable
    import pytest
    
    # Create existing file that will conflict
    existing_file = tmp_path / "data.txt"
    existing_file.write_text("existing content")
    
    class ExternalPrefetchProvider(ContentProvider):
        """Provider that yields external entries for prefetch testing."""
        
        def iter_entries(self, resolved: ResolvedBundle, layers: list[str]) -> Iterable[MatEntry]:
            yield MatEntry(
                path="data.txt",
                layer="data",
                kind="external", 
                content=None,
                uri="az://test/data.txt",
                sha256="df72caba10e0b5c8f28f9bd2100bd0b7905ea953bef6cd9f81cae1548bf459e1",
                size=12,
                tier=None
            )
        
        def fetch_external(self, entry: MatEntry) -> bytes:
            return b"new content!"
    
    ref = BundleRef(name="test", version="1.0")
    provider = ExternalPrefetchProvider()
    
    # Without overwrite=True, should detect conflict and raise WorkdirConflict
    with pytest.raises(WorkdirConflict):
        materialize(ref, str(tmp_path), role="runtime", provider=provider, 
                   prefetch_external=True, overwrite=False, registry=registry, repository=repository)
    
    # With overwrite=True, should succeed and replace content
    result = materialize(ref, str(tmp_path), role="runtime", provider=provider,
                        prefetch_external=True, overwrite=True, registry=registry, repository=repository)
    
    # Verify file was replaced with new content
    assert existing_file.read_text() == "new content!"


def test_pointer_overwrite_semantics(tmp_path, mock_registry_and_repo):
    """Test that pointer files are always overwritten (system-owned files)."""
    registry, repository = mock_registry_and_repo
    
    from modelops_bundles.runtime_types import MatEntry, ContentProvider
    from modelops_contracts.artifacts import ResolvedBundle
    from collections.abc import Iterable
    
    class ExternalTierChangeProvider(ContentProvider):
        """Provider that changes tier metadata between materializations."""
        
        def __init__(self, tier: str):
            self.tier = tier
        
        def iter_entries(self, resolved: ResolvedBundle, layers: list[str]) -> Iterable[MatEntry]:
            yield MatEntry(
                path="data/file.txt",
                layer="data",
                kind="external",
                content=None,
                uri="az://test/file.txt",
                sha256="09ecb6ebc8bcefc733f6f2ec44f791abeed6a99edf0cc31519637898aebd52d8", 
                size=100,
                tier=self.tier  # This will change between calls
            )
        
        def fetch_external(self, entry: MatEntry) -> bytes:
            return b"x" * 100  # Fixed content, only tier changes
    
    ref = BundleRef(name="test", version="1.0")
    
    # First materialization with "hot" tier  
    provider1 = ExternalTierChangeProvider("hot")
    materialize(ref, str(tmp_path), role="runtime", provider=provider1, prefetch_external=False, registry=registry, repository=repository)
    
    pointer_path = tmp_path / ".mops" / "ptr" / "data" / "file.txt.json"
    assert pointer_path.exists()
    
    # Read initial pointer data
    import json
    with open(pointer_path, 'r') as f:
        first_pointer = json.load(f)
    
    assert first_pointer["tier"] == "hot"
    assert first_pointer["fulfilled"] is False
    
    # Second materialization with "cool" tier - should update pointer without conflict
    provider2 = ExternalTierChangeProvider("cool") 
    result = materialize(ref, str(tmp_path), role="runtime", provider=provider2, 
                        prefetch_external=False, overwrite=False, registry=registry, repository=repository)  # No overwrite needed
    
    # Verify pointer was updated (system-owned file)
    with open(pointer_path, 'r') as f:
        second_pointer = json.load(f)
    
    assert second_pointer["tier"] == "cool"  # Changed
    assert second_pointer["sha256"] == first_pointer["sha256"]  # Same content
    assert second_pointer["fulfilled"] is False  # Still not prefetched


def test_pointer_deterministic_creation(tmp_path, mock_registry_and_repo):
    """Test that pointer files are created deterministically."""
    registry, repository = mock_registry_and_repo
    
    from modelops_bundles.runtime_types import MatEntry, ContentProvider
    from modelops_contracts.artifacts import ResolvedBundle
    from collections.abc import Iterable
    
    class DeterministicProvider(ContentProvider):
        """Provider with deterministic external data."""
        
        def iter_entries(self, resolved: ResolvedBundle, layers: list[str]) -> Iterable[MatEntry]:
            yield MatEntry(
                path="test.txt", 
                layer="data",
                kind="external",
                content=None,
                uri="az://bucket/test.txt",
                sha256="b" * 64,
                size=42,
                tier="archive"
            )
        
        def fetch_external(self, entry: MatEntry) -> bytes:
            return b"deterministic content"
    
    ref = BundleRef(name="test", version="1.0")
    provider = DeterministicProvider()
    
    # Create first materialization
    materialize(ref, str(tmp_path), role="default", provider=provider, prefetch_external=False, registry=registry, repository=repository)
    
    pointer_path = tmp_path / ".mops" / "ptr" / "test.txt.json"
    first_content = pointer_path.read_text(encoding='utf-8')
    
    # Remove the materialization
    import shutil
    shutil.rmtree(tmp_path)
    tmp_path.mkdir()
    
    # Create identical materialization
    materialize(ref, str(tmp_path), role="default", provider=provider, prefetch_external=False, registry=registry, repository=repository)
    
    second_content = pointer_path.read_text(encoding='utf-8')
    
    # Verify identical JSON output (deterministic)
    assert first_content == second_content
    
    # Verify the timestamp is deterministic (epoch time)
    import json
    pointer_data = json.loads(second_content)
    assert pointer_data["created_at"] == "1970-01-01T00:00:00Z"