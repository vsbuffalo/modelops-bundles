"""
ModelOps Bundles Runtime Layer.

This module implements the core resolve() and materialize() functions for
ModelOps Bundles, following the specification for role selection, overwrite
semantics, and pointer file placement.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass

from modelops_contracts.artifacts import BundleRef, ResolvedBundle

from .path_safety import safe_relpath
from .pointer_writer import write_pointer_file
from .runtime_types import ContentProvider, MatEntry
# TYPE_CHECKING import to avoid circular imports  
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .storage.oci_registry import OciRegistry

__all__ = ["resolve", "materialize", "MaterializeResult", "WorkdirConflict", "RoleLayerMismatch", "BundleNotFoundError", "BundleDownloadError", "UnsupportedMediaType"]


@dataclass(frozen=True)
class MaterializeResult:
    """
    Result of bundle materialization with runtime context.
    
    Separates immutable bundle metadata from runtime materialization decisions
    like role selection and destination path.
    """
    bundle: ResolvedBundle
    selected_role: str  
    dest_path: str




# Exception Types  
class BundleDownloadError(Exception):
    """
    Raised when bundle download fails due to network, auth, or registry errors.
    
    This corresponds to exit code 3 in the CLI.
    """
    pass


class UnsupportedMediaType(Exception):
    """
    Raised when media type is not supported or unrecognized.
    
    This corresponds to exit code 10 in the CLI.
    """
    pass


class WorkdirConflict(Exception):
    """
    Raised when materialize() encounters files that conflict with expected content.
    
    This corresponds to exit code 12 in the CLI.
    """
    def __init__(self, message: str, conflicts: List[Dict[str, str]]):
        super().__init__(message)
        self.conflicts = conflicts


class RoleLayerMismatch(Exception):
    """
    Raised when role selection fails or role references non-existent layers.
    
    This corresponds to exit code 11 in the CLI.
    """
    pass


class BundleNotFoundError(Exception):
    """
    Raised when a bundle cannot be found in registry or local path.
    
    This corresponds to exit code 1 in the CLI.
    """
    pass


def resolve(ref: BundleRef, *, registry: 'OciRegistry' = None, settings = None, cache: bool = True) -> ResolvedBundle:
    """
    Resolve bundle identity using repo-aware OCI registry operations.
    
    This function provides a clean, injectable interface for bundle resolution
    that works with any OciRegistry implementation (real or fake).
    
    Args:
        ref: Bundle reference (must include name)
        registry: Optional OCI registry (defaults to HybridOciRegistry)
        settings: Optional settings (defaults to loading from environment)
        cache: Whether to prepare cache directory structure
        
    Returns:
        ResolvedBundle with canonical digest and bundle metadata
        
    Raises:
        ValueError: If ref doesn't include name or is malformed
        BundleNotFoundError: If bundle cannot be found
        OciError: For registry operation errors
        
    Examples:
        >>> # Use default registry
        >>> resolved = resolve(BundleRef(name="my-bundle", version="1.0"))
        
        >>> # Inject specific registry (e.g., for testing)
        >>> fake_registry = FakeOciRegistry()
        >>> resolved = resolve(ref, registry=fake_registry, settings=fake_settings)
    """
    # Validate ref has required name field
    if not ref.name:
        raise ValueError("BundleRef must include name (bare digests not supported)")
    
    # Load settings if not provided
    if settings is None:
        from .settings import load_settings_from_env
        settings = load_settings_from_env()
    
    # Use provided registry or create default
    if registry is None:
        from .storage.registry_factory import make_registry
        registry = make_registry(settings)
    
    # Use new OCI-based resolver
    from .storage.resolve_oci import resolve_oci
    return resolve_oci(ref, registry, settings, cache)


def materialize(
    ref: BundleRef,
    dest: str,
    *,
    role: str | None = None,
    overwrite: bool = False,
    prefetch_external: bool = False,
    provider: ContentProvider,
    registry: 'OciRegistry' = None,
) -> MaterializeResult:
    """
    Mirror layers for the selected role into dest.
    
    This function:
    - Resolves the bundle to get manifest and layer information
    - Selects role using precedence rules (arg > ref.role > default > error)
    - Uses ContentProvider to get entries for the requested layers
    - Downloads ORAS blobs to destination directory  
    - Creates pointer files for external storage references
    - Handles overwrite semantics and conflict detection
    
    Args:
        ref: Bundle reference to materialize
        dest: Destination directory path
        role: Role to materialize (overrides ref.role)
        overwrite: Whether to overwrite conflicting files
        prefetch_external: Whether to download external data immediately
        provider: ContentProvider to enumerate entries for materialization
        registry: Optional OCI registry (defaults to HybridOciRegistry)
        
    Returns:
        MaterializeResult containing bundle, selected_role, and dest_path
        
    Raises:
        RoleLayerMismatch: If role doesn't exist or references missing layers
        WorkdirConflict: If files conflict and overwrite=False
        BundleNotFoundError: If bundle cannot be found
    """
    # First resolve the bundle to get metadata
    resolved = resolve(ref, registry=registry)
    
    # Select role using precedence rules
    selected_role = _select_role(resolved, ref, role)
    
    # Get layers for the selected role (direct dict access)
    try:
        layer_names = resolved.roles[selected_role]
    except KeyError:
        available = ", ".join(sorted(resolved.roles.keys()))
        raise RoleLayerMismatch(f"Role '{selected_role}' not found in bundle. Available: {available}")
    
    # Validate that all layers referenced by the role exist in the manifest
    missing = [l for l in layer_names if l not in resolved.layers]
    if missing:
        raise RoleLayerMismatch(
            f"Role '{selected_role}' references non-existent layers: {sorted(missing)}"
        )
    
    # Create destination directory if it doesn't exist
    dest_path = Path(dest)
    dest_path.mkdir(parents=True, exist_ok=True)
    
    # Track conflicts for reporting
    conflicts = []
    
    # Use provider to enumerate all entries for the requested layers
    # Sort entries for deterministic order and detect duplicates
    entries = sorted(provider.iter_entries(resolved, layer_names), key=lambda e: e.path)
    seen: dict[str, str] = {}  # path -> first layer that claimed it
    
    for entry in entries:
        entry_path = safe_relpath(entry.path)
        if entry_path in seen:
            raise WorkdirConflict(
                f"Duplicate materialization path: {entry_path}",
                [{"path": entry_path, "first_layer": seen[entry_path], "duplicate_layer": entry.layer}]
            )
        seen[entry_path] = entry.layer
        target_path = dest_path / entry_path
        
        if entry.kind == "oras":
            # Handle ORAS content
            # MatEntry validation ensures content is present for ORAS entries
            assert entry.content is not None
            _materialize_oras_file(
                target_path, entry.content, overwrite, conflicts, dest_path
            )
        elif entry.kind == "external":
            # Handle external storage reference using metadata from provider  
            # MatEntry validation ensures uri, sha256, size are present for external entries
            _materialize_external_file(
                dest_path, entry, prefetch_external, provider,
                overwrite=overwrite, conflicts=conflicts, base_dir=dest_path
            )
    
    # Check for conflicts
    if conflicts and not overwrite:
        raise WorkdirConflict(
            f"{len(conflicts)} files conflict with existing content",
            conflicts
        )
    
    # Write provenance file with materialization metadata
    _write_provenance(dest_path, resolved, selected_role)
    
    return MaterializeResult(
        bundle=resolved,
        selected_role=selected_role,
        dest_path=dest
    )


def _select_role(resolved: ResolvedBundle, ref: BundleRef, role_arg: str | None) -> str:
    """
    Select role using precedence rules from the specification.
    
    Precedence (highest to lowest):
    1. Function argument role=...
    2. BundleRef hint ref.role  
    3. Manifest default (if "default" role exists)
    4. Error if no role can be determined
    """
    if role_arg:
        return _validate_role(resolved, role_arg)
    if ref.role:
        return _validate_role(resolved, ref.role)
    if "default" in resolved.roles:
        return "default"
    available = ", ".join(sorted(resolved.roles.keys()))
    raise RoleLayerMismatch(
        f"No role specified and no default role in manifest. Available: {available}"
    )


def _validate_role(resolved: ResolvedBundle, role: str) -> str:
    """
    Validate that role exists in the resolved bundle.
    
    Args:
        resolved: ResolvedBundle to check
        role: Role name to validate
        
    Returns:
        The role name if valid
        
    Raises:
        RoleLayerMismatch: If role doesn't exist
    """
    if role not in resolved.roles:
        available = ", ".join(sorted(resolved.roles.keys()))
        raise RoleLayerMismatch(
            f"Role '{role}' not found in bundle. Available: {available}"
        )
    return role


def _materialize_oras_file(
    target_path: Path,
    content: bytes,
    overwrite: bool,
    conflicts: List[Dict[str, str]],
    base_dir: Path
) -> None:
    """
    Materialize an ORAS file with conflict detection and atomic writes.
    
    Args:
        target_path: Where to write the file
        content: File content bytes
        overwrite: Whether to overwrite conflicts
        conflicts: List to append conflict info to (side-effects)
        base_dir: Base directory for relative path computation
    """
    if not target_path.exists():
        # File doesn't exist, create it
        target_path.parent.mkdir(parents=True, exist_ok=True)
        _write_file_atomically(target_path, content)
        return
    
    # Check if target is a directory when we expect a file
    if target_path.is_dir():
        if overwrite:
            # Remove directory and create file
            import shutil
            shutil.rmtree(target_path)
            _write_file_atomically(target_path, content)
        else:
            conflicts.append({
                "path": str(target_path.relative_to(base_dir)),
                "error": "expected file but found directory"
            })
        return
    
    # File exists, check if content matches
    try:
        existing_content = target_path.read_bytes()
        existing_hash = hashlib.sha256(existing_content).hexdigest()
        expected_hash = hashlib.sha256(content).hexdigest()
        
        if existing_hash == expected_hash:
            return  # UNCHANGED - no action needed
        
        # Content differs - conflict!
        if overwrite:
            _write_file_atomically(target_path, content)
        else:
            conflicts.append({
                "path": str(target_path.relative_to(base_dir)),
                "expected_sha256": expected_hash,
                "actual_sha256": existing_hash
            })
            
    except Exception:
        # If we can't read existing file, treat as conflict
        if overwrite:
            _write_file_atomically(target_path, content)
        else:
            conflicts.append({
                "path": str(target_path.relative_to(base_dir)),
                "expected_sha256": hashlib.sha256(content).hexdigest(),
                "actual_sha256": "unreadable"
            })


def _materialize_external_file(
    dest_path: Path,
    entry: MatEntry,
    prefetch_external: bool,
    provider: ContentProvider,
    *,
    overwrite: bool,
    conflicts: list[dict[str, str]],
    base_dir: Path,
) -> None:
    """
    Create pointer file for external storage reference.
    
    Args:
        dest_path: Root destination directory
        entry: MatEntry with external metadata (uri, sha256, size, tier)
        prefetch_external: Whether to also download the actual data
        provider: ContentProvider to fetch external content
    """
    # MatEntry validation ensures uri, sha256, size are non-None for external entries
    assert entry.uri is not None
    assert entry.sha256 is not None  
    assert entry.size is not None
    
    # If prefetch_external=True, fetch and write actual content first
    if prefetch_external:
        actual_path = dest_path / safe_relpath(entry.path)
        content = provider.fetch_external(entry)
        
        # Verify SHA256 integrity before writing
        got = hashlib.sha256(content).hexdigest()
        if got != entry.sha256:
            conflicts.append({
                "path": entry.path,
                "expected_sha256": entry.sha256,
                "actual_sha256": got
            })
            return
        
        # Use _materialize_oras_file for conflict detection and overwrite handling
        _materialize_oras_file(actual_path, content, overwrite, conflicts, base_dir)
    
    # Write pointer file only after successful content write (if prefetching)
    sanitized_path = safe_relpath(entry.path)
    write_pointer_file(
        dest_dir=dest_path,
        original_relpath=entry.path,
        uri=entry.uri,
        sha256=entry.sha256,
        size=entry.size,
        layer=entry.layer,
        tier=entry.tier,  # Pass through as-is - tier is optional hint only
        fulfilled=prefetch_external,
        local_path=sanitized_path if prefetch_external else None
    )


def _write_file_atomically(target_path: Path, content: bytes) -> None:
    """
    Write file content atomically using temp file + rename.
    
    Args:
        target_path: Final path for the file
        content: Content bytes to write
        
    Raises:
        OSError: If write operation fails
    """
    import os
    temp_path = target_path.parent / f"{target_path.name}.tmp.{os.getpid()}"
    
    try:
        with open(temp_path, 'wb') as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        
        # Atomic rename
        os.replace(temp_path, target_path)
        
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise


def _write_provenance(dest_path: Path, resolved: ResolvedBundle, role: str) -> None:
    """
    Write provenance file containing materialization metadata.
    
    Creates .mops/.mops-manifest.json with bundle information including
    manifest digest, selected role, layer indexes, and original reference.
    
    Args:
        dest_path: Destination directory path
        resolved: ResolvedBundle with metadata
        role: Selected role name
    """
    meta_dir = dest_path / ".mops"
    meta_dir.mkdir(parents=True, exist_ok=True)
    out = meta_dir / ".mops-manifest.json"
    
    payload = {
        "manifest_digest": resolved.manifest_digest,
        "media_type": getattr(resolved, "media_type", "application/vnd.modelops.bundle.manifest+json"),
        "role": role,
        "roles": resolved.roles,
        "layer_indexes": resolved.layer_indexes,
        "ref": resolved.ref.model_dump() if hasattr(resolved.ref, "model_dump") else resolved.ref.__dict__,
    }
    
    tmp = out.with_name(out.name + f".tmp.{os.getpid()}")
    try:
        with open(tmp, "w", encoding="utf-8", newline='\n') as f:
            json.dump(payload, f, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, out)
    except Exception:
        # Clean up temp file on error
        if tmp.exists():
            tmp.unlink()
        raise


