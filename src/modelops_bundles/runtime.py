"""
ModelOps Bundles Runtime Layer.

This module implements the core resolve() and materialize() functions for
ModelOps Bundles, following the specification for role selection, overwrite
semantics, and pointer file placement.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Dict, List

from modelops_contracts.artifacts import BundleRef, ResolvedBundle

from .pointer_writer import write_pointer_file
from .runtime_types import ContentProvider, MatEntry

__all__ = ["resolve", "materialize", "WorkdirConflict", "RoleLayerMismatch", "BundleNotFoundError"]


# Exception Types
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


def resolve(ref: BundleRef, *, cache: bool = True) -> ResolvedBundle:
    """
    Resolve bundle identity without side-effects (no FS writes).
    
    Stage-1 stub: returns deterministic placeholder roles/layers and a fake digest
    computed from the ref. Stage-2 will load real manifests from the registry/local
    path and compute actual totals.
    
    This function:
    - Accepts BundleRef with one of: (name, version), digest, or local_path
    - Makes network calls to registry to resolve manifests/indices (Stage-2)
    - Returns ResolvedBundle with content addresses, roles, layer list, sizes
    - Does NOT download blobs, create files, or write to workdir
    - May create cache directory structure (mkdir only) if cache=True (Stage-2)
    
    Args:
        ref: Bundle reference to resolve
        cache: Whether to prepare cache directory structure
        
    Returns:
        ResolvedBundle with manifest digest, roles, and metadata
        
    Raises:
        BundleNotFoundError: If bundle cannot be found
        ValueError: If ref is malformed
    """
    # TODO: Implement registry/local resolution logic
    # For Stage 1 MVP, return a stub ResolvedBundle
    
    # Validate BundleRef constraints are met (contracts should handle this)
    if not any([ref.local_path, ref.digest, (ref.name and ref.version)]):
        raise ValueError("BundleRef must specify exactly one of: local_path, digest, or name+version")
    
    # For MVP, create a fake resolved bundle with deterministic content
    # In real implementation, this would:
    # 1. Resolve manifest from registry/local path
    # 2. Parse roles and layers
    # 3. Compute total sizes
    # 4. Optionally create cache directory structure
    
    fake_digest = _compute_fake_digest(ref)
    
    # Simulate some realistic roles and layers for testing
    fake_roles = {
        "default": ["code", "config"],
        "runtime": ["code", "config"], 
        "training": ["code", "config", "data"]
    }
    fake_layers = ["code", "config", "data"]
    
    return ResolvedBundle(
        ref=ref,
        manifest_digest=fake_digest,
        roles=fake_roles,
        layers=fake_layers,
        external_index_present=True,  # Assume external data exists for testing
        total_size=1024 * 1024,  # 1MB fake size
        cache_dir=None  # No cache implementation in Stage 1
    )


def materialize(
    ref: BundleRef,
    dest: str,
    *,
    role: str | None = None,
    overwrite: bool = False,
    prefetch_external: bool = False,
    provider: ContentProvider,
) -> ResolvedBundle:
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
        
    Returns:
        ResolvedBundle (same as resolve() would return)
        
    Raises:
        RoleLayerMismatch: If role doesn't exist or references missing layers
        WorkdirConflict: If files conflict and overwrite=False
        BundleNotFoundError: If bundle cannot be found
    """
    # First resolve the bundle to get metadata
    resolved = resolve(ref)
    
    # Select role using precedence rules
    selected_role = _select_role(resolved, ref, role)
    
    # Get layers for the selected role
    layer_names = resolved.get_role_layers(selected_role)
    
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
    entries = sorted(provider.enumerate(resolved, layer_names), key=lambda e: e.path)
    seen: set[str] = set()
    
    for entry in entries:
        if entry.path in seen:
            raise WorkdirConflict(
                f"Duplicate materialization path: {entry.path}",
                [{"path": entry.path, "error": "duplicate"}]
            )
        seen.add(entry.path)
        target_path = dest_path / entry.path
        
        if entry.kind == "oras":
            # Handle ORAS content
            _materialize_oras_file(
                target_path, entry.content, overwrite, conflicts, dest_path
            )
        elif entry.kind == "external":
            # Handle external storage reference using metadata from provider
            _materialize_external_file(
                dest_path, entry, prefetch_external, provider
            )
    
    # Check for conflicts
    if conflicts and not overwrite:
        raise WorkdirConflict(
            f"{len(conflicts)} files conflict with existing content",
            conflicts
        )
    
    return resolved


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
        conflicts: List to append conflict info to
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
    provider: ContentProvider
) -> None:
    """
    Create pointer file for external storage reference.
    
    Args:
        dest_path: Root destination directory
        entry: MatEntry with external metadata (uri, sha256, size, tier)
        prefetch_external: Whether to also download the actual data
        provider: ContentProvider to fetch external content
    """
    # Always create pointer file following canonical placement rule
    write_pointer_file(
        dest_dir=dest_path,
        original_relpath=entry.path,
        uri=entry.uri,
        sha256=entry.sha256,
        size=entry.size,
        layer=entry.layer,
        tier=entry.tier or "cool",
        fulfilled=prefetch_external,
        local_path=entry.path if prefetch_external else None
    )
    
    # If prefetch_external=True, fetch actual content from provider
    if prefetch_external:
        actual_path = dest_path / entry.path
        actual_path.parent.mkdir(parents=True, exist_ok=True)
        content = provider.fetch_external(entry)
        _write_file_atomically(actual_path, content)


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


def _compute_fake_digest(ref: BundleRef) -> str:
    """
    Compute a fake but deterministic manifest digest for testing.
    
    In real implementation, this would be the actual SHA-256 of the
    canonical manifest JSON.
    """
    # Create deterministic fake digest based on ref
    if ref.digest:
        return ref.digest
    elif ref.local_path:
        content = f"local:{ref.local_path}"
    else:
        content = f"{ref.name}:{ref.version}"
    
    hash_bytes = hashlib.sha256(content.encode()).digest()
    return "sha256:" + hash_bytes.hex()