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

from modelops_contracts.artifacts import BundleRef, ResolvedBundle

from .path_safety import safe_relpath
from .pointer_writer import write_pointer_file
from .runtime_types import ContentProvider, MatEntry
from .storage.base import BundleRegistryStore

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


def resolve(ref: BundleRef, *, registry: BundleRegistryStore, repository: str | None = None, cache: bool = True) -> ResolvedBundle:
    """
    Resolve bundle identity without side-effects (no FS writes).
    
    Makes network calls to registry to resolve real bundle manifests and
    compute total sizes from layer indexes.
    
    Note: The total_size field reflects only external file sizes from
    layer indexes as a best-effort calculation. ORAS blob sizes are not
    included since they would require additional registry round-trips.
    
    This function:
    - Accepts BundleRef with one of: (name, version), digest, or local_path
    - Fetches bundle manifest from OCI registry via BundleRegistryStore
    - Parses roles, layers, and layer_indexes from manifest
    - Computes total_size by peeking at layer indexes (best-effort)
    - Returns ResolvedBundle with real content addresses and metadata
    - Does NOT download blobs, create files, or write to workdir
    
    Args:
        ref: Bundle reference to resolve
        cache: Whether to prepare cache directory structure (not implemented yet)
        registry: BundleRegistryStore for registry access
        repository: Repository namespace (e.g., "myorg/bundles") - required for name+version refs
        
    Returns:
        ResolvedBundle with manifest digest, roles, and metadata
        
    Raises:
        BundleNotFoundError: If bundle cannot be found in registry
        ValueError: If ref is malformed or manifest has invalid format
    """
    # Validate BundleRef constraints are met
    if not any([ref.local_path, ref.digest, (ref.name and ref.version)]):
        raise ValueError("BundleRef must specify exactly one of: local_path, digest, or name+version")
    
    # Determine manifest reference from BundleRef
    if ref.digest:
        manifest_ref = ref.digest
    elif ref.name and ref.version:
        if "/" in ref.name:
            raise ValueError("Bundle names cannot contain '/'. Use digest or full registry path")
        if not repository:
            raise ValueError("repository required for name+version refs")
        # Compose OCI reference from registry repo + name:version
        manifest_ref = f"{repository}/{ref.name}:{ref.version}"
    elif ref.local_path:
        # Local path support (future feature)
        raise ValueError("Local path support not implemented in Stage 6")
    else:
        raise ValueError("Invalid BundleRef: no resolvable reference found")
    
    # Fetch bundle manifest from registry
    try:
        payload = registry.get_manifest(manifest_ref)
    except KeyError as e:
        raise BundleNotFoundError(f"Bundle manifest not found: {manifest_ref}") from e
    
    # Parse manifest JSON
    try:
        doc = json.loads(payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        raise ValueError(f"Invalid manifest JSON: {e}") from e
    
    # Validate media type
    from modelops_contracts.artifacts import BUNDLE_MANIFEST, LAYER_INDEX
    media_type = doc.get("mediaType")
    if media_type != BUNDLE_MANIFEST:
        raise ValueError(f"Invalid manifest mediaType: expected {BUNDLE_MANIFEST}, got {media_type!r}")
    
    # Parse manifest fields
    roles = doc.get("roles", {})
    layers = doc.get("layers", [])
    layer_indexes = doc.get("layer_indexes", {})
    external_index_present = bool(doc.get("external_index_present", True))
    
    # Compute best-effort total_size by peeking at layer indexes
    total_size = 0
    for layer in layers:
        layer_index_digest = layer_indexes.get(layer)
        if not layer_index_digest:
            continue
        
        try:
            # Fetch layer index to sum external file sizes
            index_payload = registry.get_manifest(layer_index_digest)
            index_doc = json.loads(index_payload.decode("utf-8"))
            
            # Validate layer index media type
            if index_doc.get("mediaType") != LAYER_INDEX:
                raise ValueError(f"Invalid LAYER_INDEX mediaType for layer '{layer}'")
            
            # Sum external entry sizes
            for entry in index_doc.get("entries", []):
                external = entry.get("external")
                if external and "size" in external:
                    total_size += int(external["size"])
                    
        except (KeyError, json.JSONDecodeError, ValueError):
            # Layer index not found or malformed - will surface during materialize
            # Don't fail resolve, just continue with partial size
            continue
    
    # Compute canonical manifest digest
    manifest_digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    
    return ResolvedBundle(
        ref=ref,
        manifest_digest=manifest_digest,
        roles=roles,
        layers=layers,
        external_index_present=external_index_present,
        total_size=total_size,
        cache_dir=None,  # Cache not implemented yet
        layer_indexes=layer_indexes
    )


def materialize(
    ref: BundleRef,
    dest: str,
    *,
    role: str | None = None,
    overwrite: bool = False,
    prefetch_external: bool = False,
    provider: ContentProvider,
    registry: BundleRegistryStore,
    repository: str | None = None,
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
        registry: BundleRegistryStore for registry access
        repository: Repository namespace - required for name+version refs
        
    Returns:
        ResolvedBundle (same as resolve() would return)
        
    Raises:
        RoleLayerMismatch: If role doesn't exist or references missing layers
        WorkdirConflict: If files conflict and overwrite=False
        BundleNotFoundError: If bundle cannot be found
    """
    # First resolve the bundle to get metadata
    resolved = resolve(ref, registry=registry, repository=repository)
    
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
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, out)
    except Exception:
        # Clean up temp file on error
        if tmp.exists():
            tmp.unlink()
        raise


