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
import tempfile
from pathlib import Path
from typing import Dict, List
from dataclasses import dataclass
from io import BytesIO

from modelops_contracts.artifacts import BundleRef, ResolvedBundle

from .path_safety import safe_relpath
from .pointer_writer import write_pointer_file
from .runtime_types import ContentProvider, MatEntry, ByteStream
# TYPE_CHECKING import to avoid circular imports  
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .storage.oras_bundle_registry import OrasBundleRegistry

__all__ = ["resolve", "materialize", "MaterializeResult", "WorkdirConflict", "RoleLayerMismatch", "BundleNotFoundError", "BundleDownloadError"]


# Streaming I/O constants and utilities
CHUNK_SIZE = 1024 * 1024  # 1 MiB


def write_stream_atomically(target_path: Path, bytestream: ByteStream, *, expected_sha: str) -> None:
    """
    Stream content to file with atomic write and SHA256 verification.
    
    This function provides memory-efficient writing of large files by streaming
    content in chunks while computing SHA256 hash for verification. The write
    is atomic (temp file + rename) to prevent partial files on failure.
    
    Args:
        target_path: Final path for the file
        bytestream: Content stream (file-like with read() or iterable of bytes)
        expected_sha: Expected SHA256 hash (64 hex chars, no prefix)
        
    Raises:
        ValueError: If SHA256 verification fails
        OSError: If file operations fail
    """
    # Ensure parent directory exists
    target_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create temp file in same directory for atomic rename
    temp_dir = target_path.parent
    hash_obj = hashlib.sha256()
    
    # Create temporary file
    fd, temp_path = tempfile.mkstemp(prefix=".mops.tmp.", dir=temp_dir)
    temp_path = Path(temp_path)
    
    try:
        with os.fdopen(fd, "wb", buffering=0) as out:
            # Handle file-like objects with read()
            if hasattr(bytestream, "read"):
                while True:
                    chunk = bytestream.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    hash_obj.update(chunk)
                    out.write(chunk)
            else:
                # Handle iterables of bytes
                for chunk in bytestream:
                    hash_obj.update(chunk)
                    out.write(chunk)
            
            # Ensure data is written to disk
            out.flush()
            os.fsync(out.fileno())
        
        # Verify SHA256
        actual_sha = hash_obj.hexdigest()
        if actual_sha != expected_sha:
            temp_path.unlink()  # Clean up temp file
            raise ValueError(f"SHA mismatch for {target_path}: expected {expected_sha}, got {actual_sha}")
        
        # Atomic rename to final location
        # Handle case where target is a directory (need to remove it first)
        if target_path.exists() and target_path.is_dir():
            import shutil
            shutil.rmtree(target_path)
        os.replace(temp_path, target_path)
        
    except Exception:
        # Clean up temp file on any error
        try:
            temp_path.unlink()
        except FileNotFoundError:
            pass
        raise


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


def resolve(ref: BundleRef, *, registry: 'OrasBundleRegistry' = None, settings = None, cache: bool = True) -> ResolvedBundle:
    """
    Resolve bundle identity using repo-aware OCI registry operations.
    
    This function provides a clean, injectable interface for bundle resolution
    that works with any OrasBundleRegistry implementation (real or fake).
    
    Args:
        ref: Bundle reference (must include name)
        registry: Optional ORAS bundle registry (defaults to OrasBundleRegistry)
        settings: Optional settings (defaults to loading from environment)
        cache: Whether to prepare cache directory structure
        
    Returns:
        ResolvedBundle with canonical digest and bundle metadata
        
    Raises:
        ValueError: If ref doesn't include name or is malformed
        BundleNotFoundError: If bundle cannot be found
        
    Examples:
        >>> # Use default registry
        >>> resolved = resolve(BundleRef(name="my-bundle", version="1.0"))
        
        >>> # Inject specific registry (e.g., for testing)
        >>> fake_registry = FakeOrasBundleRegistry()
        >>> resolved = resolve(ref, registry=fake_registry, settings=fake_settings)
    """
    # Validate ref has required name field
    if not ref.name:
        raise ValueError("BundleRef must include name (bare digests not supported)")
    
    # Load settings if not provided
    if settings is None:
        from .settings import create_settings_from_env
        settings = create_settings_from_env()
    
    # Use provided registry or create default
    if registry is None:
        try:
            from .storage.oras_bundle_registry import OrasBundleRegistry
            registry = OrasBundleRegistry(settings)
        except ImportError:
            # Fall back to old registry factory for tests
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
    registry: 'OrasBundleRegistry' = None,
    settings = None,
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
        registry: Optional ORAS bundle registry (defaults to OrasBundleRegistry)
        settings: Optional settings (passed to resolve())
        
    Returns:
        MaterializeResult containing bundle, selected_role, and dest_path
        
    Raises:
        RoleLayerMismatch: If role doesn't exist or references missing layers
        WorkdirConflict: If files conflict and overwrite=False
        BundleNotFoundError: If bundle cannot be found
    """
    # First resolve the bundle to get metadata
    resolved = resolve(ref, registry=registry, settings=settings)
    
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
            # Handle ORAS content with streaming
            # Check for conflicts before fetching content to avoid unnecessary network I/O
            if target_path.exists() and not overwrite:
                # Check if existing content matches expected SHA256
                try:
                    existing_content = target_path.read_bytes()
                    existing_hash = hashlib.sha256(existing_content).hexdigest()
                    if existing_hash != entry.sha256:
                        conflicts.append({
                            "path": entry_path,
                            "expected_sha256": entry.sha256,
                            "actual_sha256": existing_hash
                        })
                        continue  # Skip to next entry
                    else:
                        # Content matches, no need to rewrite
                        continue
                except Exception:
                    # If we can't read existing file, treat as conflict
                    conflicts.append({
                        "path": entry_path,
                        "expected_sha256": entry.sha256,
                        "actual_sha256": "unreadable"
                    })
                    continue
            
            # Fetch content stream and write with verification
            try:
                stream = provider.fetch_oras(entry)
                write_stream_atomically(target_path, stream, expected_sha=entry.sha256)
            except ValueError as e:
                if "SHA mismatch" in str(e):
                    # Convert SHA mismatch to conflict
                    # Extract actual SHA from error message
                    import re
                    match = re.search(r"got ([a-f0-9]{64})", str(e))
                    actual_sha = match.group(1) if match else "invalid"
                    conflicts.append({
                        "path": entry_path,
                        "expected_sha256": entry.sha256,
                        "actual_sha256": actual_sha
                    })
                else:
                    raise
        elif entry.kind == "external":
            # Handle external storage reference
            # Always write pointer file (fulfilled=False initially)
            write_pointer_file(
                dest_dir=dest_path,
                original_relpath=entry.path,
                uri=entry.uri,
                sha256=entry.sha256,
                size=entry.size,
                layer=entry.layer,
                tier=entry.tier,
                fulfilled=False,
                local_path=None
            )
            
            # Optionally prefetch the actual content
            if prefetch_external:
                # Check for conflicts before fetching
                if target_path.exists() and not overwrite:
                    try:
                        existing_content = target_path.read_bytes()
                        existing_hash = hashlib.sha256(existing_content).hexdigest()
                        if existing_hash != entry.sha256:
                            conflicts.append({
                                "path": entry_path,
                                "expected_sha256": entry.sha256,
                                "actual_sha256": existing_hash
                            })
                            continue
                        else:
                            # Content matches, no need to rewrite
                            continue
                    except Exception:
                        conflicts.append({
                            "path": entry_path,
                            "expected_sha256": entry.sha256,
                            "actual_sha256": "unreadable"
                        })
                        continue
                
                # Fetch external content stream and write
                try:
                    stream = provider.fetch_external(entry)
                    write_stream_atomically(target_path, stream, expected_sha=entry.sha256)
                    # Success: mark pointer as fulfilled
                    write_pointer_file(
                        dest_dir=dest_path,
                        original_relpath=entry.path,
                        uri=entry.uri,
                        sha256=entry.sha256,
                        size=entry.size,
                        layer=entry.layer,
                        tier=entry.tier,
                        fulfilled=True,
                        local_path=entry_path
                    )
                except ValueError as e:
                    if "SHA mismatch" in str(e):
                        # Convert SHA mismatch to conflict
                        import re
                        match = re.search(r"got ([a-f0-9]{64})", str(e))
                        actual_sha = match.group(1) if match else "invalid"
                        conflicts.append({
                            "path": entry_path,
                            "expected_sha256": entry.sha256,
                            "actual_sha256": actual_sha
                        })
                    else:
                        raise
    
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


# Legacy helper functions removed - now using streaming I/O directly in materialize()


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
        "media_type": getattr(resolved, "media_type", "application/json"),
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


