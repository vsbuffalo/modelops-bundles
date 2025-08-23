"""
Deterministic archive export.

Creates byte-identical tar archives from identical input trees by normalizing
paths, tar headers, and compression settings. Enforces USTAR format for
cross-platform compatibility.
"""
from __future__ import annotations

import os
import tarfile
import tempfile
import unicodedata
import zstandard as zstd
from pathlib import Path
from typing import Iterator, Tuple, Set
import json

from .path_safety import safe_relpath

def write_deterministic_archive(src_dir: str, out_path: str, *,
                                include_external: bool = False,
                                zstd_level: int = 19) -> None:
    """
    Create deterministic tar archive from source directory.
    
    Produces byte-identical archives from identical input trees by:
    - Normalizing paths for Windows compatibility  
    - Setting deterministic tar headers (uid=0, gid=0, mtime=0)
    - Using USTAR format without PAX headers
    - Applying consistent compression settings
    - Sorting entries deterministically
    
    Args:
        src_dir: Source directory to archive
        out_path: Output archive path (.tar or .tar.zst)
        include_external: Include external data files (vs pointer files only)
        zstd_level: Zstandard compression level (19 for max determinism)
        
    Raises:
        ValueError: If src_dir doesn't exist or contains unsafe paths
        OSError: If archive creation fails
    """
    src_path = Path(src_dir).resolve()
    if not src_path.is_dir():
        raise ValueError(f"Source directory does not exist: {src_dir}")
    
    out_path = Path(out_path).resolve()
    
    # Use atomic writes via temp file
    temp_fd = None
    temp_path = None
    
    try:
        # Create temporary file in same directory as output for atomic rename
        temp_fd, temp_path = tempfile.mkstemp(
            suffix='.tmp', 
            dir=out_path.parent,
            prefix=out_path.name + '.'
        )
        
        if out_path.suffix == '.zst' or out_path.name.endswith('.tar.zst'):
            _write_zst_archive(temp_fd, src_path, include_external, zstd_level)
        else:
            _write_tar_archive(temp_fd, src_path, include_external)
        
        os.close(temp_fd)
        temp_fd = None
        
        # Atomic rename to final path
        os.rename(temp_path, out_path)
        temp_path = None
        
    except Exception:
        # Cleanup on any failure
        if temp_fd is not None:
            try:
                os.close(temp_fd)
            except OSError:
                pass
        if temp_path is not None and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except OSError:
                pass
        raise

def _write_tar_archive(fd: int, src_path: Path, include_external: bool) -> None:
    """Write uncompressed tar archive to file descriptor."""
    with os.fdopen(os.dup(fd), 'wb') as f:
        with tarfile.open(fileobj=f, mode='w', format=tarfile.USTAR_FORMAT) as tar:
            _add_entries_to_tar(tar, src_path, include_external)

def _write_zst_archive(fd: int, src_path: Path, include_external: bool, zstd_level: int) -> None:
    """Write zstandard-compressed tar archive to file descriptor."""
    with os.fdopen(os.dup(fd), 'wb') as f:
        # Configure zstd for deterministic output
        compressor = zstd.ZstdCompressor(
            level=zstd_level,
            write_content_size=True,
            write_checksum=True
        )
        
        with compressor.stream_writer(f) as zstd_writer:
            with tarfile.open(fileobj=zstd_writer, mode='w', format=tarfile.USTAR_FORMAT) as tar:
                _add_entries_to_tar(tar, src_path, include_external)

def _collect_pointer_targets(root: Path) -> Set[str]:
    """Collect paths that have corresponding pointer files."""
    ptr_root = root / ".mops" / "ptr"
    targets: Set[str] = set()
    if not ptr_root.exists():
        return targets
    for p in ptr_root.rglob("*.json"):
        try:
            data = json.loads(p.read_text(encoding="utf-8"))
            original = data.get("original_path")
            if isinstance(original, str):
                targets.add(normalize_relpath(original))
        except Exception:
            # If a pointer is malformed, still include it; just don't exclude anything based on it
            pass
    return targets

def _add_entries_to_tar(tar: tarfile.TarFile, src_path: Path, include_external: bool) -> None:
    """Add directory entries to tar archive in deterministic order."""
    pointer_targets = set()
    if not include_external:
        pointer_targets = _collect_pointer_targets(src_path)
    
    for entry_path, arcname in _iter_entries_sorted(src_path):
        # Keep pointer files always; optionally skip the actual bytes
        if not include_external:
            rel = normalize_relpath(str(entry_path.relative_to(src_path)))
            if rel in pointer_targets:
                continue
            
        # Validate path safety for archive creation
        try:
            _validate_archive_path(arcname)
        except ValueError as e:
            raise ValueError(f"Unsafe archive path {arcname}: {e}")
        
        # Ensure directory names end with "/" for canonical tars
        arc = arcname + ("/" if entry_path.is_dir() and not arcname.endswith("/") else "")
        tarinfo = tar.gettarinfo(str(entry_path), arcname=arc)
        _apply_canonical_headers(tarinfo)
        
        if tarinfo.isreg():
            with open(entry_path, 'rb') as entry_file:
                tar.addfile(tarinfo, entry_file)
        else:
            tar.addfile(tarinfo)

def _iter_entries_sorted(src_dir: Path) -> Iterator[Tuple[Path, str]]:
    """
    Iterate entries in deterministic order.
    
    Yields (filesystem_path, archive_name) pairs sorted by archive name.
    Directories are processed before their contents for tar compatibility.
    
    Args:
        src_dir: Source directory to iterate
        
    Yields:
        (entry_path, archive_name) tuples
    """
    entries = []
    
    for root, dirs, files in os.walk(src_dir):
        root_path = Path(root)
        rel_root = root_path.relative_to(src_dir)
        
        # Add directory entry (except for root)
        if rel_root != Path('.'):
            entries.append((root_path, normalize_relpath(str(rel_root))))
        
        # Add file entries
        for file_name in files:
            file_path = root_path / file_name
            rel_file = file_path.relative_to(src_dir)
            entries.append((file_path, normalize_relpath(str(rel_file))))
    
    # Sort by archive name for deterministic ordering
    entries.sort(key=lambda x: x[1])
    
    yield from entries

def normalize_relpath(path: str) -> str:
    """
    Normalize relative path for archive creation.
    
    Converts backslashes to forward slashes and applies basic safety validation.
    Unlike safe_relpath, this allows .mops paths since they contain valid metadata.
    
    Args:
        path: Relative path to normalize
        
    Returns:
        Normalized path with forward slashes
        
    Raises:
        ValueError: If path contains unsafe sequences after normalization
    """
    # Convert backslashes to forward slashes and apply NFC normalization
    normalized = unicodedata.normalize('NFC', path.replace('\\', '/'))
    if normalized.startswith("./"):
        normalized = normalized[2:]
    
    # Apply basic safety validation (but allow .mops paths)
    _validate_archive_path(normalized)
    
    return normalized

def _validate_archive_path(path: str) -> None:
    """
    Validate path is safe for archive creation.
    
    This is more permissive than safe_relpath since it allows .mops paths
    which are needed for metadata in archives.
    
    Args:
        path: Path to validate
        
    Raises:
        ValueError: If path contains dangerous sequences
    """
    from pathlib import PurePosixPath
    
    rel = PurePosixPath(path)
    s = str(rel)
    
    # Basic safety checks
    if not s or s == ".":
        raise ValueError(f"unsafe archive path: {path}")
    if "\\" in s:
        raise ValueError(f"unsafe archive path: {path}")
    if rel.is_absolute() or ".." in rel.parts:
        raise ValueError(f"unsafe archive path: {path}")
    
    # Security: Check for NUL bytes which can cause path truncation
    if "\x00" in s:
        raise ValueError(f"archive path contains NUL byte: {path}")
    
    # Note: We allow .mops paths for metadata in archives

def _apply_canonical_headers(tarinfo: tarfile.TarInfo) -> None:
    """
    Apply canonical tar headers for deterministic output.
    
    Sets consistent ownership, timestamps, and permissions while
    preserving essential file type information.
    
    Args:
        tarinfo: Tar info object to canonicalize
    """
    # Deterministic ownership
    tarinfo.uid = 0
    tarinfo.gid = 0 
    tarinfo.uname = ""
    tarinfo.gname = ""
    
    # Deterministic timestamp 
    tarinfo.mtime = 0
    
    # Normalize permissions while preserving file type
    if tarinfo.isdir():
        tarinfo.mode = 0o755  # rwxr-xr-x for directories
    elif tarinfo.isreg():
        # Preserve execute bit for regular files
        if tarinfo.mode & 0o100:
            tarinfo.mode = 0o755  # rwxr-xr-x for executables
        else:
            tarinfo.mode = 0o644  # rw-r--r-- for regular files
    # For other types (symlinks, etc.), keep existing permissions

