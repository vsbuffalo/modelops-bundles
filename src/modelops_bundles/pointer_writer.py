"""
Pointer file handling for external storage references.

This module handles writing pointer files that reference external storage
locations. Pointer files are JSON documents that contain metadata about
external data without the actual bytes.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from pydantic import BaseModel, Field

__all__ = ["PointerFile", "write_pointer_file", "read_pointer_file"]


class PointerFile(BaseModel):
    """
    Runtime-specific pointer file model.
    
    This is separate from contracts to allow runtime-specific evolution
    without forcing cross-repo upgrades.
    """
    schema_version: int = Field(1, description="Pointer file schema version")
    uri: str = Field(description="External storage URI (e.g., az://container/path)")
    sha256: str = Field(
        pattern=r"^[a-f0-9]{64}$",
        description="Hex-encoded SHA-256 hash of file content"
    )
    size: int = Field(ge=0, description="File size in bytes")
    tier: str | None = Field(
        None,
        description="Storage tier hint (hot/cool/archive)"
    )
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC timestamp when pointer was created"
    )
    fulfilled: bool = Field(
        default=False,
        description="True if data has been downloaded locally"
    )
    local_path: str | None = Field(
        None,
        description="Relative path to local copy when fulfilled=True"
    )
    original_path: str = Field(description="Original file path within the bundle")
    layer: str = Field(description="Layer name this file belongs to")


def write_pointer_file(
    dest_dir: Path,
    original_relpath: str,
    uri: str,
    sha256: str,
    size: int,
    layer: str,
    tier: str | None = None,
    local_path: str | None = None,
    fulfilled: bool = False
) -> Path:
    """
    Write a pointer file atomically using the canonical placement rule.
    
    Pointer files are always placed at: dest/.mops/ptr/<original_dir>/<filename>.json
    Never as sidecars alongside the actual data files.
    
    Args:
        dest_dir: Destination directory root
        original_relpath: Original file path within bundle (e.g., "data/fit/2022.parquet")
        uri: External storage URI
        sha256: Content hash (hex, no 'sha256:' prefix)
        size: File size in bytes  
        layer: Layer name this file belongs to
        tier: Optional storage tier
        local_path: Path to local copy if fulfilled
        fulfilled: Whether data has been downloaded locally
        
    Returns:
        Path to the written pointer file
        
    Raises:
        OSError: If file write fails
    """
    # Construct pointer file path following the canonical rule
    # dest/.mops/ptr/<original_dir>/<filename>.json
    original_path = Path(original_relpath)
    pointer_relpath = Path(".mops/ptr") / original_path.parent / f"{original_path.name}.json"
    pointer_path = dest_dir / pointer_relpath
    
    # Ensure parent directory exists
    pointer_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create pointer file data
    pointer = PointerFile(
        uri=uri,
        sha256=sha256,
        size=size,
        tier=tier,
        original_path=original_relpath,
        layer=layer,
        fulfilled=fulfilled,
        local_path=local_path
    )
    
    # Write atomically using temp file + rename
    temp_path = pointer_path.parent / f"{pointer_path.name}.tmp.{os.getpid()}"
    
    try:
        with open(temp_path, 'w', encoding='utf-8') as f:
            # Use canonical JSON serialization for determinism
            json.dump(
                pointer.model_dump(mode="json"),
                f,
                sort_keys=True,
                separators=(',', ':'),
                ensure_ascii=False,
            )
            f.flush()
            os.fsync(f.fileno())
        
        # Atomic rename
        os.replace(temp_path, pointer_path)
        return pointer_path
        
    except Exception:
        # Clean up temp file on error
        if temp_path.exists():
            temp_path.unlink()
        raise



def read_pointer_file(pointer_path: Path) -> PointerFile:
    """
    Read and validate a pointer file.
    
    Args:
        pointer_path: Path to the pointer file
        
    Returns:
        Parsed PointerFile object
        
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If file is malformed
    """
    with open(pointer_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return PointerFile.model_validate(data)