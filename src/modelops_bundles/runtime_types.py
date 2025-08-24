"""
Runtime types for ModelOps Bundles content materialization.

These types define the interface between the runtime and content providers,
enabling dependency injection for different content sources (ORAS, local files,
external storage adapters, etc.).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Literal, Protocol, IO, Optional

from modelops_contracts.artifacts import ResolvedBundle


# Type alias for content kinds
Kind = Literal["oras", "external"]

# Type alias for byte streams (file-like or iterable)
ByteStream = IO[bytes] | Iterable[bytes]

__all__ = ["MatEntry", "ContentProvider", "Kind", "ByteStream"]


@dataclass(frozen=True, slots=True)
class MatEntry:
    """
    A single entry to be materialized by the runtime.
    
    Represents one file or reference that needs to be written to the destination
    directory. Content providers enumerate these entries based on the requested
    layers and bundle metadata.
    
    This is now a lightweight metadata-only structure. Actual content is fetched
    lazily via the ContentProvider when needed for materialization.
    """
    path: str                    # Relative path in workdir: "src/model.py" (POSIX-style)
    layer: str                   # Layer name this entry belongs to: "code", "data", etc.
    kind: Kind                   # Content type: "oras" or "external"
    size: int                    # Size in bytes
    
    # Identity and verification fields
    digest: str                  # Full digest for registry addressing: "sha256:abcdef..."
    sha256: str                  # 64-hex for verification, no 'sha256:' prefix
    
    # External-only metadata
    uri: Optional[str] = None    # External storage URI (required for external entries)
    tier: Optional[str] = None   # Storage tier hint (optional)
    
    def __post_init__(self) -> None:
        """Validate MatEntry constraints."""
        if self.kind == "external":
            if self.uri is None:
                raise ValueError("External entries require uri field")
        
        # Validate digest format (sha256:...)
        if not self.digest.startswith("sha256:") or len(self.digest) != 71:
            raise ValueError(f"digest must be 'sha256:<64 hex chars>', got '{self.digest}'")
        
        # Validate SHA256 format (64 hex characters, no prefix)
        if not re.fullmatch(r"[a-f0-9]{64}", self.sha256):
            raise ValueError("sha256 must be 64 hex chars")
        
        # Validate size
        if self.size < 0:
            raise ValueError("size must be non-negative")


class ContentProvider(Protocol):
    """
    Protocol for providing content to the materialize() function.
    
    Content providers are responsible for enumerating all the files and references
    that need to be materialized for a given set of layers. They provide lazy access
    to content via streaming interfaces to minimize memory usage.
    
    This protocol enables dependency injection, allowing different implementations
    for testing (fake data), local development (file-based), and production 
    (registry + external storage).
    """
    
    def iter_entries(
        self, 
        resolved: ResolvedBundle, 
        layers: list[str]
    ) -> Iterable[MatEntry]:
        """
        Enumerate all entries that need to be materialized for the given layers.
        
        IMPORTANT: This method must be side-effect free. It should not make any
        network calls, open files, or perform any I/O operations. It only yields
        metadata about what needs to be materialized.
        
        Args:
            resolved: The resolved bundle with manifest information
            layers: List of layer names to materialize (from selected role)
            
        Yields:
            MatEntry objects with metadata only (no content bytes)
            
        Raises:
            ValueError: If layer index is missing or invalid
            Exception: If metadata cannot be enumerated
        """
        ...
    
    def fetch_oras(self, entry: MatEntry) -> ByteStream:
        """
        Open a streaming source for a registry blob (lazy).
        
        This method is called only when the content is actually needed for
        materialization, allowing for memory-efficient processing of large files.
        
        Args:
            entry: MatEntry with kind=="oras" containing digest
            
        Returns:
            Streaming source for the blob content
            
        Raises:
            ValueError: If entry is not an ORAS entry
            Exception: If registry content cannot be accessed
        """
        ...
    
    def fetch_external(self, entry: MatEntry) -> ByteStream:
        """
        Open a streaming source for an external storage object (lazy).
        
        Used only when prefetching external content. This method provides
        streaming access to avoid loading large external files into memory.
        
        Args:
            entry: MatEntry with kind=="external" containing URI and metadata
            
        Returns:
            Streaming source for the external content
            
        Raises:
            ValueError: If entry is not an external entry
            Exception: If external content cannot be accessed
        """
        ...