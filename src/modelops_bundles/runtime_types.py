"""
Runtime types for ModelOps Bundles content materialization.

These types define the interface between the runtime and content providers,
enabling dependency injection for different content sources (ORAS, local files,
external storage adapters, etc.).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, Literal, Protocol

from modelops_contracts.artifacts import ResolvedBundle


# Type alias for content kinds
Kind = Literal["oras", "external"]

__all__ = ["MatEntry", "ContentProvider", "Kind"]


@dataclass(frozen=True, slots=True)
class MatEntry:
    """
    A single entry to be materialized by the runtime.
    
    Represents one file or reference that needs to be written to the destination
    directory. Content providers enumerate these entries based on the requested
    layers and bundle metadata.
    """
    path: str          # Relative path in workdir: "src/model.py" (POSIX-style)
    layer: str         # Layer name this entry belongs to: "code", "data", etc.
    kind: Kind         # Content type: "oras" for direct bytes, "external" for pointer
    content: bytes | None  # File bytes for oras; None for external (pointer only)
    # External-only metadata (required when kind=="external")
    uri: str | None = None        # External storage URI
    sha256: str | None = None     # 64-hex, no 'sha256:' prefix
    size: int | None = None       # Size in bytes
    tier: str | None = None       # Storage tier hint
    
    def __post_init__(self) -> None:
        """Validate MatEntry constraints."""
        if self.kind == "oras" and self.content is None:
            raise ValueError("ORAS entries must have content bytes")
        if self.kind == "external":
            if self.content is not None:
                raise ValueError("External entries must have content=None")
            # Check for None instead of truthiness to allow size=0
            missing = [k for k, v in dict(uri=self.uri, sha256=self.sha256, size=self.size).items() if v is None]
            if missing:
                raise ValueError(f"External entries require fields: {missing}")
            # Validate SHA256 format (64 hex characters)
            if self.sha256 and not re.fullmatch(r"[a-f0-9]{64}", self.sha256):
                raise ValueError("External sha256 must be 64 hex chars")


class ContentProvider(Protocol):
    """
    Protocol for providing content to the materialize() function.
    
    Content providers are responsible for enumerating all the files and references
    that need to be materialized for a given set of layers. They handle the details
    of fetching ORAS blobs, resolving external storage references, etc.
    
    This protocol enables dependency injection, allowing different implementations
    for testing (fake data), local development (file-based), and production 
    (registry + external storage).
    """
    
    def enumerate(
        self, 
        resolved: ResolvedBundle, 
        layers: list[str]
    ) -> Iterable[MatEntry]:
        """
        Enumerate all entries that need to be materialized for the given layers.
        
        Args:
            resolved: The resolved bundle with manifest information
            layers: List of layer names to materialize (from selected role)
            
        Yields:
            MatEntry objects for each file/reference to materialize
            
        Raises:
            Exception: If content cannot be retrieved or enumerated
        """
        ...
    
    def fetch_external(self, entry: MatEntry) -> bytes:
        """
        Fetch the actual content for an external storage entry.
        
        Args:
            entry: MatEntry with kind=="external" containing URI and metadata
            
        Returns:
            Content bytes from external storage
            
        Raises:
            Exception: If external content cannot be fetched
        """
        ...