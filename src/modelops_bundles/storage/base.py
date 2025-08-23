"""
Storage interfaces for ModelOps Bundles.

These protocols define the boundary between runtime and storage implementations,
enabling clean dependency injection and testing with fakes.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, runtime_checkable


@dataclass(frozen=True)
class ExternalStat:
    """
    Metadata for an external object used to build pointer files.
    
    Invariants:
    - sha256: exactly 64 lowercase hex characters, no 'sha256:' prefix
    - size: exact byte length (>= 0)
    - tier: optional hint only ("hot" | "cool" | "archive"); runtime never branches on it
    """
    uri: str
    size: int
    sha256: str
    tier: Optional[str] = None


__all__ = ["ExternalStat", "ExternalStore"]


@runtime_checkable
class ExternalStore(Protocol):
    """Protocol for external storage operations."""
    
    def stat(self, uri: str) -> ExternalStat:
        """
        Get metadata for external object without fetching content.
        
        This method is intended for future verify/integrity check workflows
        to validate external references against expected metadata without
        the cost of downloading large data files.
        
        Args:
            uri: External storage URI
            
        Returns:
            Object metadata (size, SHA256, tier) for verification
            
        Raises:
            FileNotFoundError: If object does not exist
            OSError: For other I/O errors
        """
        ...

    def get(self, uri: str) -> bytes:
        """
        Retrieve external object content.
        
        Args:
            uri: External storage URI
            
        Returns:
            Object content as bytes
            
        Raises:
            FileNotFoundError: If object does not exist
            OSError: For other I/O errors
        """
        ...

    def put(
        self, 
        uri: str, 
        data: bytes, 
        *, 
        sha256: Optional[str] = None,
        tier: Optional[str] = None
    ) -> ExternalStat:
        """
        Store external object and return metadata.
        
        Args:
            uri: External storage URI
            data: Object content bytes
            sha256: Expected SHA256 hash (64 hex chars, no prefix)
            tier: Storage tier hint ("hot" | "cool" | "archive")
            
        Returns:
            Object metadata with computed hash and size
            
        Raises:
            ValueError: If provided sha256 does not match computed hash
            OSError: For I/O errors
        """
        ...
