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


@runtime_checkable
class BundleRegistryStore(Protocol):
    """Protocol for bundle registry (e.g. OCI Registry) operations."""
    
    def blob_exists(self, digest: str) -> bool:
        """
        Check if blob exists in registry.
        
        Args:
            digest: Content digest (e.g., "sha256:abc123...")
            
        Returns:
            True if blob exists, False otherwise
            
        Raises:
            No exceptions - always returns boolean
        """
        ...

    def get_blob(self, digest: str) -> bytes:
        """
        Retrieve blob content by digest.
        
        Args:
            digest: Content digest (e.g., "sha256:abc123...")
            
        Returns:
            Blob content as bytes
            
        Raises:
            KeyError: If blob does not exist
        """
        ...

    def put_blob(self, digest: str, data: bytes) -> None:
        """
        Store blob content under digest.
        
        Args:
            digest: Content digest (e.g., "sha256:abc123...")
            data: Blob content bytes
            
        Raises:
            ValueError: If digest format is invalid
        """
        ...

    def get_manifest(self, digest_or_ref: str) -> bytes:
        """
        Retrieve manifest by digest or reference.
        
        Args:
            digest_or_ref: Digest or tag reference
            
        Returns:
            Manifest content as bytes
            
        Raises:
            KeyError: If manifest does not exist
        """
        ...

    def put_manifest(self, media_type: str, payload: bytes) -> str:
        """
        Store manifest and return its digest.
        
        Args:
            media_type: MIME type of manifest
            payload: Manifest content bytes
            
        Returns:
            Canonical digest (e.g., "sha256:abc123...")
            
        Raises:
            ValueError: If manifest is invalid
        """
        ...


__all__ = ["ExternalStat", "BundleRegistryStore", "ExternalStore"]


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
