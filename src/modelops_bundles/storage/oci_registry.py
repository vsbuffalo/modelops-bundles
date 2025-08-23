"""
OCI Registry protocol definition.

Defines the repo-aware interface for OCI registry operations. This protocol
replaces the old repo-agnostic BundleRegistryStore with a clean, honest
interface that reflects how OCI actually works.
"""
from __future__ import annotations

from typing import BinaryIO, Protocol, Union, runtime_checkable


@runtime_checkable
class OciRegistry(Protocol):
    """
    Repo-aware OCI registry operations.
    
    All operations are explicitly scoped to a repository, which reflects
    how OCI Distribution API actually works. This eliminates the need for
    callers to smuggle repository information through other channels.
    """
    
    def head_manifest(self, repo: str, ref: str) -> str:
        """
        HEAD manifest and return canonical digest.
        
        Args:
            repo: Repository path (e.g., "myorg/bundles/my-bundle")  
            ref: Tag or digest reference (e.g., "v1.0", "sha256:abc...")
            
        Returns:
            Canonical digest from Docker-Content-Digest header
            
        Raises:
            OciNotFound: If manifest doesn't exist
            OciAuthError: If authentication fails
            OciError: For other registry errors
        """
        ...
    
    def get_manifest(self, repo: str, ref: str) -> bytes:
        """
        GET manifest content.
        
        Args:
            repo: Repository path
            ref: Tag or digest reference
            
        Returns:
            Raw manifest bytes
            
        Raises:
            OciNotFound: If manifest doesn't exist
            OciAuthError: If authentication fails  
            OciError: For other registry errors
        """
        ...
    
    def put_manifest(self, repo: str, media_type: str, payload: bytes, tag: str) -> str:
        """
        PUT manifest with explicit media type, return canonical digest.
        
        This method validates that the canonical digest returned by the registry
        matches the local computation to catch registry inconsistencies early.
        
        Args:
            repo: Repository path
            media_type: Manifest media type (e.g., OCI_IMAGE_MANIFEST)
            payload: Manifest content as bytes
            tag: Tag to apply
            
        Returns:
            Canonical digest after validation
            
        Raises:
            OciDigestMismatch: If server digest != local digest
            OciAuthError: If authentication fails
            OciError: For other registry errors
        """
        ...
    
    def get_blob(self, repo: str, digest: str) -> bytes:
        """
        GET blob content by digest.
        
        Args:
            repo: Repository path
            digest: Content digest (e.g., "sha256:abc...")
            
        Returns:
            Blob content as bytes
            
        Raises:
            OciNotFound: If blob doesn't exist
            OciAuthError: If authentication fails
            OciError: For other registry errors
        """
        ...
    
    def put_blob(self, repo: str, digest: str, data: Union[bytes, BinaryIO], 
                 size: int | None = None) -> None:
        """
        PUT blob to registry with streaming support.
        
        Args:
            repo: Repository path  
            digest: Expected content digest
            data: Blob content (bytes or file-like object)
            size: Optional size hint for streaming
            
        Raises:
            OciDigestMismatch: If content doesn't match digest
            OciAuthError: If authentication fails
            OciError: For other registry errors
        """
        ...
    
    def blob_exists(self, repo: str, digest: str) -> bool:
        """
        Check if blob exists in repository.
        
        Args:
            repo: Repository path
            digest: Content digest to check
            
        Returns:
            True if blob exists, False otherwise
            
        Note:
            This method never raises exceptions - it returns False
            for any error condition (auth failure, network error, etc.)
            to maintain the same contract as the old BundleRegistryStore.
        """
        ...


__all__ = ["OciRegistry"]