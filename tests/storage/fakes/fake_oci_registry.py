"""
Fake OCI Registry implementation for testing.

This implementation follows the repo-aware OciRegistry protocol and stores
manifests and blobs in memory for testing purposes.
"""
from __future__ import annotations

import hashlib
import re
from typing import Dict, Union, BinaryIO

from modelops_bundles.storage.oci_registry import OciRegistry

# Regex for validating SHA256 digests
_DIGEST_RE = re.compile(r"^sha256:[a-f0-9]{64}$")

__all__ = ["FakeOciRegistry"]


class FakeOciRegistry(OciRegistry):
    """
    In-memory OCI registry implementation for testing.
    
    This is a test double; not for production use.
    Implements the repo-aware OciRegistry protocol.
    """
    
    def __init__(self) -> None:
        # Storage keyed by repo
        self._manifests: Dict[str, Dict[str, bytes]] = {}  # repo -> {ref: manifest_bytes}
        self._blobs: Dict[str, Dict[str, bytes]] = {}      # repo -> {digest: blob_bytes}
        self._tags: Dict[str, Dict[str, str]] = {}         # repo -> {tag: digest}
    
    def _ensure_repo(self, repo: str) -> None:
        """Ensure repo exists in storage."""
        if repo not in self._manifests:
            self._manifests[repo] = {}
        if repo not in self._blobs:
            self._blobs[repo] = {}
        if repo not in self._tags:
            self._tags[repo] = {}
    
    def head_manifest(self, repo: str, ref: str) -> str:
        """
        Get canonical digest of manifest without fetching content.
        
        Args:
            repo: Repository name
            ref: Tag or digest reference
            
        Returns:
            Canonical manifest digest (sha256:...)
            
        Raises:
            KeyError: If manifest not found
        """
        self._ensure_repo(repo)
        
        # If ref is already a digest, return it
        if ref.startswith("sha256:"):
            if ref not in self._manifests[repo]:
                raise KeyError(f"Manifest not found: {repo}@{ref}")
            return ref
        
        # Look up tag -> digest mapping
        if ref not in self._tags[repo]:
            raise KeyError(f"Tag not found: {repo}:{ref}")
        
        return self._tags[repo][ref]
    
    def get_manifest(self, repo: str, ref: str) -> bytes:
        """
        Retrieve manifest content by tag or digest.
        
        Args:
            repo: Repository name
            ref: Tag or digest reference
            
        Returns:
            Manifest content as bytes
            
        Raises:
            KeyError: If manifest not found
        """
        self._ensure_repo(repo)
        
        # Resolve ref to digest if needed
        if ref.startswith("sha256:"):
            digest = ref
        else:
            if ref not in self._tags[repo]:
                raise KeyError(f"Tag not found: {repo}:{ref}")
            digest = self._tags[repo][ref]
        
        # Get manifest by digest
        if digest not in self._manifests[repo]:
            raise KeyError(f"Manifest not found: {repo}@{digest}")
        
        return self._manifests[repo][digest]
    
    def put_manifest(self, repo: str, media_type: str, payload: bytes, tag: str) -> str:
        """
        Store manifest and tag it.
        
        Args:
            repo: Repository name
            media_type: Manifest media type
            payload: Manifest content bytes
            tag: Tag to apply to manifest
            
        Returns:
            Canonical manifest digest (sha256:...)
        """
        self._ensure_repo(repo)
        
        # Compute canonical digest
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
        
        # Store manifest by digest
        self._manifests[repo][digest] = payload
        
        # Tag the manifest
        self._tags[repo][tag] = digest
        
        return digest
    
    def get_blob(self, repo: str, digest: str) -> bytes:
        """
        Retrieve blob content by digest.
        
        Args:
            repo: Repository name
            digest: Content digest (sha256:...)
            
        Returns:
            Blob content as bytes
            
        Raises:
            KeyError: If blob not found
        """
        if not _DIGEST_RE.match(digest):
            raise ValueError(f"Invalid digest format: {digest}")
        
        self._ensure_repo(repo)
        
        if digest not in self._blobs[repo]:
            raise KeyError(f"Blob not found: {repo}@{digest}")
        
        return self._blobs[repo][digest]
    
    def put_blob(self, repo: str, digest: str, data: Union[bytes, BinaryIO], 
                 size: int | None = None) -> None:
        """
        Store blob content under digest.
        
        Args:
            repo: Repository name
            digest: Content digest (sha256:...)
            data: Blob content as bytes or readable stream
            size: Optional size hint (ignored for testing)
        """
        if not _DIGEST_RE.match(digest):
            raise ValueError(f"Invalid digest format: {digest}")
        
        self._ensure_repo(repo)
        
        # Read data if it's a stream
        if hasattr(data, 'read'):
            blob_data = data.read()
        else:
            blob_data = data
        
        # Verify digest matches content
        computed_digest = f"sha256:{hashlib.sha256(blob_data).hexdigest()}"
        if digest != computed_digest:
            raise ValueError(f"Digest mismatch: expected {digest}, got {computed_digest}")
        
        # Store blob
        self._blobs[repo][digest] = blob_data
    
    def ensure_blob(self, repo: str, digest: str, data: bytes) -> None:
        """
        Ensure blob exists with content (test utility).
        
        Args:
            repo: Repository name
            digest: Content digest
            data: Blob content bytes
        """
        # Just delegate to put_blob for testing
        self.put_blob(repo, digest, data)
    
    def clear(self) -> None:
        """Clear all stored data (test utility)."""
        self._manifests.clear()
        self._blobs.clear()
        self._tags.clear()
    
    def clear_repo(self, repo: str) -> None:
        """Clear data for specific repository (test utility)."""
        if repo in self._manifests:
            del self._manifests[repo]
        if repo in self._blobs:
            del self._blobs[repo]
        if repo in self._tags:
            del self._tags[repo]