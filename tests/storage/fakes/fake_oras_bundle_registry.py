"""
Fake ORAS Bundle Registry implementation for testing.

This implementation mirrors OrasBundleRegistry interface but stores
everything in memory for testing purposes.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Dict, List, Optional, Any
from io import BytesIO

from modelops_bundles.runtime import BundleDownloadError

# Regex for validating SHA256 digests
_DIGEST_RE = re.compile(r"^sha256:[a-f0-9]{64}$")

__all__ = ["FakeOrasBundleRegistry"]


class FakeOrasBundleRegistry:
    """
    In-memory ORAS bundle registry implementation for testing.
    
    This is a test double that mirrors OrasBundleRegistry interface
    but stores everything in memory. Not for production use.
    """
    
    def __init__(self, settings=None):
        """Initialize with optional settings (ignored for fake)."""
        self.settings = settings
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
    
    def push_bundle(self, 
                   files: List[Dict[str, Any]], 
                   repo: str,
                   tag: str,
                   manifest_annotations: Optional[Dict[str, str]] = None) -> str:
        """
        Push bundle files to registry (fake implementation).
        
        Args:
            files: List of file dicts with 'path' and optional 'annotations'
            repo: Repository path (e.g., "myorg/bundles/mybundle")
            tag: Tag for the bundle
            manifest_annotations: Annotations for the manifest
            
        Returns:
            Manifest digest (sha256:...)
        """
        self._ensure_repo(repo)
        
        # Create a simple manifest structure
        manifest = {
            "schemaVersion": 2,
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "config": {
                "mediaType": "application/vnd.oci.empty.v1+json",
                "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
                "size": 2
            },
            "layers": [],
            "annotations": manifest_annotations or {}
        }
        
        # Store each file as a blob and add to manifest
        for file_info in files:
            if isinstance(file_info, dict):
                # Assume file has 'content' for testing
                content = file_info.get('content', b'')
                if isinstance(content, str):
                    content = content.encode('utf-8')
            else:
                # Simple bytes content
                content = file_info if isinstance(file_info, bytes) else b''
            
            digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
            self._blobs[repo][digest] = content
            
            manifest["layers"].append({
                "mediaType": "application/octet-stream",
                "digest": digest,
                "size": len(content)
            })
        
        # Store config blob
        config_content = b"{}"
        config_digest = "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        self._blobs[repo][config_digest] = config_content
        
        # Serialize and store manifest
        manifest_bytes = json.dumps(manifest, separators=(',', ':'), sort_keys=True).encode()
        manifest_digest = f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}"
        
        self._manifests[repo][manifest_digest] = manifest_bytes
        self._tags[repo][tag] = manifest_digest
        
        return manifest_digest
    
    def get_manifest(self, repo: str, ref: str) -> bytes:
        """
        Get manifest from registry.
        
        Args:
            repo: Repository path
            ref: Tag or digest
            
        Returns:
            Raw manifest bytes
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
    
    def get_blob(self, repo: str, digest: str) -> bytes:
        """
        Get blob content from registry.
        
        Args:
            repo: Repository path
            digest: Blob digest (sha256:...)
            
        Returns:
            Blob content bytes
        """
        if not _DIGEST_RE.match(digest):
            raise ValueError(f"Invalid digest format: {digest}")
        
        self._ensure_repo(repo)
        
        if digest not in self._blobs[repo]:
            raise BundleDownloadError(f"Failed to fetch blob {digest}")
        
        return self._blobs[repo][digest]
    
    def blob_exists(self, repo: str, digest: str) -> bool:
        """
        Check if blob exists in registry.
        
        Args:
            repo: Repository path  
            digest: Blob digest
            
        Returns:
            True if blob exists
        """
        if not _DIGEST_RE.match(digest):
            return False
        
        self._ensure_repo(repo)
        return digest in self._blobs[repo]
    
    def head_manifest(self, repo: str, ref: str) -> str:
        """
        Get manifest digest without downloading content.
        
        Args:
            repo: Repository path
            ref: Tag or digest
            
        Returns:
            Canonical digest (sha256:...)
        """
        self._ensure_repo(repo)
        
        # If ref is already a digest, validate it exists and return it
        if ref.startswith("sha256:"):
            if ref not in self._manifests[repo]:
                raise KeyError(f"Manifest not found: {repo}@{ref}")
            return ref
        
        # Look up tag -> digest mapping
        if ref not in self._tags[repo]:
            raise KeyError(f"Tag not found: {repo}:{ref}")
        
        return self._tags[repo][ref]
    
    def pull_bundle(self, repo: str, tag: str, dest_dir: str) -> List[str]:
        """
        Pull bundle files from registry (fake implementation).
        
        Args:
            repo: Repository path
            tag: Bundle tag
            dest_dir: Destination directory
            
        Returns:
            List of pulled file paths
        """
        # For testing, just return empty list or mock paths
        # Real implementation would extract files from manifest/blobs
        return []
    
    def put_blob(self, repo: str, digest: str, data: bytes) -> None:
        """
        Store blob content under digest (test utility).
        
        Args:
            repo: Repository name
            digest: Content digest (sha256:...)
            data: Blob content as bytes
        """
        if not _DIGEST_RE.match(digest):
            raise ValueError(f"Invalid digest format: {digest}")
        
        self._ensure_repo(repo)
        
        # Verify digest matches content
        computed_digest = f"sha256:{hashlib.sha256(data).hexdigest()}"
        if digest != computed_digest:
            raise ValueError(f"Digest mismatch: expected {digest}, got {computed_digest}")
        
        # Store blob
        self._blobs[repo][digest] = data
    
    def put_manifest(self, repo: str, media_type: str, payload: bytes, tag: str) -> str:
        """
        Store manifest and tag it (test utility for compatibility).
        
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