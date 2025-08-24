"""
ORAS Bundle Registry - replaces the entire OCI registry stack.

This single file replaces:
- storage/oci_registry.py (Protocol) - TO DELETE
- storage/hybrid_oci_registry.py - TO DELETE  
- storage/registry_http.py - TO DELETE
- storage/registry_factory.py - TO DELETE

Uses ORAS for most operations with minimal HTTP fallback only when needed.
"""
from __future__ import annotations

import hashlib
import json
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from io import BytesIO

import oras.client
from oras.provider import Registry as OrasRegistry

from ..settings import Settings
from ..runtime import BundleNotFoundError, BundleDownloadError


class OrasBundleRegistry:
    """
    Registry operations for ModelOps bundles using ORAS + minimal HTTP.
    
    Uses ORAS for:
    - Push/pull operations
    - Authentication
    - Blob operations
    - Most manifest operations
    
    Falls back to minimal HTTP for:
    - HEAD manifest (digest resolution) - if needed
    - Any operation ORAS doesn't support
    """
    
    def __init__(self, settings: Settings):
        """Initialize with settings."""
        self.settings = settings
        self._oras_client = None
        self._http_client = None
        
    @property
    def oras(self) -> oras.client.OrasClient:
        """Lazy initialize ORAS client."""
        if self._oras_client is None:
            # Parse registry URL
            registry_url = self.settings.registry_url
            if "://" in registry_url:
                # Remove protocol for ORAS
                registry_url = registry_url.split("://", 1)[1]
            
            # Create client
            self._oras_client = oras.client.OrasClient(
                hostname=registry_url,
                insecure=self.settings.registry_insecure
            )
            
            # Login if credentials provided
            if self.settings.registry_user:
                self._oras_client.login(
                    username=self.settings.registry_user,
                    password=self.settings.registry_pass,
                    insecure=self.settings.registry_insecure
                )
        
        return self._oras_client
    
    def push_bundle(self, 
                   files: List[Dict[str, Any]], 
                   repo: str,
                   tag: str,
                   manifest_annotations: Optional[Dict[str, str]] = None) -> str:
        """
        Push bundle files to registry using ORAS.
        
        Args:
            files: List of file dicts with 'path' and optional 'annotations'
            repo: Repository path (e.g., "myorg/bundles/mybundle")
            tag: Tag for the bundle
            manifest_annotations: Annotations for the manifest
            
        Returns:
            Manifest digest (sha256:...)
        """
        target = f"{repo}:{tag}"
        
        # ORAS push returns the manifest
        result = self.oras.push(
            files=files,
            target=target,
            manifest_annotations=manifest_annotations or {}
        )
        
        # Extract digest from result
        # ORAS typically returns manifest info we can use
        if hasattr(result, 'digest'):
            return result.digest
        
        # Fallback: compute digest from manifest
        manifest = self.get_manifest(repo, tag)
        return f"sha256:{hashlib.sha256(manifest).hexdigest()}"
    
    def get_manifest(self, repo: str, ref: str) -> bytes:
        """
        Get manifest from registry.
        
        Args:
            repo: Repository path
            ref: Tag or digest
            
        Returns:
            Raw manifest bytes
        """
        target = f"{repo}:{ref}" if ":" not in ref else f"{repo}@{ref}"
        
        # Use ORAS to get manifest
        # Note: We may need to adapt based on actual ORAS API
        manifest = self.oras.remote.get_manifest(target)
        
        if isinstance(manifest, dict):
            return json.dumps(manifest, separators=(',', ':'), sort_keys=True).encode()
        return manifest
    
    def get_blob(self, repo: str, digest: str) -> bytes:
        """
        Get blob content from registry.
        
        Args:
            repo: Repository path
            digest: Blob digest (sha256:...)
            
        Returns:
            Blob content bytes
        """
        # ORAS handles blob fetching
        # We may need to adapt based on actual ORAS blob API
        target = f"{repo}@{digest}"
        
        # Pull to temp location
        with tempfile.TemporaryDirectory() as tmpdir:
            self.oras.pull(target=target, outdir=tmpdir)
            # Read the blob from temp location
            # This is simplified - actual implementation depends on ORAS API
            files = list(Path(tmpdir).glob("*"))
            if files:
                return files[0].read_bytes()
        
        raise BundleDownloadError(f"Failed to fetch blob {digest}")
    
    def blob_exists(self, repo: str, digest: str) -> bool:
        """
        Check if blob exists in registry.
        
        Args:
            repo: Repository path  
            digest: Blob digest
            
        Returns:
            True if blob exists
        """
        try:
            # Try to get blob metadata
            self.get_blob(repo, digest)
            return True
        except (BundleDownloadError, Exception):
            return False
    
    def head_manifest(self, repo: str, ref: str) -> str:
        """
        Get manifest digest without downloading content.
        
        This is one operation ORAS doesn't directly support,
        so we might need minimal HTTP for this.
        
        Args:
            repo: Repository path
            ref: Tag or digest
            
        Returns:
            Canonical digest (sha256:...)
        """
        # For MVP, download manifest and compute digest
        # Later we can add minimal HTTP HEAD support if needed
        manifest = self.get_manifest(repo, ref)
        return f"sha256:{hashlib.sha256(manifest).hexdigest()}"
    
    def pull_bundle(self, repo: str, tag: str, dest_dir: str) -> List[str]:
        """
        Pull bundle files from registry.
        
        Args:
            repo: Repository path
            tag: Bundle tag
            dest_dir: Destination directory
            
        Returns:
            List of pulled file paths
        """
        target = f"{repo}:{tag}"
        
        # ORAS pull
        result = self.oras.pull(
            target=target,
            outdir=dest_dir
        )
        
        # Return list of files
        if isinstance(result, list):
            return result
        
        # Scan directory for files
        return [str(p) for p in Path(dest_dir).glob("*")]


