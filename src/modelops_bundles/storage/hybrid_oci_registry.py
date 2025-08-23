"""
Hybrid OCI Registry implementation.

Uses HTTP for manifest operations (full control over media types and digests)
and ORAS SDK for blob operations (streaming, retries, chunked uploads).
"""
from __future__ import annotations

import hashlib
import logging
from typing import BinaryIO, Union

from ..settings import Settings
from .oci_errors import OciAuthError, OciDigestMismatch, OciError, OciNotFound
from .registry_http import RegistryHTTP

logger = logging.getLogger(__name__)


class HybridOciRegistry:
    """
    MVP OCI registry implementation using HTTP + SDK hybrid approach.
    
    Design rationale:
    - HTTP for manifests: Full control over media types and digest validation
    - SDK for blobs: Leverage streaming, retries, and chunked uploads
    - HTTP fallback: If SDK can't handle pre-computed digests
    """
    
    def __init__(self, settings: Settings):
        """
        Initialize hybrid registry with settings.
        
        Args:
            settings: Registry configuration including URL, auth, etc.
        """
        self._settings = settings
        self._http = self._create_http_client(settings)
        self._sdk = self._create_sdk_client(settings)
    
    def _create_http_client(self, settings: Settings):
        """Create HTTP client for manifest operations."""
        # For MVP, we'll work with the existing RegistryHTTP interface
        # which expects bundle names rather than full repo paths
        registry_url = settings.registry_url
        if "://" in registry_url:
            hostname = registry_url.split("://", 1)[1]
        else:
            hostname = registry_url
        
        return RegistryHTTP(
            registry=hostname,
            namespace=settings.registry_repo,
            insecure=(settings.registry_insecure or 
                     registry_url.startswith('http://localhost') or 
                     registry_url.startswith('http://127.0.0.1'))
        )
    
    def _create_sdk_client(self, settings: Settings):
        """Create ORAS SDK client for blob operations."""
        try:
            from oras.provider import Registry
        except ImportError:
            try:
                from oras import Registry
            except ImportError:
                raise ImportError("ORAS SDK not found. Install with: pip install oras")
        
        # Extract hostname for SDK
        registry_url = settings.registry_url
        if "://" in registry_url:
            hostname = registry_url.split("://", 1)[1] 
        else:
            hostname = registry_url
        
        # SDK needs hostname without protocol
        return Registry(hostname=hostname)
    
    def _extract_bundle_name(self, repo: str) -> str:
        """Extract bundle name from full repo path."""
        # repo format: "namespace/bundles/bundle-name"
        # We need to extract "bundle-name"
        if "/bundles/" not in repo:
            raise ValueError(f"Invalid repo path format: {repo}")
        return repo.split("/bundles/", 1)[1]
    
    # Manifest operations via HTTP (full control)
    
    def head_manifest(self, repo: str, ref: str) -> str:
        """HEAD manifest and return Docker-Content-Digest."""
        try:
            # Extract bundle name from repo path and use HEAD request
            bundle_name = self._extract_bundle_name(repo)
            return self._http.head_manifest(bundle_name, ref)
        except KeyError as e:
            raise OciNotFound(f"Manifest not found: {repo}:{ref}") from e
        except Exception as e:
            if "401" in str(e) or "403" in str(e):
                raise OciAuthError(f"Authentication failed for {repo}:{ref}: {e}") from e
            raise OciError(f"Failed to get manifest digest for {repo}:{ref}: {e}") from e
    
    def get_manifest(self, repo: str, ref: str) -> bytes:
        """GET manifest content."""
        try:
            # Extract bundle name and use HTTP client
            bundle_name = self._extract_bundle_name(repo)
            _, manifest_dict = self._http.resolve_tag(bundle_name, ref)
            
            # Convert manifest dict back to canonical JSON bytes
            import json
            manifest_json = json.dumps(manifest_dict, sort_keys=True, 
                                     separators=(',', ':'), ensure_ascii=True)
            return manifest_json.encode('utf-8')
        except KeyError as e:
            raise OciNotFound(f"Manifest not found: {repo}:{ref}") from e
        except Exception as e:
            if "401" in str(e) or "403" in str(e):
                raise OciAuthError(f"Authentication failed for {repo}:{ref}: {e}") from e
            raise OciError(f"Failed to get manifest for {repo}:{ref}: {e}") from e
    
    def put_manifest(self, repo: str, media_type: str, payload: bytes, tag: str) -> str:
        """
        PUT manifest and validate digest.
        
        TODO: This needs HTTP PUT support in RegistryHTTP to be fully implemented.
        For MVP, we'll focus on read operations first.
        """
        raise NotImplementedError(
            "put_manifest requires extending RegistryHTTP with PUT support. "
            "This will be implemented in the next phase."
        )
    
    # Blob operations via SDK (streaming and retries)
    
    def get_blob(self, repo: str, digest: str) -> bytes:
        """GET blob content via HTTP."""
        try:
            bundle_name = self._extract_bundle_name(repo)
            return self._http.get_blob_bytes(bundle_name, digest)
        except Exception as e:
            if "404" in str(e):
                raise OciNotFound(f"Blob not found: {repo}@{digest}") from e
            elif "401" in str(e) or "403" in str(e):
                raise OciAuthError(f"Authentication failed for {repo}@{digest}: {e}") from e
            raise OciError(f"Failed to get blob for {repo}@{digest}: {e}") from e
    
    def put_blob(self, repo: str, digest: str, data: Union[bytes, BinaryIO], 
                 size: int | None = None) -> None:
        """PUT blob."""
        # TODO: Implement blob operations  
        # For MVP, we'll focus on manifest operations first
        raise NotImplementedError(
            "put_blob requires SDK integration or HTTP blob support. "
            "This will be implemented in the next phase."
        )
    
    def blob_exists(self, repo: str, digest: str) -> bool:
        """Check if blob exists (never raises)."""
        # TODO: Implement blob operations
        # For MVP, return False to indicate blob operations not yet supported
        logger.debug(f"blob_exists not yet implemented, returning False for {repo}@{digest}")
        return False
    
    # Helper methods
    
    def ensure_blob(self, repo: str, digest: str, data: Union[bytes, BinaryIO], 
                   size: int | None = None) -> None:
        """
        Upload blob only if it doesn't already exist.
        
        TODO: This requires blob operations to be implemented.
        """
        raise NotImplementedError(
            "ensure_blob requires blob operations to be implemented first."
        )


__all__ = ["HybridOciRegistry"]