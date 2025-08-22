"""
ORAS registry adapter for OCI distribution.

Implements BundleRegistryStore protocol using oras-py.
Handles authentication, media type validation, and OCI spec compliance.
"""
from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from pathlib import Path
from typing import Optional
import re

from ..settings import Settings
from ..storage.base import BundleRegistryStore

# Import media types from contracts (single source of truth)
from modelops_contracts.artifacts import (
    BUNDLE_MANIFEST,
    LAYER_INDEX, 
    EXTERNAL_REF,
    OCI_MANIFEST,
)

__all__ = ["OrasAdapter"]

logger = logging.getLogger(__name__)

# Re-export known media types from contracts (single source of truth)
KNOWN_MEDIA_TYPES = {
    BUNDLE_MANIFEST,
    LAYER_INDEX,
    EXTERNAL_REF,
    OCI_MANIFEST,
}


class OrasAdapter(BundleRegistryStore):
    """
    BundleRegistryStore adapter for OCI registry operations.
    
    Requires oras-py for OCI spec compliance.
    Handles authentication via Docker config or environment variables.
    """
    
    def __init__(self, *, settings: Settings) -> None:
        """
        Initialize ORAS adapter with settings.
        
        Args:
            settings: Settings containing registry configuration
        """
        self._settings = settings
        
        # Require oras-py (HTTP fallback disabled for OCI spec compliance)
        try:
            import oras.client  # type: ignore
            
            # Setup authentication parameters
            auth_params = {
                "hostname": self._settings.registry_url,
                "insecure": self._settings.registry_insecure
            }
            
            # Add explicit credentials if available
            if self._settings.registry_user and self._settings.registry_pass:
                auth_params["username"] = self._settings.registry_user
                auth_params["password"] = self._settings.registry_pass
                logger.debug("Using explicit username/password auth with oras-py")
            else:
                # Try Docker config
                docker_auth = self._read_docker_auth()
                if docker_auth:
                    username, password = docker_auth
                    auth_params["username"] = username
                    auth_params["password"] = password
                    logger.debug("Using Docker config auth with oras-py")
                else:
                    logger.debug(f"No auth configured for {self._settings.registry_url}, proceeding anonymous")
            
            self._oras_client = oras.client.OrasClient(**auth_params)
            logger.debug(f"ORAS adapter using oras-py for {settings.registry_url}")
        except ImportError:
            raise ImportError("oras-py package is required for ORAS operations (HTTP fallback disabled for OCI spec compliance)")
        
        # Log configuration
        logger.debug(f"ORAS adapter timeout: {settings.http_timeout_s}s, retry: {settings.http_retry}, insecure: {settings.registry_insecure}")
    
    def _read_docker_auth(self) -> Optional[tuple[str, str]]:
        """Read Docker authentication from config file."""
        docker_config_dir = os.getenv("DOCKER_CONFIG", str(Path.home() / ".docker"))
        config_path = Path(docker_config_dir) / "config.json"
        
        if not config_path.exists():
            return None
        
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Look for auth entry matching our registry
            auths = config.get("auths", {})
            
            # Try exact match first, then remove protocol if present
            registry_key = self._settings.registry_url
            if registry_key not in auths:
                # Try without https:// prefix
                if registry_key.startswith("https://"):
                    registry_key = registry_key[8:]
                if registry_key not in auths:
                    return None
            
            auth_entry = auths[registry_key]
            
            # Handle auth field (base64 encoded username:password)
            if "auth" in auth_entry:
                try:
                    decoded = base64.b64decode(auth_entry["auth"]).decode()
                    if ":" in decoded:
                        username, password = decoded.split(":", 1)
                        return (username, password)
                except Exception:
                    pass
            
            # Handle username/password fields directly
            if "username" in auth_entry and "password" in auth_entry:
                return (auth_entry["username"], auth_entry["password"])
            
        except Exception as e:
            logger.debug(f"Failed to read Docker config: {e}")
        
        return None
    
    def _validate_media_type(self, media_type: str) -> None:
        """Validate media type is allowed."""
        if media_type not in KNOWN_MEDIA_TYPES:
            raise ValueError(f"Unsupported media type: {media_type}")
    
    def _validate_digest_format(self, digest: str) -> None:
        """Validate digest format."""
        if not re.match(r"^sha256:[a-f0-9]{64}$", digest):
            raise ValueError(f"Invalid digest format: {digest}")
    
    def _retry_with_backoff(self, func, *args, **kwargs):
        """Execute function with retry and exponential backoff."""
        # Let oras-py handle its own retries, just call the function directly
        return func(*args, **kwargs)
    
    def blob_exists(self, digest: str) -> bool:
        """
        Check if blob exists in registry.
        
        Args:
            digest: Content digest (e.g., "sha256:abc123...")
            
        Returns:
            True if blob exists, False otherwise
        """
        try:
            self._validate_digest_format(digest)
            return self._oras_client.blob_exists(digest)
        except Exception:
            # Never throw from blob_exists per contract
            return False
    
    
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
        self._validate_digest_format(digest)
        
        def _get_blob_impl():
            try:
                return self._oras_client.pull_blob(digest)
            except Exception as e:
                if "404" in str(e) or "not found" in str(e).lower():
                    raise KeyError(digest)
                raise OSError(f"ORAS blob fetch error: {e}")
        
        return self._retry_with_backoff(_get_blob_impl)
    
    
    def put_blob(self, digest: str, data: bytes) -> None:
        """
        Store blob content under digest.
        
        Args:
            digest: Content digest (e.g., "sha256:abc123...")
            data: Blob content bytes
            
        Raises:
            ValueError: If digest format is invalid or doesn't match data
        """
        self._validate_digest_format(digest)
        
        # Verify digest matches data
        computed = "sha256:" + hashlib.sha256(data).hexdigest()
        if digest != computed:
            raise ValueError(f"Digest mismatch: expected {digest}, got {computed}")
        
        def _put_blob_impl():
            try:
                self._oras_client.push_blob(data, digest)
            except Exception as e:
                if "401" in str(e) or "403" in str(e):
                    raise OSError("Registry authentication failed")
                raise OSError(f"ORAS blob upload error: {e}")
        
        self._retry_with_backoff(_put_blob_impl)
    
    
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
        def _get_manifest_impl():
            try:
                return self._oras_client.get_manifest(digest_or_ref)
            except Exception as e:
                if "404" in str(e) or "not found" in str(e).lower():
                    raise KeyError(digest_or_ref)
                raise OSError(f"ORAS manifest fetch error: {e}")
        
        return self._retry_with_backoff(_get_manifest_impl)
    
    
    def put_manifest(self, media_type: str, payload: bytes) -> str:
        """
        Store manifest and return its digest.
        
        Args:
            media_type: MIME type of manifest
            payload: Manifest content bytes
            
        Returns:
            Canonical digest (e.g., "sha256:abc123...")
            
        Raises:
            ValueError: If manifest is invalid or media type unsupported
        """
        self._validate_media_type(media_type)
        
        # Compute canonical digest
        digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        
        def _put_manifest_impl():
            try:
                # oras-py might return the digest, or we return our computed one
                self._oras_client.push_manifest(payload, media_type)
                return digest
            except Exception as e:
                if "401" in str(e) or "403" in str(e):
                    raise OSError("Registry authentication failed")
                raise OSError(f"ORAS manifest upload error: {e}")
        
        return self._retry_with_backoff(_put_manifest_impl)
    
    
    
