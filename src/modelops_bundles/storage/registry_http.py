"""
Registry HTTP Client for OCI Distribution API.

Provides HTTP-based registry operations with proper Docker Registry v2 auth flow,
specifically optimized for resolve operations that need canonical digests.
"""
from __future__ import annotations

import base64
import json
import re
import time
from pathlib import Path
from typing import Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..runtime import BundleNotFoundError, BundleDownloadError, UnsupportedMediaType

# OCI media types we accept for manifests (in order of preference)
ACCEPTED_MANIFEST_TYPES = [
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.oci.artifact.manifest.v1+json", 
    "application/vnd.docker.distribution.manifest.v2+json"
]

# Bundle-specific media types
BUNDLE_MANIFEST_TYPE = "application/vnd.modelops.bundle.manifest+json"
LAYER_INDEX_TYPE = "application/vnd.modelops.layer+json"


class DockerAuth:
    """Handle Docker Registry authentication from config files."""
    
    def __init__(self, config_path: Optional[Path] = None):
        self.config_path = config_path or Path.home() / ".docker" / "config.json"
        self._config_cache: Optional[dict] = None
        self._config_mtime: Optional[float] = None
    
    def get_credentials(self, registry: str) -> Optional[Tuple[str, str]]:
        """
        Get credentials for registry from Docker config.
        
        Returns: (username, password) or None if not found
        """
        config = self._load_config()
        if not config:
            return None
        
        auths = config.get("auths", {})
        
        # Try exact match first
        registry_key = registry
        if registry_key not in auths:
            # Try with https:// prefix
            registry_key = f"https://{registry}"
            if registry_key not in auths:
                # Try without protocol
                registry_key = registry.replace("https://", "").replace("http://", "")
                if registry_key not in auths:
                    return None
        
        auth_entry = auths[registry_key]
        
        # Handle base64 encoded auth field
        if "auth" in auth_entry:
            try:
                decoded = base64.b64decode(auth_entry["auth"]).decode()
                if ":" in decoded:
                    return tuple(decoded.split(":", 1))
            except Exception:
                pass
        
        # Handle username/password fields
        if "username" in auth_entry and "password" in auth_entry:
            return (auth_entry["username"], auth_entry["password"])
        
        return None
    
    def _load_config(self) -> Optional[dict]:
        """Load Docker config with caching and mtime checking."""
        if not self.config_path.exists():
            return None
        
        try:
            current_mtime = self.config_path.stat().st_mtime
            
            # Use cached version if file hasn't changed
            if (self._config_cache is not None and 
                self._config_mtime is not None and
                current_mtime == self._config_mtime):
                return self._config_cache
            
            # Load fresh config
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            
            self._config_cache = config
            self._config_mtime = current_mtime
            return config
            
        except Exception:
            return None


class RegistryHTTP:
    """
    HTTP client for OCI Distribution API operations.
    
    Implements proper Docker Registry v2 auth flow with Bearer token support,
    HTTP/2 for performance, and robust retry handling.
    """
    
    def __init__(self, registry: str, namespace: str, auth: Optional[DockerAuth] = None,
                 insecure: bool = False):
        """
        Initialize registry HTTP client.
        
        Args:
            registry: Registry hostname (e.g., "localhost:5555", "ghcr.io")
            namespace: Repository namespace (e.g., "modelops")
            auth: Docker auth handler (defaults to standard Docker config)
            insecure: Allow HTTP for development registries
        """
        self.registry = registry
        self.namespace = namespace
        self.auth = auth or DockerAuth()
        self.insecure = insecure
        
        # Determine base URL
        if insecure and not registry.startswith("http"):
            self.base_url = f"http://{registry}"
        elif not registry.startswith("http"):
            self.base_url = f"https://{registry}"
        else:
            self.base_url = registry
        
        # HTTP client with proper timeouts
        self.client = httpx.Client(
            http2=False,  # Disable HTTP/2 to avoid h2 dependency
            timeout=httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=5.0),
            follow_redirects=True,
            verify=not insecure,
            headers={"User-Agent": "modelops-bundles/0.1.0"}
        )
        
        # Token cache: {service/scope: (token, expiry_timestamp)}
        self._token_cache: Dict[str, Tuple[str, float]] = {}
    
    def resolve_tag(self, bundle_name: str, tag: str) -> Tuple[str, dict]:
        """
        Resolve tag to canonical digest and OCI manifest.
        
        Args:
            bundle_name: Bundle name (becomes repo path)
            tag: Version tag to resolve
            
        Returns:
            (canonical_digest, oci_manifest_dict)
            
        Raises:
            BundleNotFoundError: If bundle/tag not found
            BundleDownloadError: If network/auth errors
            UnsupportedMediaType: If manifest type not supported
        """
        repo = self._build_repo_path(bundle_name)
        url = f"/v2/{repo}/manifests/{tag}"
        
        headers = {
            "Accept": ", ".join(ACCEPTED_MANIFEST_TYPES)
        }
        
        try:
            response = self._request("GET", url, headers=headers)
            
            # Get canonical digest from Docker-Content-Digest header
            canonical_digest = response.headers.get("Docker-Content-Digest")
            if not canonical_digest:
                raise BundleDownloadError(
                    f"Registry did not return Docker-Content-Digest header for {repo}:{tag}"
                )
            
            # Parse OCI manifest
            try:
                manifest = response.json()
            except json.JSONDecodeError as e:
                raise UnsupportedMediaType(f"Invalid JSON in OCI manifest: {e}")
            
            # Validate it's a supported manifest type
            media_type = manifest.get("mediaType")
            if media_type not in ACCEPTED_MANIFEST_TYPES:
                raise UnsupportedMediaType(
                    f"Unsupported manifest media type: {media_type}. "
                    f"Expected one of: {', '.join(ACCEPTED_MANIFEST_TYPES)}"
                )
            
            return canonical_digest, manifest
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise BundleNotFoundError(f"Bundle not found: {repo}:{tag}")
            elif e.response.status_code in (401, 403):
                raise BundleDownloadError(f"Authentication failed for {repo}:{tag}")
            else:
                raise BundleDownloadError(f"Registry error {e.response.status_code}: {e}")
        except httpx.RequestError as e:
            raise BundleDownloadError(f"Network error resolving {repo}:{tag}: {e}")
    
    def get_blob_json(self, bundle_name: str, digest: str) -> dict:
        """
        Fetch blob by digest and parse as JSON.
        
        Used specifically for fetching bundle.manifest+json blobs.
        
        Args:
            bundle_name: Bundle name (becomes repo path)
            digest: Content digest (sha256:...)
            
        Returns:
            Parsed JSON dict
            
        Raises:
            BundleNotFoundError: If blob not found
            BundleDownloadError: If network/auth errors
            UnsupportedMediaType: If blob is not valid JSON
        """
        repo = self._build_repo_path(bundle_name)
        url = f"/v2/{repo}/blobs/{digest}"
        
        try:
            response = self._request("GET", url)
            
            try:
                return response.json()
            except json.JSONDecodeError as e:
                raise UnsupportedMediaType(f"Blob {digest} is not valid JSON: {e}")
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise BundleNotFoundError(f"Blob not found: {digest}")
            elif e.response.status_code in (401, 403):
                raise BundleDownloadError(f"Authentication failed for blob {digest}")
            else:
                raise BundleDownloadError(f"Registry error {e.response.status_code}: {e}")
        except httpx.RequestError as e:
            raise BundleDownloadError(f"Network error fetching blob {digest}: {e}")
    
    def get_blob_bytes(self, bundle_name: str, digest: str) -> bytes:
        """
        Fetch blob by digest as raw bytes.
        
        Args:
            bundle_name: Bundle name
            digest: Content digest (sha256:...)
            
        Returns:
            Raw blob content as bytes
            
        Raises:
            BundleNotFoundError: If blob not found
            BundleDownloadError: If network/auth errors
        """
        repo = self._build_repo_path(bundle_name)
        url = f"/v2/{repo}/blobs/{digest}"
        
        try:
            response = self._request("GET", url)
            return response.content
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise BundleNotFoundError(f"Blob not found: {digest}")
            elif e.response.status_code in (401, 403):
                raise BundleDownloadError(f"Authentication failed for blob {digest}")
            else:
                raise BundleDownloadError(f"Registry error {e.response.status_code}: {e}")
        except httpx.RequestError as e:
            raise BundleDownloadError(f"Network error fetching blob {digest}: {e}")
    
    def head_manifest(self, bundle_name: str, ref: str) -> str:
        """
        Get manifest digest without downloading content.
        
        Args:
            bundle_name: Bundle name
            ref: Tag or digest reference
            
        Returns:
            Docker-Content-Digest header value
        """
        repo = self._build_repo_path(bundle_name)
        url = f"/v2/{repo}/manifests/{ref}"
        
        headers = {
            "Accept": ", ".join(ACCEPTED_MANIFEST_TYPES)
        }
        
        try:
            response = self._request("HEAD", url, headers=headers)
            
            canonical_digest = response.headers.get("Docker-Content-Digest")
            if not canonical_digest:
                raise BundleDownloadError(
                    f"Registry did not return Docker-Content-Digest header for {repo}:{ref}"
                )
            
            return canonical_digest
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise BundleNotFoundError(f"Bundle not found: {repo}:{ref}")
            else:
                raise BundleDownloadError(f"Registry error {e.response.status_code}: {e}")
        except httpx.RequestError as e:
            raise BundleDownloadError(f"Network error: {e}")
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type((httpx.ConnectTimeout, httpx.ReadTimeout, httpx.TimeoutException))
    )
    def _request(self, method: str, path: str, headers: Optional[dict] = None, **kwargs) -> httpx.Response:
        """
        Make HTTP request with transparent Bearer token auth flow.
        
        Handles 401 responses by:
        1. Parsing WWW-Authenticate header for Bearer realm/service/scope
        2. Looking up credentials in Docker config
        3. Exchanging credentials for Bearer token
        4. Retrying original request with Authorization header
        5. Caching tokens per service/scope
        """
        url = urljoin(self.base_url, path)
        request_headers = headers or {}
        
        # Try request without auth first
        response = self.client.request(method, url, headers=request_headers, **kwargs)
        
        # If 401, try Bearer token flow
        if response.status_code == 401:
            auth_header = response.headers.get("WWW-Authenticate", "")
            if auth_header.startswith("Bearer "):
                token = self._handle_bearer_auth(auth_header)
                if token:
                    request_headers["Authorization"] = f"Bearer {token}"
                    response = self.client.request(method, url, headers=request_headers, **kwargs)
        
        # Raise for HTTP errors
        response.raise_for_status()
        return response
    
    def _handle_bearer_auth(self, www_authenticate: str) -> Optional[str]:
        """
        Handle Bearer token authentication flow.
        
        Parses WWW-Authenticate header, gets credentials, exchanges for token.
        """
        # Parse Bearer realm/service/scope from header
        # Format: Bearer realm="...",service="...",scope="..."
        bearer_params = {}
        
        # Simple regex parsing (could use proper parser but this works for common cases)
        for match in re.finditer(r'(\w+)="([^"]*)"', www_authenticate):
            bearer_params[match.group(1)] = match.group(2)
        
        realm = bearer_params.get("realm")
        service = bearer_params.get("service")
        scope = bearer_params.get("scope")
        
        if not all([realm, service]):
            return None
        
        # Check token cache
        cache_key = f"{service}:{scope or ''}"
        if cache_key in self._token_cache:
            token, expiry = self._token_cache[cache_key]
            if time.time() < expiry - 30:  # 30s buffer before expiry
                return token
        
        # Get credentials
        creds = self.auth.get_credentials(self.registry)
        if not creds:
            return None
        
        username, password = creds
        
        # Exchange for token
        try:
            auth_response = self.client.get(
                realm,
                auth=(username, password),
                params={"service": service, "scope": scope} if scope else {"service": service}
            )
            auth_response.raise_for_status()
            
            token_data = auth_response.json()
            token = token_data.get("token")
            
            if token:
                # Cache with expiry (default 1 hour if not specified)
                expires_in = token_data.get("expires_in", 3600)
                expiry = time.time() + expires_in
                self._token_cache[cache_key] = (token, expiry)
                
                return token
        
        except Exception:
            # Token exchange failed, continue without auth
            pass
        
        return None
    
    def _build_repo_path(self, bundle_name: str) -> str:
        """Build repository path for bundle."""
        return f"{self.namespace}/bundles/{bundle_name}"
    
    def close(self):
        """Close HTTP client."""
        self.client.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def build_repository_path(registry: str, namespace: str, bundle_name: str) -> str:
    """
    Centralized repository path building for consistency between HTTP and ORAS.
    
    Args:
        registry: Registry hostname
        namespace: Repository namespace
        bundle_name: Bundle name
        
    Returns:
        Full repository path
    """
    return f"{registry}/{namespace}/bundles/{bundle_name}"