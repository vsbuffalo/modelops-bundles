"""
ContentProvider implementation using storage interfaces.

This module provides the BundleContentProvider that uses
storage interfaces for registry and external storage operations.
"""
from __future__ import annotations

import json
from io import BytesIO
from typing import Iterable

from modelops_contracts.artifacts import ResolvedBundle, LAYER_INDEX

from ..runtime_types import ContentProvider, MatEntry, ByteStream
from ..storage.base import ExternalStore  
from ..storage.oras_bundle_registry import OrasBundleRegistry
from ..storage.repo_path import build_repo
from ..settings import Settings
from ..path_safety import safe_relpath

__all__ = ["BundleContentProvider", "create_provider_from_env"]


def _short_digest(digest: str) -> str:
    """Truncate digest for friendlier error messages."""
    if digest.startswith("sha256:") and len(digest) > 18:
        return digest[:18] + "..."
    return digest


class BundleContentProvider(ContentProvider):
    """
    A concrete class that implements ContentProvider that uses storage
    interfaces for bundle registry and external operations.
    
    This provider accepts storage implementations via dependency injection,
    enabling testing with fakes and production use with real implementations.
    
    Constructor accepts stores and implements iter_entries() interface.
    """
    
    def __init__(self, *, registry: OrasBundleRegistry, external: ExternalStore, settings: Settings) -> None:
        """
        Initialize the provider with storage interfaces.
        
        Args:
            registry: ORAS bundle registry for registry operations
            external: External storage interface for blob operations  
            settings: Settings for repository path construction
        """
        self._registry = registry
        self._external = external
        self._settings = settings
        self._current_repo: str | None = None  # Track repo context for lazy fetching
    
    def iter_entries(
        self,
        resolved: ResolvedBundle,
        layers: list[str]
    ) -> Iterable[MatEntry]:
        """
        Enumerate content from ORAS registry and external storage.
        
        Reads layer index manifests from ORAS store and yields MatEntry objects
        for both ORAS content (with bytes) and external references (with metadata).
        
        Args:
            resolved: The resolved bundle with manifest information  
            layers: List of layer names to enumerate
            
        Yields:
            MatEntry objects for each file to materialize
            
        Raises:
            ValueError: If layer index missing, invalid, or malformed
        """
        # Build repository path from bundle name
        if not resolved.ref.name:
            raise ValueError("ResolvedBundle must have ref.name for repo-aware operations")
        repo = build_repo(self._settings, resolved.ref.name)
        
        # Store repo context for lazy fetching
        self._current_repo = repo
        
        for layer in layers:
            # 1. Check layer has index
            if layer not in resolved.layer_indexes:
                raise ValueError(f"resolved missing index for layer '{layer}'")
            
            # 2. Fetch and validate index
            index_digest = resolved.layer_indexes[layer]
            try:
                payload = self._registry.get_blob(repo, index_digest)
            except Exception:
                raise ValueError(f"missing index manifest {_short_digest(index_digest)} for layer '{layer}'")
            
            try:
                doc = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as e:
                raise ValueError(f"invalid JSON in index for layer '{layer}': {e}")
                
            mt = doc.get("mediaType")
            if mt != LAYER_INDEX:
                raise ValueError(f"invalid mediaType for layer '{layer}': expected {LAYER_INDEX}, got {mt!r}")
            
            # 3. Process each entry
            for entry in doc.get("entries", []):
                path = entry.get("path")
                if not path:
                    raise ValueError(f"entry missing 'path' in layer '{layer}'")
                
                # Validate path early for reserved/unsafe paths
                try:
                    safe_relpath(path)
                except ValueError as e:
                    raise ValueError(f"invalid path '{path}' in layer '{layer}': {e}") from e
                
                # Check layer field if present
                if "layer" in entry and entry["layer"] != layer:
                    raise ValueError(f"entry layer mismatch in '{layer}': entry says '{entry['layer']}'")
                
                # Determine storage type - support both nested and flat formats
                oras_node = entry.get("oras")
                external_node = entry.get("external")
                has_legacy_digest = "digest" in entry
                
                # Validation: exactly one storage type
                storage_types = [oras_node is not None, external_node is not None, has_legacy_digest]
                if sum(storage_types) != 1:
                    raise ValueError(f"entry must have exactly one of 'oras', 'external', or legacy 'digest' for path '{path}'")
                
                if external_node:
                    ext = external_node
                    # Validate required external fields
                    required_fields = ["uri", "sha256", "size"]
                    missing = [f for f in required_fields if f not in ext]
                    if missing:
                        raise ValueError(f"external entry missing fields {missing} for path '{path}' in layer '{layer}'")
                    
                    # Create digest from SHA256 for consistency with ORAS entries
                    sha256_hex = ext["sha256"]
                    digest = f"sha256:{sha256_hex}"
                    
                    yield MatEntry(
                        path=path,
                        layer=layer,
                        kind="external",
                        size=ext["size"],
                        digest=digest,
                        sha256=sha256_hex,
                        uri=ext["uri"],
                        tier=ext.get("tier")  # Optional
                    )
                elif oras_node:
                    # Nested format from planner: {"oras": {"digest": ..., "size": ...}}
                    digest = oras_node.get("digest")
                    size = oras_node.get("size", 0)
                    
                    # Validate digest format
                    if not digest or not isinstance(digest, str):
                        raise ValueError(f"invalid digest in oras node for layer '{layer}' path '{path}': must be non-empty string")
                    if not digest.startswith("sha256:") or len(digest) != 71:
                        raise ValueError(f"invalid digest format in oras node for layer '{layer}' path '{path}': expected 'sha256:<64 hex chars>', got '{digest}'")
                    hex_part = digest[7:]  # Remove "sha256:" prefix
                    if not all(c in '0123456789abcdef' for c in hex_part):
                        raise ValueError(f"invalid digest format in oras node for layer '{layer}' path '{path}': contains non-hex characters")
                    
                    yield MatEntry(
                        path=path,
                        layer=layer,
                        kind="oras",
                        size=size,
                        digest=digest,
                        sha256=hex_part  # Store bare hex for verification
                    )
                else:  # has_legacy_digest
                    # Legacy flat format: {"digest": ...}
                    digest = entry["digest"]
                    
                    # Validate digest format
                    if not digest or not isinstance(digest, str):
                        raise ValueError(f"invalid digest for layer '{layer}' path '{path}': must be non-empty string")
                    if not digest.startswith("sha256:") or len(digest) != 71:
                        raise ValueError(f"invalid digest format for layer '{layer}' path '{path}': expected 'sha256:<64 hex chars>', got '{digest}'")
                    hex_part = digest[7:]  # Remove "sha256:" prefix
                    if not all(c in '0123456789abcdef' for c in hex_part):
                        raise ValueError(f"invalid digest format for layer '{layer}' path '{path}': contains non-hex characters")
                    
                    # Get size from entry, or estimate from digest (we don't fetch content here)
                    size = entry.get("size", 0)  # Fallback to 0 if not provided
                    
                    yield MatEntry(
                        path=path,
                        layer=layer,
                        kind="oras",
                        size=size,
                        digest=digest,
                        sha256=hex_part  # Store bare hex for verification
                    )

    def fetch_oras(self, entry: MatEntry) -> ByteStream:
        """
        Open a streaming source for a registry blob (lazy).
        
        Args:
            entry: MatEntry with kind=="oras" containing digest
            
        Returns:
            Streaming source for the blob content
            
        Raises:
            ValueError: If entry is not an ORAS entry or missing context
        """
        if entry.kind != "oras":
            raise ValueError(f"Expected ORAS entry, got {entry.kind}")
        
        if not entry.digest:
            raise ValueError("ORAS entry missing digest")
            
        if self._current_repo is None:
            raise ValueError("No repository context - call iter_entries() first")
        
        # Get blob content from registry
        # Note: Current registry.get_blob() returns bytes, not stream
        # We wrap in BytesIO to provide stream interface
        blob_bytes = self._registry.get_blob(self._current_repo, entry.digest)
        return BytesIO(blob_bytes)
    
    def fetch_external(self, entry: MatEntry) -> ByteStream:
        """
        Open a streaming source for an external storage object (lazy).
        
        Args:
            entry: MatEntry with external metadata
            
        Returns:
            Streaming source for external content
            
        Raises:
            ValueError: If entry is missing required external metadata
        """
        if entry.kind != "external":
            raise ValueError(f"Expected external entry, got {entry.kind}")
            
        if entry.uri is None:
            raise ValueError("External entry missing uri")
        
        # Get content from external store
        # Note: Current external.get() returns bytes, not stream
        # We wrap in BytesIO to provide stream interface
        content_bytes = self._external.get(entry.uri)
        return BytesIO(content_bytes)


def create_provider_from_env() -> BundleContentProvider:
    """
    Create BundleContentProvider with real adapters from environment settings.
    
    This factory creates a fresh provider instance every time without caching,
    ensuring test isolation and eliminating global state.
    
    Returns:
        BundleContentProvider configured with real adapters
        
    Raises:
        ValueError: If required configuration is missing
        ImportError: If required adapter dependencies are missing
        
    Example:
        >>> provider = create_provider_from_env()
        >>> # Use with runtime.materialize(ref, dest, provider=provider)
    """
    from ..settings import create_settings_from_env
    from ..storage.oras_bundle_registry import OrasBundleRegistry
    from ..storage.object_store import AzureExternalAdapter
    
    settings = create_settings_from_env()
    
    # Validate Azure configuration is present
    has_conn_str = bool(settings.az_connection_string)
    has_account_key = bool(settings.az_account and settings.az_key)
    
    if not (has_conn_str or has_account_key):
        raise ValueError("Azure authentication not configured. Set either AZURE_STORAGE_CONNECTION_STRING or (AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY)")
    
    # Create real adapters
    registry = OrasBundleRegistry(settings)
    external_adapter = AzureExternalAdapter(settings=settings)
    
    return BundleContentProvider(
        registry=registry,
        external=external_adapter,
        settings=settings
    )


