"""
ContentProvider implementation using storage interfaces.

This module provides the ORAS+External ContentProvider that uses
storage interfaces for registry and external storage operations.
"""
from __future__ import annotations

import json
from typing import Iterable

from modelops_contracts.artifacts import ResolvedBundle, LAYER_INDEX

from ..runtime_types import ContentProvider, MatEntry
from ..storage.base import ExternalStore, OrasStore
from ..path_safety import safe_relpath

__all__ = ["OrasExternalProvider", "default_provider_from_env"]


def _short_digest(digest: str) -> str:
    """Truncate digest for friendlier error messages."""
    if digest.startswith("sha256:") and len(digest) > 18:
        return digest[:18] + "..."
    return digest


class OrasExternalProvider(ContentProvider):
    """
    ContentProvider that uses storage interfaces for ORAS and external operations.
    
    This provider accepts storage implementations via dependency injection,
    enabling testing with fakes and production use with real implementations.
    
    Stage 2: Constructor accepts stores, iter_entries() is implemented in Stage 3.
    """
    
    def __init__(self, *, oras: OrasStore, external: ExternalStore) -> None:
        """
        Initialize the provider with storage interfaces.
        
        Args:
            oras: ORAS storage interface for registry operations
            external: External storage interface for blob operations
        """
        self._oras = oras
        self._external = external
    
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
        for layer in layers:
            # 1. Check layer has index
            if layer not in resolved.layer_indexes:
                raise ValueError(f"resolved missing index for layer '{layer}'")
            
            # 2. Fetch and validate index
            index_digest = resolved.layer_indexes[layer]
            try:
                payload = self._oras.get_manifest(index_digest)
            except KeyError:
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
                
                # Exactly one of digest or external
                has_digest = "digest" in entry
                has_external = "external" in entry
                
                if has_digest and has_external:
                    raise ValueError(f"entry must have exactly one of 'digest' or 'external' for path '{path}'")
                elif not has_digest and not has_external:
                    raise ValueError(f"entry must have exactly one of 'digest' or 'external' for path '{path}'")
                
                if has_external:
                    ext = entry["external"]
                    # Validate required external fields
                    required_fields = ["uri", "sha256", "size"]
                    missing = [f for f in required_fields if f not in ext]
                    if missing:
                        raise ValueError(f"external entry missing fields {missing} for path '{path}' in layer '{layer}'")
                    
                    yield MatEntry(
                        path=path,
                        layer=layer,
                        kind="external",
                        content=None,
                        uri=ext["uri"],
                        sha256=ext["sha256"],
                        size=ext["size"],
                        tier=ext.get("tier")  # Optional
                    )
                else:  # has_digest
                    digest = entry["digest"]
                    
                    # Validate digest format
                    if not digest or not isinstance(digest, str):
                        raise ValueError(f"invalid digest for layer '{layer}' path '{path}': must be non-empty string")
                    if not digest.startswith("sha256:") or len(digest) != 71:
                        raise ValueError(f"invalid digest format for layer '{layer}' path '{path}': expected 'sha256:<64 hex chars>', got '{digest}'")
                    hex_part = digest[7:]  # Remove "sha256:" prefix
                    if not all(c in '0123456789abcdef' for c in hex_part):
                        raise ValueError(f"invalid digest format for layer '{layer}' path '{path}': contains non-hex characters")
                    
                    try:
                        blob = self._oras.get_blob(digest)
                    except KeyError:
                        raise ValueError(f"missing blob {_short_digest(digest)} for layer '{layer}' path '{path}'")
                    
                    yield MatEntry(
                        path=path,
                        layer=layer,
                        kind="oras",
                        content=blob
                    )

    def fetch_external(self, entry: MatEntry) -> bytes:
        """
        Fetch external content using the external store.
        
        Stage 2: This path will be exercised only if entries are manually injected.
        
        Args:
            entry: MatEntry with external metadata
            
        Returns:
            Content bytes from external storage
            
        Raises:
            ValueError: If entry is missing required external metadata
        """
        if entry.uri is None:
            raise ValueError("external entry missing uri")
        return self._external.get(entry.uri)


def default_provider_from_env() -> OrasExternalProvider:
    """
    Create OrasExternalProvider with real adapters from environment settings.
    
    This is the standard entry point for production use and CLI integration.
    Loads settings from environment variables and creates real ORAS and 
    external storage adapters.
    
    Returns:
        OrasExternalProvider configured with real adapters
        
    Raises:
        ValueError: If required configuration is missing
        ImportError: If required adapter dependencies are missing
        
    Example:
        >>> provider = default_provider_from_env()
        >>> # Use with runtime.materialize(ref, dest, provider=provider)
    """
    from ..settings import load_settings_from_env
    from ..storage.oras import OrasAdapter
    from ..storage.object_store import AzureExternalAdapter
    
    settings = load_settings_from_env()
    
    # Create real adapters
    oras_adapter = OrasAdapter(settings=settings)
    external_adapter = AzureExternalAdapter(settings=settings)
    
    return OrasExternalProvider(
        oras=oras_adapter,
        external=external_adapter
    )

