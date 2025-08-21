"""
ContentProvider implementation using storage interfaces.

This module provides the ORAS+External ContentProvider that uses
storage interfaces for registry and external storage operations.
"""
from __future__ import annotations

from typing import Iterable

from modelops_contracts.artifacts import ResolvedBundle

from ..runtime_types import ContentProvider, MatEntry
from ..storage.base import ExternalStore, OrasStore

__all__ = ["OrasExternalProvider"]


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
        
        Stage 2: Intentionally unimplemented (wired for compile-time only).
        Stage 3 will use self._oras/self._external to produce MatEntry items.
        
        Args:
            resolved: The resolved bundle with manifest information
            layers: List of layer names to enumerate
            
        Yields:
            MatEntry objects for each file to materialize
        """
        # Stage 2: intentionally unimplemented (compile-time wiring only)
        # Stage 3 will use self._oras/self._external here to produce entries
        raise NotImplementedError("Implemented in Stage 3")

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

