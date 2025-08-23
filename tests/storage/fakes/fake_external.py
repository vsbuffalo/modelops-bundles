"""
Fake external store implementation for testing.

This implementation explicitly subclasses ExternalStore to ensure interface changes
break CI immediately, preventing silent drift.
"""
from __future__ import annotations

import hashlib
from typing import Dict, Optional

from modelops_bundles.storage.base import ExternalStore, ExternalStat

__all__ = ["FakeExternalStore"]


class FakeExternalStore(ExternalStore):
    """
    In-memory object store keyed by URI for testing.
    
    This is a test double; not for production use.
    Computes and stores SHA256 hash on put() to ensure consistency.
    """
    
    def __init__(self) -> None:
        # Store data and metadata separately to ensure hash consistency
        self._objects: Dict[str, bytes] = {}
        self._metadata: Dict[str, ExternalStat] = {}

    def stat(self, uri: str) -> ExternalStat:
        """Get object metadata."""
        if uri not in self._metadata:
            raise FileNotFoundError(uri)
        return self._metadata[uri]

    def get(self, uri: str) -> bytes:
        """Retrieve object content."""
        if uri not in self._objects:
            raise FileNotFoundError(uri)
        return self._objects[uri]

    def put(
        self, 
        uri: str, 
        data: bytes, 
        *, 
        sha256: Optional[str] = None,
        tier: Optional[str] = None
    ) -> ExternalStat:
        """Store object and return metadata."""
        # Always compute hash from actual data
        computed_hash = hashlib.sha256(data).hexdigest()
        
        # Validate provided hash if given
        if sha256 is not None and sha256 != computed_hash:
            raise ValueError(
                f"sha256 mismatch: expected={sha256} actual={computed_hash}"
            )
        
        # Store data and metadata
        self._objects[uri] = data
        metadata = ExternalStat(
            uri=uri,
            size=len(data),
            sha256=computed_hash,
            tier=tier
        )
        self._metadata[uri] = metadata
        
        return metadata

    def clear(self) -> None:
        """Clear all stored data (test utility)."""
        self._objects.clear()
        self._metadata.clear()