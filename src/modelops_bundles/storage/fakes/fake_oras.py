"""
Fake ORAS store implementation for testing.

This implementation explicitly subclasses OrasStore to ensure interface changes
break CI immediately, preventing silent drift.
"""
from __future__ import annotations

import hashlib
import re
from typing import Dict

from ..base import OrasStore

# Regex for validating SHA256 digests
_DIGEST_RE = re.compile(r"^sha256:[a-f0-9]{64}$")

__all__ = ["FakeOrasStore"]


class FakeOrasStore(OrasStore):
    """
    In-memory ORAS store implementation for testing.
    
    This is a test double; not for production use.
    Keys are digests; values are bytes.
    """
    
    def __init__(self) -> None:
        self._blobs: Dict[str, bytes] = {}
        self._manifests: Dict[str, bytes] = {}

    def blob_exists(self, digest: str) -> bool:
        """Check if blob exists. Always returns boolean, never raises."""
        return digest in self._blobs

    def get_blob(self, digest: str) -> bytes:
        """Retrieve blob content."""
        if digest not in self._blobs:
            raise KeyError(digest)
        return self._blobs[digest]

    def put_blob(self, digest: str, data: bytes) -> None:
        """Store blob content."""
        if not _DIGEST_RE.match(digest):
            raise ValueError(f"invalid digest format: {digest}")
        self._blobs[digest] = data

    def get_manifest(self, digest_or_ref: str) -> bytes:
        """Retrieve manifest by digest or reference. Fake supports only digests."""
        if digest_or_ref not in self._manifests:
            raise KeyError(digest_or_ref)
        return self._manifests[digest_or_ref]

    def put_manifest(self, media_type: str, payload: bytes) -> str:
        """Store manifest and return computed digest."""
        # For fakes, compute digest over payload and store under that digest
        h = hashlib.sha256(payload).hexdigest()
        digest = f"sha256:{h}"
        self._manifests[digest] = payload
        return digest

    def clear(self) -> None:
        """Clear all stored data (test utility)."""
        self._blobs.clear()
        self._manifests.clear()