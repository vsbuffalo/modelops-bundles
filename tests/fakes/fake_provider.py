"""
Fake ContentProvider implementation for testing.

This module provides a test-only implementation of the ContentProvider protocol
that generates predictable fake content for testing the runtime layer.
"""
from __future__ import annotations

import hashlib
from typing import Iterable
from io import BytesIO

from modelops_contracts.artifacts import ResolvedBundle

from modelops_bundles.runtime_types import ContentProvider, MatEntry, ByteStream


class FakeProvider(ContentProvider):
    """
    Test-only ContentProvider that generates predictable fake content.
    
    This provider creates deterministic fake entries based on the requested
    layers, useful for testing the runtime without needing real registry
    or external storage connections.
    """
    
    def __init__(self):
        self._content_cache = {}  # Cache generated content by SHA256
    
    def iter_entries(
        self, 
        resolved: ResolvedBundle, 
        layers: list[str]
    ) -> Iterable[MatEntry]:
        """
        Generate fake entries for the requested layers.
        
        Creates predictable test content:
        - "code" layer: src/model.py, src/utils.py
        - "config" layer: configs/base.yaml
        - "data" layer: data/train.csv (external), data/test.csv (external)
        
        Args:
            resolved: The resolved bundle (used for deterministic content)
            layers: List of layer names to generate entries for
            
        Yields:
            MatEntry objects with metadata only (no content)
        """
        # Generate deterministic content based on manifest digest
        seed = resolved.manifest_digest
        
        for layer in layers:
            if layer == "code":
                # ORAS files for code layer
                content_bytes = self._content(seed, "src/model.py", "# Fake model code\nimport numpy as np\n")
                sha256_hex = hashlib.sha256(content_bytes).hexdigest()
                self._content_cache[sha256_hex] = content_bytes  # Cache for later
                yield MatEntry(
                    path="src/model.py",
                    layer="code", 
                    kind="oras",
                    size=len(content_bytes),
                    digest=f"sha256:{sha256_hex}",
                    sha256=sha256_hex
                )
                
                content_bytes = self._content(seed, "src/utils.py", "# Fake utility code\ndef helper():\n    pass\n")
                sha256_hex = hashlib.sha256(content_bytes).hexdigest()
                self._content_cache[sha256_hex] = content_bytes  # Cache for later
                yield MatEntry(
                    path="src/utils.py",
                    layer="code",
                    kind="oras",
                    size=len(content_bytes),
                    digest=f"sha256:{sha256_hex}",
                    sha256=sha256_hex
                )
                
            elif layer == "config":
                # ORAS files for config layer
                content_bytes = self._content(seed, "configs/base.yaml", "# Fake config\nmodel:\n  type: test\n")
                sha256_hex = hashlib.sha256(content_bytes).hexdigest()
                self._content_cache[sha256_hex] = content_bytes  # Cache for later
                yield MatEntry(
                    path="configs/base.yaml",
                    layer="config",
                    kind="oras",
                    size=len(content_bytes),
                    digest=f"sha256:{sha256_hex}",
                    sha256=sha256_hex
                )
                
            elif layer == "data":
                # External storage references for data layer
                for name in ("data/train.csv", "data/test.csv"):
                    uri = f"az://fake-container/{name}"
                    content_bytes = f"fake-bytes-for:{name}".encode()
                    sha = hashlib.sha256(content_bytes).hexdigest()
                    self._content_cache[sha] = content_bytes  # Cache external content too
                    size = 1_048_576 if "train" in name else 512_000  # 1MB / 512KB
                    yield MatEntry(
                        path=name, layer="data", kind="external",
                        size=size, digest=f"sha256:{sha}", sha256=sha,
                        uri=uri, tier="cool"
                    )
    
    def _content(self, seed: str, path: str, template: str) -> bytes:
        """Generate deterministic fake content for a file."""
        h = hashlib.sha256(f"{seed}:{path}".encode()).hexdigest()[:8]
        return (f"{template}\n# Generated with hash: {h}\n").encode()
    
    def fetch_oras(self, entry: MatEntry) -> ByteStream:
        """Fetch fake ORAS content for testing."""
        if entry.kind != "oras":
            raise ValueError(f"Expected ORAS entry, got {entry.kind}")
        
        # Look up cached content by SHA256
        cached_content = self._content_cache.get(entry.sha256)
        if cached_content is not None:
            return BytesIO(cached_content)
        
        # If not cached, this is an error in the test setup
        raise ValueError(f"No cached content found for SHA256 {entry.sha256}. Call iter_entries() first.")
    
    def fetch_external(self, entry: MatEntry) -> ByteStream:
        """Fetch fake external content for testing."""
        if entry.kind != "external":
            raise ValueError(f"Expected external entry, got {entry.kind}")
        
        # Look up cached content by SHA256
        cached_content = self._content_cache.get(entry.sha256)
        if cached_content is not None:
            return BytesIO(cached_content)
        
        # If not cached, this is an error in the test setup
        raise ValueError(f"No cached external content found for SHA256 {entry.sha256}. Call iter_entries() first.")