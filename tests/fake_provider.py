"""
Fake ContentProvider implementation for testing.

This module provides a test-only implementation of the ContentProvider protocol
that generates predictable fake content for testing the runtime layer.
"""
from __future__ import annotations

import hashlib
from typing import Iterable

from modelops_contracts.artifacts import ResolvedBundle

from modelops_bundles.runtime_types import ContentProvider, MatEntry


class FakeProvider(ContentProvider):
    """
    Test-only ContentProvider that generates predictable fake content.
    
    This provider creates deterministic fake entries based on the requested
    layers, useful for testing the runtime without needing real registry
    or external storage connections.
    """
    
    def enumerate(
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
            MatEntry objects with fake content
        """
        # Generate deterministic content based on manifest digest
        seed = resolved.manifest_digest
        
        for layer in layers:
            if layer == "code":
                # ORAS files for code layer
                yield MatEntry(
                    path="src/model.py",
                    layer="code", 
                    kind="oras",
                    content=self._content(seed, "src/model.py", "# Fake model code\nimport numpy as np\n")
                )
                yield MatEntry(
                    path="src/utils.py",
                    layer="code",
                    kind="oras", 
                    content=self._content(seed, "src/utils.py", "# Fake utility code\ndef helper():\n    pass\n")
                )
                
            elif layer == "config":
                # ORAS files for config layer
                yield MatEntry(
                    path="configs/base.yaml",
                    layer="config",
                    kind="oras",
                    content=self._content(seed, "configs/base.yaml", "# Fake config\nmodel:\n  type: test\n")
                )
                
            elif layer == "data":
                # External storage references for data layer
                for name in ("data/train.csv", "data/test.csv"):
                    uri = f"az://fake-container/{name}"
                    sha = hashlib.sha256(f"fake-bytes-for:{name}".encode()).hexdigest()
                    size = 1_048_576 if "train" in name else 512_000  # 1MB / 512KB
                    yield MatEntry(
                        path=name, layer="data", kind="external",
                        content=None, uri=uri, sha256=sha, size=size, tier="cool"
                    )
    
    def _content(self, seed: str, path: str, template: str) -> bytes:
        """Generate deterministic fake content for a file."""
        h = hashlib.sha256(f"{seed}:{path}".encode()).hexdigest()[:8]
        return (f"{template}\n# Generated with hash: {h}\n").encode()
    
    def fetch_external(self, entry: MatEntry) -> bytes:
        """Fetch fake external content for testing."""
        # Predictable content for tests
        return (f"External content for {entry.path}\nURI: {entry.uri}\n").encode()