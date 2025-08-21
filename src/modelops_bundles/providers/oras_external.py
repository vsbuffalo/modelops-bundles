"""
Default ContentProvider implementation for production use.

This module provides the ORAS+External ContentProvider that is used
in production to enumerate bundle content from registries and external
storage systems.
"""
from __future__ import annotations

import os
from typing import Iterable

from modelops_contracts.artifacts import ResolvedBundle

from ..runtime_types import ContentProvider, MatEntry


class OrasExternalProvider(ContentProvider):
    """
    Production ContentProvider that handles ORAS and external storage.
    
    This provider:
    - Fetches ORAS blobs from registries
    - Resolves external storage references
    - Yields MatEntry objects for materialization
    
    In Stage 1, this is a stub that will be implemented later.
    """
    
    def __init__(
        self,
        registry_url: str | None = None,
        registry_auth: str | None = None,
        external_config: dict | None = None,
    ):
        """
        Initialize the ORAS+External provider.
        
        Args:
            registry_url: ORAS registry URL (or from env)
            registry_auth: Registry authentication (or from env)
            external_config: External storage configuration
        """
        self.registry_url = registry_url or os.getenv("MOPS_REGISTRY_URL", "")
        self.registry_auth = registry_auth or os.getenv("MOPS_REGISTRY_AUTH", "")
        self.external_config = external_config or {}
    
    def enumerate(
        self,
        resolved: ResolvedBundle,
        layers: list[str]
    ) -> Iterable[MatEntry]:
        """
        Enumerate content from ORAS registry and external storage.
        
        This is a stub for Stage 1. The real implementation will:
        1. Connect to the ORAS registry
        2. Fetch layer manifests
        3. Download ORAS blobs
        4. Resolve external storage references
        5. Yield MatEntry objects
        
        Args:
            resolved: The resolved bundle with manifest information
            layers: List of layer names to enumerate
            
        Yields:
            MatEntry objects for each file to materialize
        """
        # Stage 1: Stub implementation
        # Real implementation will fetch from registry
        raise NotImplementedError(
            "OrasExternalProvider will be implemented in Stage 2"
        )


def default_provider_from_env() -> ContentProvider:
    """
    Create the default ContentProvider from environment configuration.
    
    This helper is used by the CLI to construct the production provider
    while keeping the runtime API pure (provider required).
    
    Environment variables:
    - MOPS_REGISTRY_URL: ORAS registry URL
    - MOPS_REGISTRY_AUTH: Registry authentication
    - MOPS_EXTERNAL_CONFIG: Path to external storage config JSON
    
    Returns:
        Configured ContentProvider for production use
    """
    # Load external config if provided
    external_config = {}
    config_path = os.getenv("MOPS_EXTERNAL_CONFIG")
    if config_path:
        import json
        with open(config_path) as f:
            external_config = json.load(f)
    
    return OrasExternalProvider(
        registry_url=os.getenv("MOPS_REGISTRY_URL"),
        registry_auth=os.getenv("MOPS_REGISTRY_AUTH"),
        external_config=external_config
    )