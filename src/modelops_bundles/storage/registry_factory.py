"""
Registry factory with implementation switching.

Provides a single factory function that can create different OCI registry
implementations based on environment configuration. This allows easy A/B
testing and experimentation without changing call sites.
"""
from __future__ import annotations

import os

from ..settings import Settings
from .hybrid_oci_registry import HybridOciRegistry
from .oci_registry import OciRegistry


def make_registry(settings: Settings) -> OciRegistry:
    """
    Create an OCI registry implementation based on environment configuration.
    
    Args:
        settings: Registry configuration
        
    Returns:
        OCI registry implementation
        
    Environment Variables:
        OCI_REGISTRY_IMPL: Implementation to use
            - "hybrid" (default): HybridOciRegistry (HTTP for manifests, SDK for blobs)
            - "http": HttpOciRegistry (pure HTTP - not yet implemented)
            
    Examples:
        >>> # Use default hybrid implementation
        >>> registry = make_registry(settings)
        
        >>> # Force HTTP implementation via environment
        >>> os.environ["OCI_REGISTRY_IMPL"] = "http"
        >>> registry = make_registry(settings)
        
    Raises:
        ValueError: If OCI_REGISTRY_IMPL specifies unknown implementation
        NotImplementedError: If implementation is not yet available
    """
    impl_type = os.getenv("OCI_REGISTRY_IMPL", "hybrid").lower()
    
    if impl_type == "hybrid":
        return HybridOciRegistry(settings)
    elif impl_type == "http":
        # Pure HTTP implementation - not yet available
        raise NotImplementedError(
            "Pure HTTP implementation (HttpOciRegistry) not yet implemented. "
            "Use 'hybrid' for now."
        )
    else:
        raise ValueError(
            f"Unknown OCI_REGISTRY_IMPL: {impl_type}. "
            f"Supported values: hybrid, http"
        )


__all__ = ["make_registry"]