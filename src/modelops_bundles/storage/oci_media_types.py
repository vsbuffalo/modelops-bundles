"""
OCI media types and constants.

Single source of truth for all OCI-related media types and constants.
"""
from __future__ import annotations

# OCI standard manifest types
OCI_IMAGE_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_EMPTY_CONFIG = "application/vnd.oci.empty.v1+json"

# Empty config for minimal OCI images (always {})
OCI_EMPTY_CONFIG_BYTES = b"{}"
OCI_EMPTY_CONFIG_DIGEST = "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
OCI_EMPTY_CONFIG_SIZE = 2

# Re-export bundle-specific types from contracts (single source of truth)
from modelops_contracts.artifacts import (
    BUNDLE_MANIFEST,  # "application/vnd.modelops.bundle.manifest+json"  
    LAYER_INDEX,      # "application/vnd.modelops.layer+json"
)

__all__ = [
    "OCI_IMAGE_MANIFEST",
    "OCI_EMPTY_CONFIG", 
    "OCI_EMPTY_CONFIG_BYTES",
    "OCI_EMPTY_CONFIG_DIGEST",
    "OCI_EMPTY_CONFIG_SIZE",
    "BUNDLE_MANIFEST",
    "LAYER_INDEX",
]