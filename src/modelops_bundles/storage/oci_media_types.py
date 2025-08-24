"""
OCI media types and constants.

Single source of truth for all OCI-related media types and constants.
"""
from __future__ import annotations

# OCI standard manifest types - now used for ModelOps bundles
OCI_IMAGE_MANIFEST = "application/vnd.oci.image.manifest.v1+json"
OCI_ARTIFACT_MANIFEST = "application/vnd.oci.artifact.manifest.v1+json"
OCI_EMPTY_CONFIG = "application/vnd.oci.empty.v1+json"

# OCI standard layer types - now used for our JSON files  
OCI_IMAGE_LAYER = "application/vnd.oci.image.layer.v1.tar+gzip"
OCI_GENERIC_LAYER = "application/octet-stream"

# Empty config for minimal OCI images (always {})
OCI_EMPTY_CONFIG_BYTES = b"{}"
OCI_EMPTY_CONFIG_DIGEST = "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
OCI_EMPTY_CONFIG_SIZE = 2

# ModelOps Bundle annotations (replaces custom media types)
MODELOPS_BUNDLE_ANNOTATION = "org.opencontainers.artifact.type"
MODELOPS_BUNDLE_TYPE = "modelops/bundle"
MODELOPS_TITLE_ANNOTATION = "org.opencontainers.image.title"

# Standard file titles for identification
BUNDLE_MANIFEST_TITLE = "bundle.manifest.json"
LAYER_INDEX_TITLE_FORMAT = "layer.{name}.json"


__all__ = [
    "OCI_IMAGE_MANIFEST",
    "OCI_ARTIFACT_MANIFEST", 
    "OCI_EMPTY_CONFIG",
    "OCI_IMAGE_LAYER",
    "OCI_GENERIC_LAYER",
    "OCI_EMPTY_CONFIG_BYTES",
    "OCI_EMPTY_CONFIG_DIGEST", 
    "OCI_EMPTY_CONFIG_SIZE",
    "MODELOPS_BUNDLE_ANNOTATION",
    "MODELOPS_BUNDLE_TYPE",
    "MODELOPS_TITLE_ANNOTATION",
    "BUNDLE_MANIFEST_TITLE",
    "LAYER_INDEX_TITLE_FORMAT"
]