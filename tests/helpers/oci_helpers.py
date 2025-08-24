"""
OCI manifest helpers for tests.

Provides utilities for creating properly structured OCI image manifests 
that wrap bundle manifests according to the OCI Distribution spec.
"""
import json
import hashlib
from typing import Dict, Any

from modelops_bundles.storage.oci_media_types import (
    OCI_IMAGE_MANIFEST,
    OCI_EMPTY_CONFIG,
    OCI_EMPTY_CONFIG_DIGEST,
    OCI_EMPTY_CONFIG_SIZE,
    BUNDLE_MANIFEST
)


def create_oci_image_manifest(bundle_manifest_bytes: bytes) -> bytes:
    """
    Wrap a bundle manifest in an OCI image manifest.
    
    Creates a proper OCI image manifest v1 that includes the bundle manifest
    as a layer, following the pattern used by our publisher.
    
    Args:
        bundle_manifest_bytes: Serialized bundle manifest bytes
        
    Returns:
        OCI image manifest bytes that wraps the bundle manifest
    """
    bundle_digest = f"sha256:{hashlib.sha256(bundle_manifest_bytes).hexdigest()}"
    bundle_size = len(bundle_manifest_bytes)
    
    oci_manifest = {
        "schemaVersion": 2,
        "mediaType": OCI_IMAGE_MANIFEST,
        "config": {
            "mediaType": OCI_EMPTY_CONFIG,
            "size": OCI_EMPTY_CONFIG_SIZE,
            "digest": OCI_EMPTY_CONFIG_DIGEST
        },
        "layers": [
            {
                "mediaType": BUNDLE_MANIFEST,
                "size": bundle_size,
                "digest": bundle_digest
            }
        ]
    }
    
    return json.dumps(oci_manifest, sort_keys=True, separators=(',', ':')).encode()


def setup_fake_bundle_in_registry(registry, repo: str, bundle_manifest: Dict[str, Any], 
                                  tag: str, layer_blobs: Dict[str, bytes] = None) -> str:
    """
    Set up a complete fake bundle in a registry.
    
    This includes:
    1. Storing layer blobs (if provided)
    2. Storing the bundle manifest as a blob
    3. Creating an OCI image manifest that references the bundle manifest
    4. Tagging the OCI manifest
    
    Args:
        registry: FakeOrasBundleRegistry instance
        repo: Repository path (e.g., "testns/bundles/mybundle")
        bundle_manifest: Bundle manifest dict
        tag: Tag for the bundle (e.g., "v1.0.0")
        layer_blobs: Optional dict of {digest: content} for layer blobs
        
    Returns:
        OCI image manifest digest
    """
    # Store layer blobs if provided
    if layer_blobs:
        for digest, content in layer_blobs.items():
            registry.put_blob(repo, digest, content)
    
    # Store bundle manifest as blob
    bundle_manifest_bytes = json.dumps(bundle_manifest, sort_keys=True, separators=(',', ':')).encode()
    bundle_digest = f"sha256:{hashlib.sha256(bundle_manifest_bytes).hexdigest()}"
    registry.put_blob(repo, bundle_digest, bundle_manifest_bytes)
    
    # Store OCI empty config blob
    empty_config = b"{}"
    registry.put_blob(repo, OCI_EMPTY_CONFIG_DIGEST, empty_config)
    
    # Create and store OCI image manifest
    oci_manifest_bytes = create_oci_image_manifest(bundle_manifest_bytes)
    oci_digest = registry.put_manifest(repo, OCI_IMAGE_MANIFEST, oci_manifest_bytes, tag)
    
    return oci_digest