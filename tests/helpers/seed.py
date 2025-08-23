"""
Test data seeding helpers.

Utilities for populating fake registries with test bundle data.
"""
import json
import hashlib
from typing import Dict

from modelops_bundles.storage.repo_path import build_repo
from modelops_bundles.storage.oci_media_types import (
    BUNDLE_MANIFEST, 
    OCI_IMAGE_MANIFEST,
    OCI_EMPTY_CONFIG,
    OCI_EMPTY_CONFIG_DIGEST,
    OCI_EMPTY_CONFIG_SIZE
)
from modelops_bundles.settings import Settings
from ..storage.fakes.fake_oci_registry import FakeOciRegistry


def seed_bundle(
    registry: FakeOciRegistry, 
    settings: Settings, 
    name: str, 
    tag: str, 
    roles: Dict[str, list], 
    layer_indexes: Dict[str, str]
) -> tuple[str, str]:
    """
    Seed a fake registry with test bundle data.
    
    Args:
        registry: Fake OCI registry to populate
        settings: Settings for repo path construction
        name: Bundle name
        tag: Version tag
        roles: Role -> layer list mapping
        layer_indexes: Layer name -> digest mapping
        
    Returns:
        Tuple of (image_manifest_digest, bundle_manifest_digest)
    """
    repo = build_repo(settings, name)
    
    # Create bundle manifest
    bundle_manifest = {
        "mediaType": BUNDLE_MANIFEST,
        "name": name,
        "version": tag,
        "roles": roles,
        "layers": list(layer_indexes.keys()),
        "layer_indexes": {k: v for k, v in layer_indexes.items()},
        "external_index_present": True,
    }
    
    bm_bytes = json.dumps(
        bundle_manifest, 
        sort_keys=True, 
        separators=(',', ':'), 
        ensure_ascii=True
    ).encode()
    bm_digest = f"sha256:{hashlib.sha256(bm_bytes).hexdigest()}"
    
    # Store the bundle manifest blob
    registry.put_blob(repo, bm_digest, bm_bytes)
    
    # Create OCI image manifest
    image_manifest = {
        "schemaVersion": 2,
        "mediaType": OCI_IMAGE_MANIFEST,
        "config": {
            "mediaType": OCI_EMPTY_CONFIG,
            "digest": OCI_EMPTY_CONFIG_DIGEST,
            "size": OCI_EMPTY_CONFIG_SIZE
        },
        "layers": [{
            "mediaType": BUNDLE_MANIFEST,
            "digest": bm_digest,
            "size": len(bm_bytes)
        }],
    }
    
    im_bytes = json.dumps(
        image_manifest, 
        sort_keys=True, 
        separators=(',', ':'), 
        ensure_ascii=True
    ).encode()
    im_digest = f"sha256:{hashlib.sha256(im_bytes).hexdigest()}"
    
    # Store image manifest and tag it
    registry.put_manifest(repo, OCI_IMAGE_MANIFEST, im_bytes, tag)
    
    return im_digest, bm_digest


def seed_simple_bundle(registry: FakeOciRegistry, settings: Settings) -> str:
    """
    Seed a simple test bundle with standard test data.
    
    Returns:
        Image manifest digest
    """
    # Create fake layer indexes
    layer_indexes = {}
    for layer in ["code", "config", "data"]:
        layer_index = {
            "mediaType": "application/vnd.modelops.layer+json",
            "entries": []
        }
        
        if layer == "data":
            layer_index["entries"] = [{
                "path": "data/train.csv",
                "external": {
                    "uri": "az://fake-container/train.csv",
                    "sha256": "fake-train-sha256", 
                    "size": 1048576
                }
            }]
        else:
            layer_index["entries"] = [{
                "path": f"{layer}/example.txt",
                "oras": {
                    "digest": f"sha256:fake-{layer}-blob-digest"
                }
            }]
        
        layer_payload = json.dumps(layer_index).encode()
        digest = f"sha256:{hashlib.sha256(layer_payload).hexdigest()}"
        
        # Store layer index as blob
        repo = build_repo(settings, "test-bundle")
        registry.put_blob(repo, digest, layer_payload)
        layer_indexes[layer] = digest
    
    roles = {
        "default": ["code", "config"],
        "runtime": ["code", "config"],
        "training": ["code", "config", "data"]
    }
    
    im_digest, _ = seed_bundle(
        registry, settings, "test-bundle", "1.0", roles, layer_indexes
    )
    
    return im_digest