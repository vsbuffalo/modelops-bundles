"""
OCI-based bundle resolution.

Implements bundle resolution directly using OrasBundleRegistry,
replacing the legacy resolve_http implementation. This provides a clean
path from BundleRef to ResolvedBundle using repo-aware registry operations.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from modelops_contracts.artifacts import BundleRef, ResolvedBundle

from ..runtime import BundleNotFoundError
from ..settings import Settings
from .oci_media_types import BUNDLE_MANIFEST_TITLE, MODELOPS_TITLE_ANNOTATION
from .oras_bundle_registry import OrasBundleRegistry
from .repo_path import build_repo

logger = logging.getLogger(__name__)


def resolve_oci(ref: BundleRef, registry: OrasBundleRegistry, settings: Settings, 
                cache: bool = True) -> ResolvedBundle:
    """
    Resolve bundle using OrasBundleRegistry.
    
    This function replaces resolve_http and is built directly on the
    OrasBundleRegistry for cleaner dependency management.
    
    Args:
        ref: Bundle reference to resolve
        registry: ORAS bundle registry or OCI registry implementation
        settings: Settings for repository path construction
        cache: Whether to prepare cache directory
        
    Returns:
        ResolvedBundle with metadata and content addresses
        
    Raises:
        ValueError: If ref doesn't include name or is malformed
        BundleNotFoundError: If bundle cannot be found
    """
    # Validate ref has required fields
    if not ref.name:
        raise ValueError("BundleRef must include name (bare digests not supported)")
    
    # Build repository path from settings and bundle name
    repo = build_repo(settings, ref.name)
    logger.debug(f"Resolving bundle {ref} from repository {repo}")
    
    # Determine manifest reference to fetch
    if ref.digest:
        # Use digest directly (format: sha256:abc...)
        manifest_ref = ref.digest
    elif ref.version:
        # Use version as tag
        manifest_ref = ref.version
    else:
        raise ValueError("BundleRef must have either version or digest")
    
    try:
        # Step 1: Get OCI image manifest and canonical digest
        manifest_bytes = registry.get_manifest(repo, manifest_ref)
        canonical_digest = registry.head_manifest(repo, manifest_ref)
        
        logger.debug(f"Retrieved manifest with digest {canonical_digest}")
        
        # Step 2: Parse OCI image manifest
        try:
            oci_manifest = json.loads(manifest_bytes.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise BundleNotFoundError(f"Invalid manifest JSON for {repo}:{manifest_ref}: {e}")
        
        # Step 3: Find bundle manifest in layers
        bundle_descriptor = _find_bundle_manifest_descriptor(oci_manifest)
        if not bundle_descriptor:
            raise BundleNotFoundError(
                f"No bundle manifest layer found in {repo}:{manifest_ref}. "
                f"Expected layer with title annotation '{BUNDLE_MANIFEST_TITLE}' or media type 'application/json'"
            )
        
        # Step 4: Fetch and parse bundle manifest
        bundle_manifest_digest = bundle_descriptor["digest"]
        logger.debug(f"Fetching bundle manifest from blob {bundle_manifest_digest}")
        
        bundle_manifest_bytes = registry.get_blob(repo, bundle_manifest_digest)
        
        try:
            bundle_manifest = json.loads(bundle_manifest_bytes.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            raise BundleNotFoundError(f"Invalid bundle manifest JSON: {e}")
        
        # Step 5: Extract metadata
        roles = bundle_manifest.get("roles", {})
        layers_raw = bundle_manifest.get("layers", {})
        
        # Handle both dict and list formats for layers
        if isinstance(layers_raw, dict):
            # New format: dict mapping layer name -> digest
            layers = list(layers_raw.keys())
        else:
            # Old format: list of layer names
            layers = layers_raw
            
        layer_indexes = bundle_manifest.get("layer_indexes", {})
        external_index_present = bundle_manifest.get("external_index_present", False)
        
        # Step 6: Prepare cache directory if requested
        cache_dir = None
        if cache:
            cache_dir = _prepare_cache_directory(canonical_digest)
        
        # Step 7: Create ResolvedBundle
        resolved = ResolvedBundle(
            ref=ref,
            manifest_digest=canonical_digest,
            media_type=oci_manifest.get("mediaType", "application/vnd.oci.image.manifest.v1+json"),
            roles=roles,
            layers=layers,  # Already a list from manifest
            layer_indexes=layer_indexes,
            external_index_present=external_index_present,
            total_size=0,  # TODO: Calculate from layer indexes  
            cache_dir=cache_dir
        )
        
        logger.info(f"Successfully resolved bundle {ref.name}:{ref.version or ref.digest} "
                   f"to digest {canonical_digest}")
        
        return resolved
        
    except Exception as e:
        if isinstance(e, (ValueError, BundleNotFoundError)):
            raise  # Re-raise our own exceptions
        
        # Map registry errors to bundle errors
        logger.error(f"Failed to resolve bundle {repo}:{manifest_ref}: {e}")
        raise BundleNotFoundError(f"Bundle resolution failed for {ref}: {e}") from e


def _find_bundle_manifest_descriptor(oci_manifest: dict) -> dict | None:
    """
    Find bundle manifest layer descriptor in OCI manifest.
    
    Supports both new annotation-based detection and legacy media type detection.
    
    Args:
        oci_manifest: Parsed OCI image manifest
        
    Returns:
        Bundle manifest layer descriptor, or None if not found
    """
    # Check layers for bundle manifest
    for layer in oci_manifest.get("layers", []):
        # Try new annotation-based detection first
        annotations = layer.get("annotations", {})
        if annotations.get(MODELOPS_TITLE_ANNOTATION) == BUNDLE_MANIFEST_TITLE:
            return layer
        
        # Fall back to legacy media type detection
        if layer.get("mediaType") == "application/json":
            return layer
    
    # Check config blob (alternative location)
    config = oci_manifest.get("config", {})
    if config:
        # Try new annotation-based detection first
        annotations = config.get("annotations", {})
        if annotations.get(MODELOPS_TITLE_ANNOTATION) == BUNDLE_MANIFEST_TITLE:
            return config
        
        # Fall back to legacy media type detection
        if config.get("mediaType") == "application/json":
            return config
    
    return None


def _prepare_cache_directory(digest: str) -> Path | None:
    """
    Prepare cache directory for bundle.
    
    Args:
        digest: Canonical manifest digest
        
    Returns:
        Cache directory path, or None if cache setup fails
    """
    try:
        # TODO: Implement cache directory creation
        # This should create ~/.modelops/bundles/<digest>/ structure
        logger.debug(f"Cache directory preparation not yet implemented for {digest}")
        return None
    except Exception as e:
        logger.warning(f"Failed to prepare cache directory for {digest}: {e}")
        return None


__all__ = ["resolve_oci"]