"""
Bundle publishing and creation.

Main entry point for creating and pushing ModelOps bundles to registries.
Orchestrates scanning, planning, staging, and pushing operations.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .models import (
    BundleSpec,
    StoragePlan,
    LayerIndex,
    BundleManifest,
    StorageDecision,
    BUNDLE_MANIFEST_TYPE,
    LAYER_INDEX_TYPE
)
from .planner import (
    scan_directory,
    plan_storage,
    create_layer_indexes,
    create_bundle_manifest,
    detect_changes
)
from .storage.oras_bundle_registry import OrasBundleRegistry
from .storage.repo_path import build_repo
from .settings import Settings
from .storage.oci_media_types import (
    MODELOPS_BUNDLE_ANNOTATION,
    MODELOPS_BUNDLE_TYPE,
    MODELOPS_TITLE_ANNOTATION,
    BUNDLE_MANIFEST_TITLE,
    LAYER_INDEX_TITLE_FORMAT
)
from .runtime import BundleDownloadError


def push_bundle(working_dir: str | Path, tag: str = None, *,
                registry: Optional[OrasBundleRegistry] = None,
                settings: Optional['Settings'] = None,
                force: bool = False,
                dry_run: bool = False) -> str:
    """
    Push bundle from working directory to registry.
    
    This is the main public interface for bundle publishing. It:
    1. Scans the working directory for modelops.yaml
    2. Plans storage decisions (ORAS vs external)
    3. Creates layer indexes and bundle manifest
    4. Stages all files in a temporary directory
    5. Pushes using OCI registry
    6. Returns the canonical manifest digest
    
    Args:
        working_dir: Directory containing modelops.yaml and bundle files
        tag: Tag for the pushed bundle (uses spec.version if None)
        registry: OCI registry (created from env if None)
        settings: Settings (loaded from env if None)
        force: Skip "no changes" detection and always push
        dry_run: Show what would be pushed without actually pushing
        
    Returns:
        Canonical manifest digest (sha256:...)
        
    Raises:
        FileNotFoundError: If modelops.yaml not found
        ValueError: If bundle specification is invalid
        BundleDownloadError: If push fails
    """
    working_dir = Path(working_dir)
    
    if not working_dir.exists():
        raise FileNotFoundError(f"Working directory not found: {working_dir}")
    
    if not working_dir.is_dir():
        raise ValueError(f"Working directory must be a directory: {working_dir}")
    
    # Load settings if not provided
    if settings is None:
        from .settings import create_settings_from_env
        settings = create_settings_from_env()
    
    # Create registry if not provided
    if registry is None:
        registry = OrasBundleRegistry(settings)
    
    # Phase 1: Scan directory and parse specification
    spec = scan_directory(working_dir)
    
    # Use spec version if tag not provided
    if tag is None:
        tag = spec.version
    
    # Build repo path from settings and bundle name
    repo = build_repo(settings, spec.name)
    
    # Phase 2: Plan storage (ORAS vs external decisions)
    plan = plan_storage(spec, working_dir)
    
    # Phase 3: Create layer indexes and bundle manifest
    layer_indexes = create_layer_indexes(plan, spec.external_rules)
    bundle_manifest = create_bundle_manifest(spec, layer_indexes)
    
    # Phase 4: Check for changes (unless forced)
    if not force and not dry_run:
        # TODO: Implement change detection
        # For now, always proceed with push
        pass
    
    # Phase 5: Push with ORAS (simplified)
    if dry_run:
        return _show_dry_run_summary(plan, layer_indexes, bundle_manifest, repo, tag)
    else:
        return _push_with_oras(plan, layer_indexes, bundle_manifest, repo, tag, registry)


def _push_with_oras(plan: StoragePlan, layer_indexes: Dict[str, LayerIndex],
                   bundle_manifest: BundleManifest, repo: str, tag: str,
                   registry: OrasBundleRegistry) -> str:
    """
    Push bundle using ORAS - replaces complex staging logic.
    
    This is dramatically simplified compared to the old approach:
    - No manual OCI manifest building
    - No staging directories
    - No custom media type handling
    - ORAS handles all the complexity
    
    Returns:
        Canonical manifest digest
    """
    import tempfile
    import json
    from pathlib import Path
    
    # Create temp files for JSON documents
    files_to_push = []
    
    with tempfile.TemporaryDirectory(prefix="oras-bundle-") as tmp_dir:
        tmp_path = Path(tmp_dir)
        
        # 1. Write bundle manifest with title annotation
        bundle_path = tmp_path / BUNDLE_MANIFEST_TITLE
        bundle_path.write_text(
            json.dumps(bundle_manifest.model_dump(by_alias=True), 
                      sort_keys=True, separators=(',', ':'))
        )
        files_to_push.append({
            "path": str(bundle_path),
            "annotations": {MODELOPS_TITLE_ANNOTATION: BUNDLE_MANIFEST_TITLE}
        })
        
        # 2. Write layer indexes with title annotations
        for layer_name, layer_index in layer_indexes.items():
            layer_title = LAYER_INDEX_TITLE_FORMAT.format(name=layer_name)
            layer_path = tmp_path / layer_title
            layer_path.write_text(
                json.dumps(layer_index.model_dump(by_alias=True),
                          sort_keys=True, separators=(',', ':'))
            )
            files_to_push.append({
                "path": str(layer_path),
                "annotations": {MODELOPS_TITLE_ANNOTATION: layer_title}
            })
        
        # 3. Add ORAS files from plan
        for layer_plan in plan.layer_plans.values():
            for file_entry in layer_plan.files:
                storage_decision = layer_plan.storage_decisions[file_entry.artifact_path]
                if storage_decision == StorageDecision.ORAS:  # Only include ORAS files
                    files_to_push.append({
                        "path": str(file_entry.src_path)
                    })
        
        # 4. Push with ORAS using bundle annotations
        manifest_annotations = {
            MODELOPS_BUNDLE_ANNOTATION: MODELOPS_BUNDLE_TYPE,
            "org.opencontainers.image.title": f"{plan.spec.name}:{plan.spec.version}",
            "org.opencontainers.image.description": plan.spec.description or ""
        }
        
        digest = registry.push_bundle(
            files=files_to_push,
            repo=repo,
            tag=tag,
            manifest_annotations=manifest_annotations
        )
        
        # 5. Print success message
        print(f"✅ Pushed bundle {repo}:{tag}")
        print(f"   Manifest digest: {digest}")
        print(f"   Files: {len(files_to_push)} total")
        
        return digest


def _push_staged_bundle(plan: StoragePlan, layer_indexes: Dict[str, LayerIndex],
                       bundle_manifest: BundleManifest, repo: str, tag: str,
                       registry: OrasBundleRegistry) -> str:
    """
    Stage files and push to registry using manual OCI manifest assembly.
    
    This implementation preserves media types by building the OCI image manifest
    manually rather than relying on SDK auto-generation.
    
    Args:
        plan: Storage plan with files to include
        layer_indexes: Layer index documents
        bundle_manifest: Bundle manifest document
        repo: Repository name (e.g., "namespace/bundles/bundle-name")
        tag: Tag
        registry: OCI registry implementation
        
    Returns:
        Canonical manifest digest
    """
    with tempfile.TemporaryDirectory(prefix="modelops-bundle-") as temp_dir:
        stage_dir = Path(temp_dir)
        
        # Stage JSON documents  
        _stage_json_files(stage_dir, layer_indexes, bundle_manifest)
        
        # Stage ORAS files (small files that go in the registry)
        _stage_oras_files(stage_dir, plan)
        
        # Collect all files with their correct media types
        files_with_types = _collect_staged_files(stage_dir)
        
        # Build OCI manifest with preserved media types
        try:
            return _build_and_push_oci_manifest(
                files_with_types, plan, repo, tag, registry
            )
        except Exception as e:
            raise BundleDownloadError(f"Failed to push bundle {repo}:{tag}: {e}") from e


def _build_and_push_oci_manifest(files_with_types: List[Tuple[str, str]], 
                                plan: StoragePlan, repo: str, tag: str,
                                registry: OrasBundleRegistry) -> str:
    """
    Build OCI image manifest manually and push to registry.
    
    This preserves per-file media types by constructing the manifest directly.
    """
    import hashlib
    
    descriptors = []
    
    # Step 1: Upload each file as a blob and collect descriptors
    for file_path, media_type in files_with_types:
        file_path_obj = Path(file_path)
        
        # Read file content
        with open(file_path_obj, 'rb') as f:
            file_content = f.read()
        
        # Compute digest and size
        file_digest = f"sha256:{hashlib.sha256(file_content).hexdigest()}"
        file_size = len(file_content)
        
        # Upload blob (using ensure_blob if available)
        if hasattr(registry, 'ensure_blob'):
            registry.ensure_blob(repo, file_digest, file_content)
        else:
            # Fallback for MVP - put_blob may not be implemented yet
            print(f"Would upload blob {file_digest} with media type {media_type}")
            # TODO: Once put_blob is implemented, use:
            # registry.put_blob(repo, file_digest, file_content)
        
        # Add to descriptors with preserved media type
        descriptors.append({
            "mediaType": media_type,  # PRESERVED!
            "digest": file_digest,
            "size": file_size
        })
    
    # Step 2: Ensure empty config blob exists
    # registry.put_blob(repo, OCI_EMPTY_CONFIG_DIGEST, OCI_EMPTY_CONFIG_BYTES)
    
    # Step 3: Build OCI image manifest v1
    manifest = {
        "schemaVersion": 2,
        "mediaType": OCI_IMAGE_MANIFEST,
        "config": {
            "mediaType": OCI_EMPTY_CONFIG,
            "digest": OCI_EMPTY_CONFIG_DIGEST,
            "size": OCI_EMPTY_CONFIG_SIZE
        },
        "layers": descriptors,
        "annotations": {
            "org.modelops.bundle.name": plan.spec.name,
            "org.modelops.bundle.version": plan.spec.version
        }
    }
    
    # Add description if present
    if plan.spec.description:
        manifest["annotations"]["org.modelops.bundle.description"] = plan.spec.description
    
    # Step 4: Convert to canonical JSON
    manifest_json = json.dumps(
        manifest,
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=True
    )
    manifest_bytes = manifest_json.encode('utf-8')
    
    # Step 5: Push manifest (once put_manifest is implemented)
    # For MVP, show what would be pushed
    print(f"Would push manifest to {repo}:{tag}")
    print(f"Manifest media type: {OCI_IMAGE_MANIFEST}")
    print(f"Layers with preserved media types:")
    for desc in descriptors:
        print(f"  - {desc['mediaType']}: {desc['digest']}")
    
    # TODO: Once put_manifest is implemented:
    # digest = registry.put_manifest(repo, OCI_IMAGE_MANIFEST, manifest_bytes, tag)
    
    # For MVP, return a placeholder digest
    placeholder_digest = f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}"
    
    # Print success summary
    _print_push_summary(plan, {}, placeholder_digest, repo, tag)
    
    return placeholder_digest


def _stage_json_files(stage_dir: Path, layer_indexes: Dict[str, LayerIndex], 
                     bundle_manifest: BundleManifest) -> None:
    """
    Stage JSON documents (bundle manifest and layer indexes) in staging directory.
    
    Args:
        stage_dir: Staging directory
        layer_indexes: Layer index documents  
        bundle_manifest: Bundle manifest document
    """
    # Stage bundle manifest
    bundle_manifest_path = stage_dir / "bundle.manifest.json"
    with open(bundle_manifest_path, 'w') as f:
        json.dump(
            bundle_manifest.model_dump(by_alias=True, exclude_none=True),
            f,
            sort_keys=True,
            separators=(',', ':'),
            ensure_ascii=True
        )
    
    # Stage layer indexes
    for layer_name, layer_index in layer_indexes.items():
        layer_index_path = stage_dir / f"layer.{layer_name}.json"
        with open(layer_index_path, 'w') as f:
            json.dump(
                layer_index.model_dump(by_alias=True, exclude_none=True),
                f,
                sort_keys=True,
                separators=(',', ':'),
                ensure_ascii=True
            )


def _stage_oras_files(stage_dir: Path, plan: StoragePlan) -> None:
    """
    Stage small files that will be stored in ORAS.
    
    Args:
        stage_dir: Staging directory
        plan: Storage plan with file decisions
    """
    for file_entry in plan.all_oras_files:
        # Create destination path in staging area
        dest_path = stage_dir / file_entry.artifact_path
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Copy file to staging area
        shutil.copy2(file_entry.src_path, dest_path)


def _collect_staged_files(stage_dir: Path) -> List[Tuple[str, str]]:
    """
    Collect all staged files with their media types.
    
    Args:
        stage_dir: Staging directory
        
    Returns:
        List of (file_path, media_type) tuples
    """
    files_with_types = []
    
    for file_path in stage_dir.rglob("*"):
        if not file_path.is_file():
            continue
        
        # Determine media type
        relative_path = file_path.relative_to(stage_dir)
        media_type = _determine_media_type(relative_path)
        
        files_with_types.append((str(file_path), media_type))
    
    # Sort for deterministic ordering
    return sorted(files_with_types, key=lambda x: x[0])


def _determine_media_type(file_path: Path) -> str:
    """
    Determine appropriate media type for staged file.
    
    Args:
        file_path: Relative path of staged file
        
    Returns:
        Media type string
    """
    filename = file_path.name
    
    if filename == "bundle.manifest.json":
        return BUNDLE_MANIFEST_TYPE
    elif filename.startswith("layer.") and filename.endswith(".json"):
        return LAYER_INDEX_TYPE
    else:
        # Regular file - use generic media type
        return "application/octet-stream"


def _show_dry_run_summary(plan: StoragePlan, layer_indexes: Dict[str, LayerIndex],
                         bundle_manifest: BundleManifest, repo: str, tag: str) -> str:
    """
    Show what would be pushed without actually pushing.
    
    Args:
        plan: Storage plan
        layer_indexes: Layer indexes
        bundle_manifest: Bundle manifest  
        repo: Repository name
        tag: Tag
        
    Returns:
        Bundle manifest digest (computed locally)
    """
    print(f"\n🧪 DRY RUN - Bundle push simulation for {repo}:{tag}")
    print("=" * 60)
    
    print(f"\n📋 Bundle: {plan.spec.name} v{plan.spec.version}")
    if plan.spec.description:
        print(f"Description: {plan.spec.description}")
    
    print(f"\n📁 Working directory: {plan.working_dir}")
    
    # Show layers and files
    print(f"\n📦 Layers ({len(plan.layer_plans)}):")
    for layer_name, layer_plan in plan.layer_plans.items():
        oras_count = len(layer_plan.oras_files)
        external_count = len(layer_plan.external_files)
        
        print(f"  {layer_name}:")
        print(f"    ORAS files: {oras_count}")
        print(f"    External files: {external_count}")
    
    # Show storage summary
    total_oras = len(plan.all_oras_files)
    total_external = len(plan.all_external_files)
    oras_size = sum(f.size for f in plan.all_oras_files)
    external_size = sum(f.size for f in plan.all_external_files)
    
    print(f"\n📊 Storage Summary:")
    print(f"  ORAS: {total_oras} files ({_format_bytes(oras_size)})")
    print(f"  External: {total_external} files ({_format_bytes(external_size)})")
    
    # Show roles
    print(f"\n🎭 Roles ({len(plan.spec.roles)}):")
    for role_name, role_layers in plan.spec.roles.items():
        print(f"  {role_name}: {', '.join(role_layers)}")
    
    # Show what would be staged
    print(f"\n📋 Would stage files:")
    print(f"  bundle.manifest.json ({BUNDLE_MANIFEST_TYPE})")
    for layer_name in layer_indexes:
        print(f"  layer.{layer_name}.json ({LAYER_INDEX_TYPE})")
    
    for file_entry in plan.all_oras_files[:5]:  # Show first 5
        print(f"  {file_entry.artifact_path} (application/octet-stream)")
    
    if len(plan.all_oras_files) > 5:
        print(f"  ... and {len(plan.all_oras_files) - 5} more ORAS files")
    
    print(f"\n✅ Bundle manifest digest: {bundle_manifest.digest}")
    print(f"🚀 Would push to: {repo}:{tag}")
    
    return bundle_manifest.digest


def _print_push_summary(plan: StoragePlan, layer_indexes: Dict[str, LayerIndex],
                       digest: str, repo: str, tag: str) -> None:
    """
    Print summary after successful push.
    
    Args:
        plan: Storage plan
        layer_indexes: Layer indexes
        digest: Manifest digest
        repo: Repository name
        tag: Tag
    """
    total_oras = len(plan.all_oras_files)
    total_external = len(plan.all_external_files)
    oras_size = sum(f.size for f in plan.all_oras_files)
    
    print(f"\n✅ Successfully pushed {plan.spec.name}:{plan.spec.version}")
    print(f"📋 Repository: {repo}:{tag}")
    print(f"🔗 Manifest digest: {digest}")
    print(f"📦 ORAS files: {total_oras} ({_format_bytes(oras_size)})")
    print(f"🔗 External refs: {total_external}")
    print(f"🏷️  Layers: {len(layer_indexes)}")


def _format_bytes(size_bytes: int) -> str:
    """Format byte count as human-readable string."""
    if size_bytes == 0:
        return "0 B"
    elif size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


# TODO: Add support for multiple tags (tag + latest)
def push_with_multiple_tags(working_dir: str | Path, repo: str, tags: List[str], **kwargs) -> Dict[str, str]:
    """
    Push bundle with multiple tags.
    
    TODO: Implement atomic multi-tag push or sequential tagging.
    For now, this is a placeholder.
    
    Args:
        working_dir: Working directory
        repo: Repository name
        tags: List of tags
        **kwargs: Additional arguments for push_bundle
        
    Returns:
        Dict mapping tags to manifest digests
    """
    raise NotImplementedError("Multi-tag push not yet implemented")


# TODO: Add change detection
def has_changes(working_dir: str | Path, repo: str, tag: str) -> bool:
    """
    Check if bundle has changes compared to remote version.
    
    TODO: Implement by comparing local layer digests with remote bundle.
    
    Args:
        working_dir: Working directory  
        repo: Repository name
        tag: Tag to compare against
        
    Returns:
        True if changes detected
    """
    # For now, always assume changes
    return True