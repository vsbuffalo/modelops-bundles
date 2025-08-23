"""
Bundle planning and scanning logic.

Handles parsing modelops.yaml, scanning directories, and making storage decisions
about what goes in ORAS vs external storage. Ensures deterministic behavior.
"""
from __future__ import annotations

import hashlib
import os
from fnmatch import fnmatch
from pathlib import Path
from typing import Dict, List, Set

from .models import (
    BundleSpec,
    FileEntry,
    LayerPlan,
    StorageDecision,
    StoragePlan,
    ExternalRule,
    LayerIndex,
    LayerIndexEntry,
    OrasDescriptor,
    ExternalDescriptor,
    BundleManifest
)


def scan_directory(working_dir: Path) -> BundleSpec:
    """
    Scan working directory and parse bundle specification.
    
    Args:
        working_dir: Directory containing modelops.yaml
        
    Returns:
        Parsed BundleSpec
        
    Raises:
        FileNotFoundError: If modelops.yaml not found
        ValueError: If modelops.yaml is malformed
    """
    working_dir = Path(working_dir)
    
    # Look for bundle specification file
    spec_files = ["modelops.yaml", "modelops.yml", ".mops-bundle.yaml", ".mops-bundle.yml"]
    spec_path = None
    
    for filename in spec_files:
        candidate = working_dir / filename
        if candidate.exists():
            spec_path = candidate
            break
    
    if spec_path is None:
        raise FileNotFoundError(
            f"Bundle specification not found in {working_dir}. "
            f"Expected one of: {', '.join(spec_files)}"
        )
    
    try:
        return BundleSpec.from_yaml_file(spec_path)
    except Exception as e:
        raise ValueError(f"Failed to parse {spec_path.name}: {e}") from e


def plan_storage(spec: BundleSpec, working_dir: Path) -> StoragePlan:
    """
    Create storage plan by scanning files and making ORAS vs external decisions.
    
    Args:
        spec: Bundle specification
        working_dir: Working directory to scan
        
    Returns:
        Storage plan with per-layer file inventories and decisions
        
    Raises:
        ValueError: If files are too large and no external rule matches
    """
    working_dir = Path(working_dir)
    layer_plans = {}
    
    for layer_spec in spec.layers:
        # Scan files for this layer
        layer_files = _scan_layer_files(layer_spec, working_dir)
        
        # Make storage decisions
        storage_decisions = _make_storage_decisions(
            layer_files, 
            spec.external_rules,
            spec.oras_size_limit
        )
        
        layer_plans[layer_spec.name] = LayerPlan(
            name=layer_spec.name,
            files=layer_files,
            storage_decisions=storage_decisions
        )
    
    return StoragePlan(
        spec=spec,
        layer_plans=layer_plans,
        working_dir=working_dir
    )


def create_layer_indexes(plan: StoragePlan, external_rules: List[ExternalRule]) -> Dict[str, LayerIndex]:
    """
    Create layer index documents from storage plan.
    
    Args:
        plan: Storage plan with file inventories
        external_rules: Rules for generating external URIs
        
    Returns:
        Dict mapping layer names to LayerIndex documents
    """
    layer_indexes = {}
    
    for layer_name, layer_plan in plan.layer_plans.items():
        entries = []
        
        for file_entry in layer_plan.files:
            storage_type = layer_plan.storage_decisions[file_entry.artifact_path]
            
            if storage_type == StorageDecision.ORAS:
                # Create ORAS descriptor
                entry = LayerIndexEntry(
                    path=file_entry.artifact_path,
                    layer=layer_name,
                    oras=OrasDescriptor(
                        digest=f"sha256:{file_entry.sha256}",
                        size=file_entry.size
                    )
                )
            else:
                # Create external descriptor
                external_uri = _generate_external_uri(file_entry, external_rules)
                entry = LayerIndexEntry(
                    path=file_entry.artifact_path,
                    layer=layer_name,
                    external=ExternalDescriptor(
                        uri=external_uri,
                        sha256=file_entry.sha256,
                        size=file_entry.size,
                        tier=_determine_storage_tier(file_entry, external_rules)
                    )
                )
            
            entries.append(entry)
        
        # Create layer index with deterministic ordering
        layer_indexes[layer_name] = LayerIndex(
            layer=layer_name,
            entries=sorted(entries, key=lambda e: e.path)  # Deterministic ordering
        )
    
    return layer_indexes


def create_bundle_manifest(spec: BundleSpec, layer_indexes: Dict[str, LayerIndex]) -> BundleManifest:
    """
    Create bundle manifest document.
    
    Args:
        spec: Bundle specification
        layer_indexes: Layer index documents
        
    Returns:
        Bundle manifest document
    """
    # Create layer name to digest mapping
    layers_mapping = {
        name: index.digest
        for name, index in layer_indexes.items()
    }
    
    # Check if any external references exist
    external_index_present = any(
        any(entry.external is not None for entry in index.entries)
        for index in layer_indexes.values()
    )
    
    return BundleManifest(
        name=spec.name,
        version=spec.version,
        description=spec.description,
        roles=spec.roles,
        layers=layers_mapping,
        external_index_present=external_index_present
    )


def _scan_layer_files(layer_spec, working_dir: Path) -> List[FileEntry]:
    """
    Scan files for a single layer based on include/exclude patterns.
    
    Args:
        layer_spec: Layer specification with file patterns
        working_dir: Directory to scan
        
    Returns:
        List of FileEntry objects for matched files
    """
    matched_files = []
    seen_paths = set()  # Prevent duplicates
    
    # Process each include pattern
    for pattern in layer_spec.files:
        pattern_matches = _glob_files(working_dir, pattern, layer_spec.ignore)
        
        for abs_path in pattern_matches:
            # Create relative path for the artifact
            try:
                rel_path = abs_path.relative_to(working_dir)
            except ValueError:
                # File is outside working directory - skip
                continue
            
            artifact_path = str(rel_path).replace('\\', '/')  # Normalize to forward slashes
            
            # Skip if already seen
            if artifact_path in seen_paths:
                continue
            seen_paths.add(artifact_path)
            
            # Compute file hash
            sha256_hash = _compute_file_hash(abs_path)
            
            matched_files.append(FileEntry(
                src_path=abs_path,
                artifact_path=artifact_path,
                size=abs_path.stat().st_size,
                sha256=sha256_hash,
                layer=layer_spec.name
            ))
    
    # Sort for deterministic ordering
    return sorted(matched_files, key=lambda f: f.artifact_path)


def _glob_files(working_dir: Path, pattern: str, ignore_patterns: List[str]) -> List[Path]:
    """
    Find files matching pattern, excluding ignore patterns.
    
    Args:
        working_dir: Base directory
        pattern: Glob pattern (supports **)
        ignore_patterns: Patterns to ignore
        
    Returns:
        List of matching file paths
    """
    import glob
    
    # Handle absolute vs relative patterns
    if os.path.isabs(pattern):
        # Absolute pattern - use as-is
        search_pattern = pattern
    else:
        # Relative pattern - join with working dir
        search_pattern = str(working_dir / pattern)
    
    # Find all matching paths
    matches = []
    for path_str in glob.glob(search_pattern, recursive=True):
        path = Path(path_str)
        
        # Only include files (not directories)
        if not path.is_file():
            continue
        
        # Check ignore patterns
        try:
            rel_path = path.relative_to(working_dir)
            rel_path_str = str(rel_path).replace('\\', '/')
            
            # Check if any ignore pattern matches
            ignored = False
            for ignore_pattern in ignore_patterns:
                if fnmatch(rel_path_str, ignore_pattern):
                    ignored = True
                    break
            
            if not ignored:
                matches.append(path)
                
        except ValueError:
            # File is outside working directory - skip
            continue
    
    return matches


def _make_storage_decisions(files: List[FileEntry], external_rules: List[ExternalRule], 
                           oras_size_limit: int) -> Dict[str, StorageDecision]:
    """
    Decide whether each file goes to ORAS or external storage.
    
    Args:
        files: Files to classify
        external_rules: External storage rules
        oras_size_limit: Maximum file size for ORAS storage
        
    Returns:
        Dict mapping file paths to storage decisions
    """
    decisions = {}
    
    for file_entry in files:
        # Check if any external rule matches
        external_match = None
        for rule in external_rules:
            if rule.matches(file_entry.artifact_path, file_entry.size):
                external_match = rule
                break
        
        if external_match:
            decisions[file_entry.artifact_path] = StorageDecision.EXTERNAL
        elif file_entry.size > oras_size_limit:
            # File too large for ORAS and no external rule matches
            raise ValueError(
                f"File {file_entry.artifact_path} ({_format_bytes(file_entry.size)}) "
                f"exceeds ORAS limit ({_format_bytes(oras_size_limit)}) but no external "
                f"storage rule matches. Add an external rule for this file pattern."
            )
        else:
            decisions[file_entry.artifact_path] = StorageDecision.ORAS
    
    return decisions


def _compute_file_hash(file_path: Path) -> str:
    """
    Compute SHA256 hash of file contents.
    
    Args:
        file_path: Path to file
        
    Returns:
        SHA256 hash as hex string (without sha256: prefix)
    """
    sha256_hash = hashlib.sha256()
    
    with open(file_path, 'rb') as f:
        # Read in chunks to handle large files
        for chunk in iter(lambda: f.read(8192), b""):
            sha256_hash.update(chunk)
    
    return sha256_hash.hexdigest()


def _generate_external_uri(file_entry: FileEntry, external_rules: List[ExternalRule]) -> str:
    """
    Generate external storage URI for a file.
    
    Args:
        file_entry: File to generate URI for
        external_rules: External storage rules
        
    Returns:
        External storage URI
    """
    # Find the first matching rule
    for rule in external_rules:
        if rule.matches(file_entry.artifact_path, file_entry.size):
            return rule.format_uri(file_entry.artifact_path)
    
    # No rule matches - this shouldn't happen if storage decisions were made correctly
    raise ValueError(f"No external rule matches for {file_entry.artifact_path}")


def _determine_storage_tier(file_entry: FileEntry, external_rules: List[ExternalRule]):
    """Determine storage tier for external file."""
    for rule in external_rules:
        if rule.matches(file_entry.artifact_path, file_entry.size):
            return rule.tier
    
    # Default tier if no rule matches
    from .models import StorageTier
    return StorageTier.HOT


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


# TODO: Implement "no changes" detection by comparing layer digests with remote bundle
# This would allow skipping push if nothing has changed since last push
def detect_changes(plan: StoragePlan, remote_manifest: BundleManifest = None) -> bool:
    """
    Detect if bundle has changes compared to remote version.
    
    TODO: Compare local layer digests with remote bundle manifest.
    If all layer digests match, return False to skip push.
    Requires resolving remote bundle first.
    
    Args:
        plan: Local storage plan
        remote_manifest: Remote bundle manifest (None to force push)
        
    Returns:
        True if changes detected or remote_manifest is None
    """
    if remote_manifest is None:
        return True  # No remote to compare against
    
    # TODO: Implement digest comparison
    return True  # Always assume changes for now