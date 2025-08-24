"""
Operations Facade - Application service layer.

Provides a clean interface between CLI and runtime APIs, centralizing
command orchestration, configuration, and policy decisions while keeping
CLI commands thin and testable.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from modelops_contracts.artifacts import BundleRef, ResolvedBundle

from ..runtime import resolve as _resolve, materialize as _materialize, MaterializeResult
from ..runtime_types import ContentProvider
from ..storage.oras_bundle_registry import OrasBundleRegistry


def _apply_version_bump(current_version: str, bump: str) -> str:
    """
    Apply semantic version bump to current version.
    
    Args:
        current_version: Current version string (e.g., "1.2.3")
        bump: Bump strategy ("patch", "minor", "major")
        
    Returns:
        New version string
        
    Raises:
        ValueError: If version format is invalid or bump strategy is unknown
    """
    # Simple semver parsing - handle "v" prefix
    version = current_version.lstrip("v")
    
    try:
        parts = version.split(".")
        if len(parts) != 3:
            raise ValueError("Version must be in format 'major.minor.patch'")
        
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])
        
        if bump == "patch":
            patch += 1
        elif bump == "minor":
            minor += 1
            patch = 0
        elif bump == "major":
            major += 1
            minor = 0
            patch = 0
        else:
            raise ValueError(f"Unknown bump strategy: {bump}. Use 'patch', 'minor', or 'major'")
        
        # Preserve "v" prefix if it was present
        new_version = f"{major}.{minor}.{patch}"
        if current_version.startswith("v"):
            new_version = f"v{new_version}"
        
        return new_version
        
    except (ValueError, IndexError) as e:
        raise ValueError(f"Invalid version format '{current_version}': {e}") from e

@dataclass(frozen=True)
class OpsConfig:
    """
    Configuration for Operations facade.
    
    Centralizes policy decisions like caching, output formatting,
    and compression settings to avoid scattered configuration.
    """
    ci: bool = False              # Running in CI environment
    cache: bool = True            # Enable bundle caching
    zstd_level: int = 19          # Fixed compression level for determinism
    human: bool = True            # Human text output (JSON mode in future)
    verbose: bool = False         # Show detailed output

class Operations:
    """
    Application service facade for CLI operations.
    
    Design Notes: Operations Facade
    
    This facade provides a clean separation between CLI parsing/formatting
    and the core runtime logic. It centralizes:
    
    - Command orchestration (one method per CLI verb)
    - Configuration policy (caching, compression, output modes)
    - Provider injection (enables testing with fakes)
    - Error boundary (exceptions bubble up for central mapping)
    
    The facade is stateless except for injected config and provider,
    making it easy to test and reason about. Each method delegates
    to the appropriate runtime/export functions while applying
    configuration policy consistently.
    """
    
    def __init__(self, config: OpsConfig, provider: Optional[ContentProvider] = None, 
                 registry: Optional[OrasBundleRegistry] = None, settings = None):
        """
        Initialize Operations facade.
        
        Args:
            config: Configuration settings
            provider: Content provider for materialize operations (None for resolve-only)
            registry: OCI registry (if None, loaded from environment)
            settings: Optional settings (if None, loaded from environment)
        """
        self.cfg = config
        self.provider = provider
        
        # Load settings if not provided
        if settings is None:
            from ..settings import create_settings_from_env
            settings = create_settings_from_env()
        self.settings = settings
        
        # Create registry if not provided
        if registry is None:
            self.registry = OrasBundleRegistry(settings)
        else:
            self.registry = registry

    def resolve(self, ref: BundleRef) -> ResolvedBundle:
        """
        Resolve bundle identity without side effects.
        
        Args:
            ref: Bundle reference to resolve
            
        Returns:
            Resolved bundle with manifest digest and metadata
        """
        return _resolve(ref, registry=self.registry, settings=self.settings, cache=self.cfg.cache)

    def materialize(self, ref: BundleRef, dest: str, *,
                    role: Optional[str] = None,
                    overwrite: bool = False,
                    prefetch_external: bool = False) -> MaterializeResult:
        """
        Materialize bundle layers to filesystem.
        
        Args:
            ref: Bundle reference to materialize
            dest: Destination directory path
            role: Role to materialize (None for default)
            overwrite: Whether to overwrite existing files
            prefetch_external: Whether to download external data immediately
            
        Returns:
            MaterializeResult containing bundle, selected_role, and dest_path
            
        Raises:
            ValueError: If no provider configured for materialization
        """
        if self.provider is None:
            raise ValueError("Provider required for materialize operations")
        
        return _materialize(
            ref=ref,
            dest=dest,
            role=role,
            overwrite=overwrite,
            prefetch_external=prefetch_external,
            provider=self.provider,
            registry=self.registry,
            settings=self.settings
        )

    def pull(self, ref: BundleRef, dest: str, *,
             role: Optional[str] = None,
             overwrite: bool = False,
             prefetch_external: bool = False) -> MaterializeResult:
        """
        Pull bundle (alias for materialize).
        
        Args:
            ref: Bundle reference to pull
            dest: Destination directory path
            role: Role to pull (None for default)
            overwrite: Whether to overwrite existing files
            prefetch_external: Whether to download external data immediately
            
        Returns:
            MaterializeResult containing bundle, selected_role, and dest_path
        """
        return self.materialize(
            ref=ref,
            dest=dest,
            role=role,
            overwrite=overwrite,
            prefetch_external=prefetch_external
        )

    def export(self, src_dir: str, out_path: str, *,
               include_external: bool = False) -> None:
        """
        Export materialized workdir to deterministic archive.
        
        Creates byte-identical archives from identical input trees
        by normalizing paths, tar headers, and compression settings.
        
        Args:
            src_dir: Source directory to export
            out_path: Output archive path (.tar or .tar.zst)
            include_external: Whether to include external data bytes
        """
        # Import here to avoid circular dependencies
        from ..export import write_deterministic_archive
        
        write_deterministic_archive(
            src_dir=src_dir,
            out_path=out_path,
            include_external=include_external,
            zstd_level=self.cfg.zstd_level
        )

    # Stubbed commands for development
    
    def scan(self, working_dir: str) -> str:
        """
        Scan working directory for bundle configuration.
        
        Stubbed implementation.
        
        Args:
            working_dir: Directory to scan
            
        Returns:
            Human-readable scan results
        """
        return f"Scanned {working_dir} (stub)"

    def plan(self, working_dir: str, *, external_preview: bool = False) -> str:
        """
        Show storage plan for bundle creation.
        
        Stubbed implementation.
        
        Args:
            working_dir: Directory to analyze
            external_preview: Whether to preview external storage decisions
            
        Returns:
            Human-readable storage plan
        """
        preview = " with external preview" if external_preview else ""
        return f"Storage plan for {working_dir}{preview} (stub)"

    def diff(self, ref_or_path: str) -> str:
        """
        Compare bundle or working directory.
        
        Stubbed implementation.
        
        Args:
            ref_or_path: Bundle reference or local path
            
        Returns:
            Human-readable diff results
        """
        return f"Diff for {ref_or_path} (stub)"

    def push(self, working_dir: str, *, bump: Optional[str] = None, 
             dry_run: bool = False, force: bool = False) -> str:
        """
        Push bundle to registry.
        
        Args:
            working_dir: Directory containing bundle
            bump: Version bump strategy (patch, minor, major)
            dry_run: Show what would be pushed without actually pushing
            force: Skip change detection and always push
            
        Returns:
            Canonical manifest digest (sha256:...)
        """
        # Handle version bumping if requested
        if bump:
            from pathlib import Path
            from ..planner import scan_directory
            
            # Load current spec to get version
            spec = scan_directory(Path(working_dir))
            current_version = spec.version
            new_version = _apply_version_bump(current_version, bump)
            
            # Update version in spec file
            _update_version_in_spec(Path(working_dir), new_version)
            print(f"🔄 Version bumped: {current_version} -> {new_version}")
        
        # Delegate to publisher
        from ..publisher import push_bundle
        
        return push_bundle(
            working_dir=working_dir,
            registry=self.registry,
            settings=self.settings,
            force=force,
            dry_run=dry_run
        )


def _update_version_in_spec(working_dir: "Path", new_version: str) -> None:
    """
    Update version in modelops.yaml file.
    
    Args:
        working_dir: Directory containing modelops.yaml
        new_version: New version to set
    """
    import yaml
    
    # Find spec file
    spec_files = ["modelops.yaml", "modelops.yml", ".mops-bundle.yaml", ".mops-bundle.yml"]
    spec_path = None
    
    for filename in spec_files:
        candidate = working_dir / filename
        if candidate.exists():
            spec_path = candidate
            break
    
    if not spec_path:
        raise FileNotFoundError("No bundle specification file found")
    
    # Load, update, and save
    with open(spec_path, 'r') as f:
        data = yaml.safe_load(f)
    
    # Update version in metadata
    if "metadata" not in data:
        data["metadata"] = {}
    data["metadata"]["version"] = new_version
    
    with open(spec_path, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False)