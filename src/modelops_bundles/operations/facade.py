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
from ..storage.base import BundleRegistryStore

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
                 registry: Optional[BundleRegistryStore] = None, repository: Optional[str] = None):
        """
        Initialize Operations facade.
        
        Args:
            config: Configuration settings
            provider: Content provider for materialize operations (None for resolve-only)
            registry: Bundle registry store (if None, loaded from environment)
            repository: Repository namespace (if None, loaded from environment)
        """
        self.cfg = config
        self.provider = provider
        
        if registry is not None:
            # Use provided registry and repository (explicit injection)
            if repository is None:
                raise ValueError("repository must be provided when injecting registry")
            self.registry = registry
            self.repository = repository
        else:
            # Create registry and repository from environment settings
            from ..settings import load_settings_from_env
            from ..storage.oras import OrasAdapter
            settings = load_settings_from_env()
            self.registry = OrasAdapter(settings=settings)
            self.repository = settings.registry_repo

    def resolve(self, ref: BundleRef) -> ResolvedBundle:
        """
        Resolve bundle identity without side effects.
        
        Args:
            ref: Bundle reference to resolve
            
        Returns:
            Resolved bundle with manifest digest and metadata
        """
        return _resolve(ref, registry=self.registry, repository=self.repository, cache=self.cfg.cache)

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
            Tuple of (ResolvedBundle, selected_role) where selected_role is the role that was actually used
            
        Raises:
            AssertionError: If no provider configured for materialization
        """
        assert self.provider is not None, "Provider required for materialize operations"
        
        return _materialize(
            ref=ref,
            dest=dest,
            role=role,
            overwrite=overwrite,
            prefetch_external=prefetch_external,
            provider=self.provider,
            registry=self.registry,
            repository=self.repository
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
            Tuple of (ResolvedBundle, selected_role) where selected_role is the role that was actually used
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

    def push(self, working_dir: str, *, bump: Optional[str] = None) -> str:
        """
        Push bundle to registry.
        
        Stubbed implementation.
        
        Args:
            working_dir: Directory containing bundle
            bump: Version bump strategy (patch, minor, major)
            
        Returns:
            Human-readable push results
        """
        bump_info = f" with {bump} bump" if bump else ""
        return f"Pushed {working_dir}{bump_info} (stub)"