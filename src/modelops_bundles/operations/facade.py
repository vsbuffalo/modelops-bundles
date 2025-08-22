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

from ..runtime import resolve as _resolve, materialize as _materialize
from ..runtime_types import ContentProvider

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
    
    def __init__(self, config: OpsConfig, provider: Optional[ContentProvider] = None):
        """
        Initialize Operations facade.
        
        Args:
            config: Configuration settings
            provider: Content provider for materialize operations (None for resolve-only)
        """
        self.cfg = config
        self.provider = provider

    def resolve(self, ref: BundleRef) -> ResolvedBundle:
        """
        Resolve bundle identity without side effects.
        
        Args:
            ref: Bundle reference to resolve
            
        Returns:
            Resolved bundle with manifest digest and metadata
        """
        return _resolve(ref, cache=self.cfg.cache)

    def materialize(self, ref: BundleRef, dest: str, *,
                    role: Optional[str] = None,
                    overwrite: bool = False,
                    prefetch_external: bool = False) -> ResolvedBundle:
        """
        Materialize bundle layers to filesystem.
        
        Args:
            ref: Bundle reference to materialize
            dest: Destination directory path
            role: Role to materialize (None for default)
            overwrite: Whether to overwrite existing files
            prefetch_external: Whether to download external data immediately
            
        Returns:
            Resolved bundle that was materialized
            
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
            provider=self.provider
        )

    def pull(self, ref: BundleRef, dest: str, *,
             role: Optional[str] = None,
             overwrite: bool = False,
             prefetch_external: bool = False) -> ResolvedBundle:
        """
        Pull bundle (alias for materialize).
        
        Args:
            ref: Bundle reference to pull
            dest: Destination directory path
            role: Role to pull (None for default)
            overwrite: Whether to overwrite existing files
            prefetch_external: Whether to download external data immediately
            
        Returns:
            Resolved bundle that was pulled
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

    # Stubbed commands for Stage 5 (smoke testing with fakes)
    
    def scan(self, working_dir: str) -> str:
        """
        Scan working directory for bundle configuration.
        
        Stage 5: Stubbed for smoke testing.
        
        Args:
            working_dir: Directory to scan
            
        Returns:
            Human-readable scan results
        """
        return f"Scanned {working_dir} (stub)"

    def plan(self, working_dir: str, *, external_preview: bool = False) -> str:
        """
        Show storage plan for bundle creation.
        
        Stage 5: Stubbed for smoke testing.
        
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
        
        Stage 5: Stubbed for smoke testing.
        
        Args:
            ref_or_path: Bundle reference or local path
            
        Returns:
            Human-readable diff results
        """
        return f"Diff for {ref_or_path} (stub)"

    def push(self, working_dir: str, *, bump: Optional[str] = None) -> str:
        """
        Push bundle to registry.
        
        Stage 5: Stubbed for smoke testing.
        
        Args:
            working_dir: Directory containing bundle
            bump: Version bump strategy (patch, minor, major)
            
        Returns:
            Human-readable push results
        """
        bump_info = f" with {bump} bump" if bump else ""
        return f"Pushed {working_dir}{bump_info} (stub)"