"""
ModelOps Bundles CLI

Implements 8 CLI verbs with Operations facade integration:
- resolve: Resolve bundle identity without side effects
- materialize/pull: Materialize bundle layers to filesystem  
- export: Export workdir to deterministic archive
- scan: Scan workdir for bundle configuration (stub)
- plan: Show storage plan for bundle creation (stub)
- diff: Compare bundle or workdir (stub)
- push: Push bundle to registry (stub)
"""
from __future__ import annotations

import os
import typer
from pathlib import Path
from typing import Optional

from modelops_contracts.artifacts import BundleRef
from .operations import Operations, OpsConfig, run_and_exit
from .operations.printers import (
    print_resolved_bundle, print_materialize_summary, print_export_summary,
    print_push_summary, print_stub_message
)
from .runtime_types import ContentProvider
from .providers.bundle_content import default_provider_from_env

app = typer.Typer(name="modelops-bundles", help="ModelOps Bundles CLI")

def _parse_bundle_ref(ref_str: str) -> BundleRef:
    """
    Parse bundle reference string into BundleRef object.
    
    Supports formats:
    - "name:version" -> BundleRef(name, version)  
    - "name@sha256:digest" -> BundleRef(name, digest)
    - "/local/path" -> BundleRef(local_path)
    - r"C:\\local\\path" -> BundleRef(local_path) [Windows]
    
    Note: Bare digests ("sha256:digest" or "@sha256:digest") are NOT supported.
    
    Args:
        ref_str: Bundle reference string
        
    Returns:
        BundleRef object
        
    Raises:
        ValueError: If ref_str format is invalid
    """
    ref_str = ref_str.strip()
    
    # Support name@sha256:digest format  
    if "@" in ref_str and "sha256:" in ref_str.split("@", 1)[1]:
        name, digest = ref_str.split("@", 1)
        if not name:  # Empty name before @ - this is a bare digest
            raise ValueError("Bare digests not supported. Use name@sha256:<digest>")
        return BundleRef(name=name, digest=digest.lower())
    
    # Reject bare digests
    elif ref_str.startswith("sha256:") or ref_str.startswith("@sha256:"):
        raise ValueError("Bare digests not supported. Use name@sha256:<digest>")
    
    # Local paths - Windows paths need special handling due to colon
    elif os.path.isabs(ref_str):
        return BundleRef(local_path=ref_str)
    elif ref_str.startswith("./") or ref_str.startswith("../") or ref_str.startswith(".\\") or ref_str.startswith("..\\"):
        return BundleRef(local_path=ref_str)
    
    # name:version format
    elif ":" in ref_str:
        name, version = ref_str.split(":", 1)
        return BundleRef(name=name, version=version)
    
    raise ValueError(f"Invalid bundle reference format: {ref_str}")

def _create_provider(provider_name: Optional[str] = None) -> Optional[ContentProvider]:
    """
    Create content provider for operations.
    
    Args:
        provider_name: Provider type override for testing
        
    Returns:
        Content provider instance or None for resolve-only operations
    """
    if provider_name == "fake":
        # Import here to avoid dependency on test code
        try:
            from tests.fakes.fake_provider import FakeProvider
            return FakeProvider()
        except ImportError:
            # Try alternate path for tests run outside pytest
            try:
                import sys
                import os
                sys.path.insert(0, os.path.join(os.getcwd(), 'tests'))
                from fakes.fake_provider import FakeProvider
                return FakeProvider()
            except ImportError:
                typer.echo("Warning: FakeProvider not available, using real provider")
    
    # Use real provider for production
    return default_provider_from_env()

def _create_fake_registry():
    """
    Create FakeOciRegistry for testing.
    
    Returns:
        FakeOciRegistry instance with seeded test data, or None if unavailable
    """
    try:
        from tests.storage.fakes.fake_oci_registry import FakeOciRegistry
        fake_registry = FakeOciRegistry()
        # Add some fake manifests for testing
        _add_fake_manifests_oci(fake_registry)
        return fake_registry
    except ImportError:
        typer.echo("Warning: FakeOciRegistry not available")
        return None

def _add_fake_manifests_oci(fake_registry):
    """Add fake manifests to FakeOciRegistry for testing."""
    import json
    import hashlib
    
    # Import OCI helper
    try:
        from tests.helpers.oci_helpers import setup_fake_bundle_in_registry, create_oci_image_manifest
    except ImportError:
        # Fallback if helpers not available
        typer.echo("Warning: OCI helpers not available for fake registry setup")
        return
    
    repo = "testns/bundles/bundle"
    
    # Create layer indexes and blobs
    layer_blobs = {}
    layer_indexes = {}
    
    # Create ORAS content for code and config layers
    for layer in ["code", "config"]:
        layer_entries = []
        for i in range(2):  # Fewer files for simplicity
            fake_content = f"fake-{layer}-content-{i}".encode()
            content_digest = f"sha256:{hashlib.sha256(fake_content).hexdigest()}"
            layer_blobs[content_digest] = fake_content
            
            layer_entries.append({
                "path": f"{layer}/file{i}.txt",
                "digest": content_digest
            })
        
        # Create layer index
        layer_index = {
            "mediaType": "application/vnd.modelops.layer+json",
            "entries": layer_entries
        }
        layer_payload = json.dumps(layer_index, sort_keys=True, separators=(',', ':')).encode()
        layer_digest = f"sha256:{hashlib.sha256(layer_payload).hexdigest()}"
        layer_blobs[layer_digest] = layer_payload
        layer_indexes[layer] = layer_digest
    
    # Create data layer with external entries
    data_layer_index = {
        "mediaType": "application/vnd.modelops.layer+json",
        "entries": [
            {
                "path": "data/train.csv",
                "external": {
                    "uri": "az://fake-container/train.csv",
                    "sha256": "1234567890abcdef" * 8,  # 64 chars
                    "size": 1024,
                    "tier": "hot"
                }
            },
            {
                "path": "data/test.csv", 
                "external": {
                    "uri": "az://fake-container/test.csv",
                    "sha256": "abcdef1234567890" * 8,  # 64 chars
                    "size": 512,
                    "tier": "cool"
                }
            }
        ]
    }
    data_payload = json.dumps(data_layer_index, sort_keys=True, separators=(',', ':')).encode()
    data_digest = f"sha256:{hashlib.sha256(data_payload).hexdigest()}"
    layer_blobs[data_digest] = data_payload
    layer_indexes["data"] = data_digest
    
    # Create bundle manifest for "bundle" name (used by CLI tests)
    bundle_manifest = {
        "mediaType": "application/vnd.modelops.bundle.manifest+json",
        "name": "bundle",
        "version": "1.0.0",
        "roles": {
            "default": ["code", "config"],
            "runtime": ["code"],
            "training": ["code", "config", "data"]
        },
        "layers": list(layer_indexes.keys()),  # List of layer names
        "layer_indexes": layer_indexes,
        "external_index_present": True
    }
    
    # Set up bundle using helper
    setup_fake_bundle_in_registry(
        fake_registry, 
        repo, 
        bundle_manifest, 
        "v1.0.0",
        layer_blobs
    )
    
    # Also tag with "1.0.0" for compatibility
    bundle_manifest_bytes = json.dumps(bundle_manifest, sort_keys=True, separators=(',', ':')).encode()
    oci_manifest_bytes = create_oci_image_manifest(bundle_manifest_bytes)
    fake_registry.put_manifest(repo, "application/vnd.oci.image.manifest.v1+json", oci_manifest_bytes, "1.0.0")

@app.command()
def resolve(
    bundle_ref: str = typer.Argument(..., help="Bundle reference to resolve"),
    no_cache: bool = typer.Option(False, "--no-cache", help="Disable bundle caching"),
    provider: Optional[str] = typer.Option(None, "--provider", envvar="MODELOPS_PROVIDER", hidden=True, help="Provider override for testing"),
    verbose: bool = typer.Option(False, "--verbose", help="Show detailed output")
) -> None:
    """Resolve bundle identity without side effects."""
    
    def _resolve() -> None:
        ref = _parse_bundle_ref(bundle_ref)
        config = OpsConfig(cache=not no_cache, verbose=verbose)
        
        # Create registry and settings based on provider type
        if provider == "fake":
            registry = _create_fake_registry()
            # Use fake settings for testing
            from .settings import Settings
            settings = Settings(
                registry_url="http://fake-registry:5000",
                registry_repo="testns"
            )
            if registry is None:
                # Fallback to real registry
                from .settings import load_settings_from_env
                from .storage.registry_factory import make_registry
                settings = load_settings_from_env()
                registry = make_registry(settings)
        else:
            # Production path - create registry using factory
            from .settings import load_settings_from_env
            from .storage.registry_factory import make_registry
            settings = load_settings_from_env()
            registry = make_registry(settings)
        
        ops = Operations(config=config, registry=registry, settings=settings)
        
        resolved = ops.resolve(ref)
        print_resolved_bundle(resolved, verbose=verbose)
    
    run_and_exit(_resolve)

@app.command()
def materialize(
    bundle_ref: str = typer.Argument(..., help="Bundle reference to materialize"),
    dest: str = typer.Argument(..., help="Destination directory"),
    role: Optional[str] = typer.Option(None, "--role", help="Role to materialize"),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing files"),
    prefetch_external: bool = typer.Option(False, "--prefetch-external", help="Download external data immediately"),
    no_cache: bool = typer.Option(False, "--no-cache", help="Disable bundle caching"),
    ci: bool = typer.Option(False, "--ci", help="CI mode (suppress progress)"),
    provider: Optional[str] = typer.Option(None, "--provider", envvar="MODELOPS_PROVIDER", hidden=True, help="Provider override for testing"),
    verbose: bool = typer.Option(False, "--verbose", help="Show detailed output")
) -> None:
    """Materialize bundle layers to filesystem."""
    
    def _materialize() -> None:
        ref = _parse_bundle_ref(bundle_ref)
        config = OpsConfig(cache=not no_cache, ci=ci, verbose=verbose)
        
        # Load settings for all paths
        from .settings import load_settings_from_env
        settings = load_settings_from_env()
        
        # Create registry and provider based on provider type
        if provider == "fake":
            registry = _create_fake_registry()
            if registry is None:
                # Fallback to real registry
                from .storage.registry_factory import make_registry
                registry = make_registry(settings)
            
            # Use fake external store too
            try:
                from tests.storage.fakes.fake_external import FakeExternalStore
                external = FakeExternalStore()
            except ImportError:
                from .storage.object_store import AzureExternalAdapter
                external = AzureExternalAdapter(settings=settings)
        else:
            # Production path - create real adapters
            from .storage.registry_factory import make_registry
            from .storage.object_store import AzureExternalAdapter
            
            registry = make_registry(settings)
            external = AzureExternalAdapter(settings=settings)
        
        # Always pass settings to provider
        from .providers.bundle_content import BundleContentProvider
        content_provider = BundleContentProvider(
            registry=registry, 
            external=external,
            settings=settings
        )
        
        ops = Operations(
            config=config,
            provider=content_provider,
            registry=registry
        )
        
        result = ops.materialize(
            ref=ref,
            dest=dest,
            role=role,
            overwrite=overwrite,
            prefetch_external=prefetch_external
        )
        
        print_materialize_summary(result.bundle, result.dest_path, result.selected_role)
    
    run_and_exit(_materialize)

@app.command()
def pull(
    bundle_ref: str = typer.Argument(..., help="Bundle reference to pull"),
    dest: str = typer.Argument(..., help="Destination directory"),
    role: Optional[str] = typer.Option(None, "--role", help="Role to pull"),
    overwrite: bool = typer.Option(False, "--overwrite", help="Overwrite existing files"),
    prefetch_external: bool = typer.Option(False, "--prefetch-external", help="Download external data immediately"),
    no_cache: bool = typer.Option(False, "--no-cache", help="Disable bundle caching"),
    ci: bool = typer.Option(False, "--ci", help="CI mode (suppress progress)"),
    provider: Optional[str] = typer.Option(None, "--provider", envvar="MODELOPS_PROVIDER", hidden=True, help="Provider override for testing"),
    verbose: bool = typer.Option(False, "--verbose", help="Show detailed output")
) -> None:
    """Pull bundle (alias for materialize)."""
    
    def _pull() -> None:
        ref = _parse_bundle_ref(bundle_ref)
        config = OpsConfig(cache=not no_cache, ci=ci, verbose=verbose)
        
        # Load settings for all paths
        from .settings import load_settings_from_env
        settings = load_settings_from_env()
        
        # Create registry and provider based on provider type
        if provider == "fake":
            registry = _create_fake_registry()
            if registry is None:
                # Fallback to real registry
                from .storage.registry_factory import make_registry
                registry = make_registry(settings)
            
            # Use fake external store too
            try:
                from tests.storage.fakes.fake_external import FakeExternalStore
                external = FakeExternalStore()
            except ImportError:
                from .storage.object_store import AzureExternalAdapter
                external = AzureExternalAdapter(settings=settings)
        else:
            # Production path - create real adapters
            from .storage.registry_factory import make_registry
            from .storage.object_store import AzureExternalAdapter
            
            registry = make_registry(settings)
            external = AzureExternalAdapter(settings=settings)
        
        # Always pass settings to provider
        from .providers.bundle_content import BundleContentProvider
        content_provider = BundleContentProvider(
            registry=registry, 
            external=external,
            settings=settings
        )
        
        ops = Operations(
            config=config,
            provider=content_provider,
            registry=registry
        )
        
        result = ops.pull(
            ref=ref,
            dest=dest,
            role=role,
            overwrite=overwrite,
            prefetch_external=prefetch_external
        )
        
        print_materialize_summary(result.bundle, result.dest_path, result.selected_role)
    
    run_and_exit(_pull)

@app.command()
def export(
    src_dir: str = typer.Argument(..., help="Source directory to export"),
    out_path: Optional[str] = typer.Argument(None, help="Output archive path (auto-generated if not provided)"),
    compression: str = typer.Option("zstd", "--compression", help="Compression format: zstd or none"),
    include_external: bool = typer.Option(False, "--include-external", help="Include external data bytes")
) -> None:
    """Export materialized workdir to deterministic archive."""
    
    def _export() -> None:
        # Validate and map compression format
        compression_map = {
            "zstd": ("tar.zst", True),
            "none": ("tar", False)
        }
        
        if compression not in compression_map:
            raise typer.BadParameter(f"Invalid compression '{compression}'. Use 'zstd' or 'none'.")
        
        ext, use_compression = compression_map[compression]
        
        # Generate output path if not provided
        if out_path is None:
            src_name = Path(src_dir).name or "archive"
            final_out_path = f"{src_name}.{ext}"
        else:
            final_out_path = out_path
            # Validate extension matches compression choice
            if use_compression and not final_out_path.endswith(('.tar.zst', '.zst')):
                raise typer.BadParameter(f"With --compression zstd, output path must end with .tar.zst or .zst")
            elif not use_compression and not final_out_path.endswith('.tar'):
                raise typer.BadParameter(f"With --compression none, output path must end with .tar")
        
        # Export doesn't need registry access, so call directly
        from modelops_bundles.export import write_deterministic_archive
        
        write_deterministic_archive(
            src_dir=src_dir,
            out_path=final_out_path,
            include_external=include_external,
            zstd_level=19  # Fixed level for determinism
        )
        
        print_export_summary(src_dir, final_out_path, include_external)
    
    run_and_exit(_export)

@app.command()
def scan(
    working_dir: str = typer.Argument(".", help="Directory to scan")
) -> None:
    """Scan working directory for bundle configuration."""
    
    def _scan() -> None:
        config = OpsConfig()
        # Provide minimal registry for stub operations 
        registry = _create_fake_registry()
        if registry is None:
            from .settings import load_settings_from_env
            from .storage.registry_factory import make_registry
            settings = load_settings_from_env()
            registry = make_registry(settings)
        else:
            # Use fake settings for testing
            from .settings import Settings
            settings = Settings(
                registry_url="http://fake-registry:5000",
                registry_repo="testns"
            )
        ops = Operations(config=config, registry=registry, settings=settings)
        
        result = ops.scan(working_dir)
        print_stub_message("scan")
        typer.echo(result)
    
    run_and_exit(_scan)

@app.command()
def plan(
    working_dir: str = typer.Argument(".", help="Directory to analyze"),
    external_preview: bool = typer.Option(False, "--external-preview", help="Preview external storage decisions")
) -> None:
    """Show storage plan for bundle creation."""
    
    def _plan() -> None:
        config = OpsConfig()
        # Provide minimal registry for stub operations
        registry = _create_fake_registry()
        if registry is None:
            from .settings import load_settings_from_env
            from .storage.registry_factory import make_registry
            settings = load_settings_from_env()
            registry = make_registry(settings)
        ops = Operations(config=config, registry=registry)
        
        result = ops.plan(working_dir, external_preview=external_preview)
        print_stub_message("plan")
        typer.echo(result)
    
    run_and_exit(_plan)

@app.command()
def diff(
    ref_or_path: str = typer.Argument(..., help="Bundle reference or local path")
) -> None:
    """Compare bundle or working directory."""
    
    def _diff() -> None:
        config = OpsConfig()
        # Provide minimal registry for stub operations
        registry = _create_fake_registry()
        if registry is None:
            from .settings import load_settings_from_env
            from .storage.registry_factory import make_registry
            settings = load_settings_from_env()
            registry = make_registry(settings)
        ops = Operations(config=config, registry=registry)
        
        result = ops.diff(ref_or_path)
        print_stub_message("diff")
        typer.echo(result)
    
    run_and_exit(_diff)

@app.command()
def push(
    working_dir: str = typer.Argument(".", help="Directory containing bundle"),
    bump: Optional[str] = typer.Option(None, "--bump", help="Version bump strategy (patch, minor, major)"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Show what would be pushed without actually pushing"),
    force: bool = typer.Option(False, "--force", help="Skip change detection and always push")
) -> None:
    """Push bundle to registry."""
    
    def _push() -> None:
        config = OpsConfig()
        
        # Create registry from settings
        from .settings import load_settings_from_env
        from .storage.registry_factory import make_registry
        
        settings = load_settings_from_env()
        registry = make_registry(settings)
        ops = Operations(
            config=config,
            registry=registry,
            settings=settings
        )
        
        # Push bundle - this now returns a digest
        digest = ops.push(working_dir, bump=bump, dry_run=dry_run, force=force)
        print_push_summary(digest, working_dir, bump)
    
    run_and_exit(_push)

def main() -> None:
    """CLI entry point."""
    app()

if __name__ == "__main__":
    main()