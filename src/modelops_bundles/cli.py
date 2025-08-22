"""
ModelOps Bundles CLI - Minimal Typer interface for Stage 5.

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

import typer
from pathlib import Path
from typing import Optional

from modelops_contracts.artifacts import BundleRef
from .operations import Operations, OpsConfig, run_and_exit
from .operations.printers import (
    print_resolved_bundle, print_materialize_summary, print_export_summary,
    print_stub_message
)
from .runtime_types import ContentProvider
from .providers.bundle_content import default_provider_from_env

app = typer.Typer(name="modelops-bundles", help="ModelOps Bundles CLI")

def _parse_bundle_ref(ref_str: str) -> BundleRef:
    """
    Parse bundle reference string into BundleRef object.
    
    Supports formats:
    - "name:version" -> BundleRef(name, version)  
    - "sha256:digest" -> BundleRef(digest)
    - "@sha256:digest" -> BundleRef(digest) [OCI format]
    - "/local/path" -> BundleRef(local_path)
    
    Args:
        ref_str: Bundle reference string
        
    Returns:
        BundleRef object
        
    Raises:
        ValueError: If ref_str format is invalid
    """
    if ref_str.startswith("sha256:"):
        return BundleRef(digest=ref_str)
    elif ref_str.startswith("@sha256:"):
        # OCI digest format - strip the @ prefix
        return BundleRef(digest=ref_str[1:])
    elif ref_str.startswith("/") or ref_str.startswith("./") or ref_str.startswith("../"):
        return BundleRef(local_path=ref_str)
    elif ":" in ref_str:
        parts = ref_str.split(":", 1)  # Split only on first ':'
        if len(parts) == 2:
            # Reject names containing "/" to avoid namespace confusion
            if "/" in parts[0]:
                raise ValueError("Bundle names cannot contain '/'. Use digest format for full registry paths")
            return BundleRef(name=parts[0], version=parts[1])
    
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
            from .test.fake_provider import FakeProvider
            return FakeProvider()
        except ImportError:
            typer.echo("Warning: FakeProvider not available, using real provider")
    
    # Use real provider for production
    return default_provider_from_env()

def _create_registry_store(provider_name: Optional[str] = None):
    """
    Create BundleRegistryStore for resolve operations.
    
    Args:
        provider_name: Provider type override for testing
        
    Returns:
        BundleRegistryStore instance for registry access
    """
    if provider_name == "fake":
        # Import here to avoid dependency on test code
        try:
            from .storage.fakes.fake_oras import FakeBundleRegistryStore
            fake_store = FakeBundleRegistryStore()
            # Add some fake manifests for testing
            _add_fake_manifests(fake_store)
            return fake_store
        except ImportError:
            typer.echo("Warning: FakeBundleRegistryStore not available, using real store")
    
    # Use real ORAS adapter for production
    from .settings import load_settings_from_env
    from .storage.oras import OrasAdapter
    settings = load_settings_from_env()
    return OrasAdapter(settings=settings)

def _add_fake_manifests(fake_store):
    """Add fake manifests to FakeBundleRegistryStore for testing."""
    import json
    import hashlib
    
    # First, create fake layer indexes with known digests
    layer_index_digests = {}
    
    for layer in ["code", "config", "data"]:
        layer_index = {
            "mediaType": "application/vnd.modelops.layer+json",
            "entries": []
        }
        
        if layer == "data":
            # Data layer has external entries
            layer_index["entries"] = [
                {
                    "path": "data/train.csv",
                    "external": {
                        "uri": "az://fake-container/train.csv",
                        "sha256": "fake-train-sha256",
                        "size": 1048576
                    }
                },
                {
                    "path": "data/test.csv", 
                    "external": {
                        "uri": "az://fake-container/test.csv",
                        "sha256": "fake-test-sha256",
                        "size": 512000
                    }
                }
            ]
        else:
            # Code/config layers have ORAS entries
            layer_index["entries"] = [
                {
                    "path": f"{layer}/example.txt",
                    "oras": {
                        "digest": f"sha256:fake-{layer}-blob-digest"
                    }
                }
            ]
        
        # Store layer index and get its computed digest
        layer_payload = json.dumps(layer_index).encode()
        digest = fake_store.put_manifest("application/vnd.modelops.layer+json", layer_payload)
        layer_index_digests[layer] = digest
    
    # Now create bundle manifest with actual layer index digests
    bundle_manifest = {
        "mediaType": "application/vnd.modelops.bundle.manifest+json",
        "roles": {
            "default": ["code", "config"],
            "runtime": ["code", "config"],
            "training": ["code", "config", "data"]
        },
        "layers": ["code", "config", "data"],
        "layer_indexes": layer_index_digests,
        "external_index_present": True
    }
    
    # Store bundle manifest
    bundle_payload = json.dumps(bundle_manifest).encode()
    bundle_digest = fake_store.put_manifest("application/vnd.modelops.bundle.manifest+json", bundle_payload)
    
    # Tag manifest with references for testing using public API
    fake_store.tag_manifest("test/repo/my/repo:1.2.3", bundle_digest)
    fake_store.tag_manifest("test/repo/bundle:v1.0.0", bundle_digest)  # For CLI smoke tests

@app.command()
def resolve(
    bundle_ref: str = typer.Argument(..., help="Bundle reference to resolve"),
    no_cache: bool = typer.Option(False, "--no-cache", help="Disable bundle caching"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider override for testing")
) -> None:
    """Resolve bundle identity without side effects."""
    
    def _resolve() -> None:
        ref = _parse_bundle_ref(bundle_ref)
        config = OpsConfig(cache=not no_cache)
        
        # Create registry and repository based on provider type
        if provider == "fake":
            registry = _create_registry_store(provider)
            ops = Operations(config=config, registry=registry, repository="test/repo")
        else:
            ops = Operations(config=config)
        
        resolved = ops.resolve(ref)
        print_resolved_bundle(resolved)
    
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
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider override for testing")
) -> None:
    """Materialize bundle layers to filesystem."""
    
    def _materialize() -> None:
        ref = _parse_bundle_ref(bundle_ref)
        config = OpsConfig(cache=not no_cache, ci=ci)
        content_provider = _create_provider(provider)
        
        # Create registry and repository based on provider type
        if provider == "fake":
            registry = _create_registry_store(provider)
            ops = Operations(config=config, provider=content_provider, registry=registry, repository="test/repo")
        else:
            ops = Operations(config=config, provider=content_provider)
        
        resolved = ops.materialize(
            ref=ref,
            dest=dest,
            role=role,
            overwrite=overwrite,
            prefetch_external=prefetch_external
        )
        
        # Determine actual role that was materialized
        actual_role = role or next(iter(resolved.roles.keys())) if resolved.roles else "unknown"
        print_materialize_summary(resolved, dest, actual_role)
    
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
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider override for testing")
) -> None:
    """Pull bundle (alias for materialize)."""
    
    def _pull() -> None:
        ref = _parse_bundle_ref(bundle_ref)
        config = OpsConfig(cache=not no_cache, ci=ci)
        content_provider = _create_provider(provider)
        
        # Create registry and repository based on provider type
        if provider == "fake":
            registry = _create_registry_store(provider)
            ops = Operations(config=config, provider=content_provider, registry=registry, repository="test/repo")
        else:
            ops = Operations(config=config, provider=content_provider)
        
        resolved = ops.pull(
            ref=ref,
            dest=dest,
            role=role,
            overwrite=overwrite,
            prefetch_external=prefetch_external
        )
        
        # Determine actual role that was pulled
        actual_role = role or next(iter(resolved.roles.keys())) if resolved.roles else "unknown"
        print_materialize_summary(resolved, dest, actual_role)
    
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
    working_dir: str = typer.Argument(".", help="Directory to scan"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider override for testing")
) -> None:
    """Scan working directory for bundle configuration."""
    
    def _scan() -> None:
        config = OpsConfig()
        content_provider = _create_provider(provider)
        
        # Create registry and repository based on provider type
        if provider == "fake":
            registry = _create_registry_store(provider)
            ops = Operations(config=config, provider=content_provider, registry=registry, repository="test/repo")
        else:
            ops = Operations(config=config, provider=content_provider)
        
        result = ops.scan(working_dir)
        print_stub_message("scan")
        typer.echo(result)
    
    run_and_exit(_scan)

@app.command()
def plan(
    working_dir: str = typer.Argument(".", help="Directory to analyze"),
    external_preview: bool = typer.Option(False, "--external-preview", help="Preview external storage decisions"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider override for testing")
) -> None:
    """Show storage plan for bundle creation."""
    
    def _plan() -> None:
        config = OpsConfig()
        content_provider = _create_provider(provider)
        
        # Create registry and repository based on provider type
        if provider == "fake":
            registry = _create_registry_store(provider)
            ops = Operations(config=config, provider=content_provider, registry=registry, repository="test/repo")
        else:
            ops = Operations(config=config, provider=content_provider)
        
        result = ops.plan(working_dir, external_preview=external_preview)
        print_stub_message("plan")
        typer.echo(result)
    
    run_and_exit(_plan)

@app.command()
def diff(
    ref_or_path: str = typer.Argument(..., help="Bundle reference or local path"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider override for testing")
) -> None:
    """Compare bundle or working directory."""
    
    def _diff() -> None:
        config = OpsConfig()
        content_provider = _create_provider(provider)
        
        # Create registry and repository based on provider type
        if provider == "fake":
            registry = _create_registry_store(provider)
            ops = Operations(config=config, provider=content_provider, registry=registry, repository="test/repo")
        else:
            ops = Operations(config=config, provider=content_provider)
        
        result = ops.diff(ref_or_path)
        print_stub_message("diff")
        typer.echo(result)
    
    run_and_exit(_diff)

@app.command()
def push(
    working_dir: str = typer.Argument(".", help="Directory containing bundle"),
    bump: Optional[str] = typer.Option(None, "--bump", help="Version bump strategy (patch, minor, major)"),
    provider: Optional[str] = typer.Option(None, "--provider", help="Provider override for testing")
) -> None:
    """Push bundle to registry."""
    
    def _push() -> None:
        config = OpsConfig()
        content_provider = _create_provider(provider)
        
        # Create registry and repository based on provider type
        if provider == "fake":
            registry = _create_registry_store(provider)
            ops = Operations(config=config, provider=content_provider, registry=registry, repository="test/repo")
        else:
            ops = Operations(config=config, provider=content_provider)
        
        result = ops.push(working_dir, bump=bump)
        print_stub_message("push")
        typer.echo(result)
    
    run_and_exit(_push)

def main() -> None:
    """CLI entry point."""
    app()

if __name__ == "__main__":
    main()