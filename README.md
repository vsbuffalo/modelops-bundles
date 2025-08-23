# ModelOps Bundles

## Overview

ModelOps Bundles provides a CLI and Python runtime for managing ML model artifacts using OCI registries and Azure Blob storage. It implements the ModelOps Bundle specification with full OCI distribution compatibility.

## Features

- **Real OCI Registry Integration**: Full ORAS-based registry support with manifest reading
- **Azure Blob Storage**: External storage for large model artifacts with configurable retry
- **CLI Commands**: 8 commands including resolve, materialize, export, and development stubs
- **Role-based Materialization**: Deploy different bundle subsets (runtime, training, inference)
- **Security**: Path validation, digest normalization, explicit dependency injection
- **Deterministic Export**: Reproducible archive creation with best-effort size calculation

## Installation

```bash
# Install with UV
uv add modelops-bundles

# Or with pip
pip install modelops-bundles
```

## Environment Variables

ModelOps Bundles requires the following environment variables for production use:

### Required Settings

- `AZURE_STORAGE_KEY` - Azure Storage account key for external blob access
- `REGISTRY_REPO` - OCI registry repository (e.g., "myorg/models")

### Optional Settings

- `AZURE_STORAGE_ACCOUNT` - Azure Storage account name (default: "modelops")
- `AZURE_STORAGE_CONTAINER` - Container name for external objects (default: "bundles")
- `EXTERNAL_TIMEOUT_S` - External storage timeout in seconds (default: 30)

## CLI Usage

### Basic Commands

```bash
# Resolve bundle identity
modelops-bundles resolve my-model:1.0.0

# Materialize bundle to filesystem
modelops-bundles materialize my-model:1.0.0 ./workspace

# Materialize specific role
modelops-bundles materialize my-model:1.0.0 ./runtime --role runtime

# Export directory to bundle archive
modelops-bundles export ./workspace ./my-bundle.tar.zst

# Verbose output
modelops-bundles resolve my-model:1.0.0 --verbose
```

### Bundle Reference Formats

- **Name:Version**: `my-model:1.0.0` (requires REGISTRY_REPO)
- **Digest**: `sha256:abc123...` (direct manifest access)
- **OCI Digest**: `@sha256:abc123...` (OCI format)

### Advanced Options

```bash
# Prefetch external data immediately
modelops-bundles materialize my-model:1.0.0 ./workspace --prefetch-external

# Overwrite existing files
modelops-bundles materialize my-model:1.0.0 ./workspace --overwrite

# Disable caching
modelops-bundles resolve my-model:1.0.0 --no-cache

# CI mode (suppress progress)
modelops-bundles materialize my-model:1.0.0 ./workspace --ci
```

## Python API

```python
from modelops_bundles.operations import Operations, OpsConfig
from modelops_bundles.providers.bundle_content import default_provider_from_env
from modelops_contracts.artifacts import BundleRef

# Create operations facade with default provider
config = OpsConfig(verbose=True)
provider = default_provider_from_env()
ops = Operations(config=config, provider=provider)

# Resolve bundle
ref = BundleRef(name="my-model", version="1.0.0")
resolved = ops.resolve(ref)

# Materialize to filesystem
result = ops.materialize(
    ref=ref, 
    dest="./workspace",
    role="training",
    prefetch_external=False
)

# Export workspace to archive
ops.export(
    workdir="./workspace",
    dest="./my-bundle.tar.zst"
)
```

## Bundle Structure

ModelOps Bundles organize ML artifacts into layers:

- **code**: Python/R code, notebooks
- **config**: Configuration files, parameters
- **data**: Training/test datasets (stored as external references)
- **models**: Serialized model files

## Roles

Bundles support role-based deployment:

- **runtime**: Code + config (for inference)
- **training**: Code + config + data (for retraining)
- **default**: Minimal deployment set

## External Storage

Large files are stored in Azure Blob Storage and referenced via pointer files:

```json
{
  "fulfilled": false,
  "original_path": "data/train.csv",
  "uri": "az://container/train.csv",
  "sha256": "abc123...",
  "size": 1048576,
  "tier": "cool"
}
```

## Security Features

- **Path Validation**: Prevents directory traversal attacks
- **Digest Normalization**: Converts uppercase hex to registry-compatible lowercase
- **No Silent Defaults**: Explicit repository requirements prevent accidental test deployments
- **Reserved Path Protection**: `.mops/` directory protected from bundle content

## Limitations

- Local path bundles not yet implemented
- Push operations are stubs (future implementation)
- Windows file locking may affect concurrent operations
- Best-effort size calculation (external entries only)

## Error Codes

- **1**: Bundle not found
- **2**: Validation error
- **3**: Download/runtime error
- **10**: Unsupported media type
- **11**: Role/layer mismatch
- **12**: Workspace conflict

## Development

### Testing

```bash
# Unit tests
uv run python -m pytest tests/ -k "not integration"

# Integration tests
uv run python -m pytest tests/integration/

# Feature tests
uv run python -m pytest tests/test_bundle_features.py

# CLI smoke tests
uv run python -m pytest tests/test_cli_smoke.py
```

### Testing

The package includes comprehensive test coverage with fake implementations for testing without external dependencies.

## Architecture

- **Runtime**: Core resolution and materialization logic
- **Operations**: High-level facade with configuration management
- **Storage**: ORAS and Azure Blob adapters
- **Providers**: Content delivery with external/registry coordination
- **CLI**: Typer-based command interface with production wiring

This implementation prioritizes security, determinism, and production readiness while maintaining compatibility with the ModelOps Bundle specification.
