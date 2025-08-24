# Summary: ORAS-py Library Analysis & Implementation Plan

## ORAS-py Capabilities

The ORAS Python library (v0.2.37) provides:

1. **High-Level Client API**:
   - `OrasClient()` for simple push/pull operations
   - Built-in authentication (basic, token, ECR)
   - File-based artifact management

2. **Low-Level Provider API**:
   - `oras.provider.Registry` class for custom implementations
   - Direct blob operations (upload_blob, download_blob, blob_exists)
   - Manifest operations (get_manifest, upload_manifest)
   - Tag management (get_tags, delete_tag)

3. **OCI Utilities**:
   - `oras.oci.NewManifest()` for creating manifests
   - Layer management and annotations
   - Support for custom media types

## Our Current Implementation

We have:
1. **RegistryHTTP**: Custom HTTP client with Docker auth flow
   - `resolve_tag()`, `get_blob_json()`, `get_blob_bytes()`, `head_manifest()`
   - Bearer token authentication
   - Retry logic with tenacity

2. **HybridOciRegistry**: Hybrid approach (HTTP + SDK placeholder)
   - Uses RegistryHTTP for manifests
   - Placeholder for SDK blob operations
   - Missing: `put_blob()`, `put_manifest()`, `ensure_blob()`

3. **Push functionality**: Partially implemented
   - Creates layer indexes and manifests
   - Missing actual upload operations

## What We Could Replace with ORAS-py

✅ **Good candidates for replacement**:
1. **Blob operations** (download/upload/exists)
   - ORAS handles streaming, chunking, retries
   - Cleaner than raw HTTP implementation

2. **Authentication flow**
   - ORAS handles Docker config and token refresh
   - Supports multiple auth backends

3. **Basic push/pull**
   - For simple file artifacts without complex requirements

❌ **Keep our custom implementation for**:
1. **Manifest operations with custom media types**
   - We need precise control over `application/vnd.modelops.bundle.manifest+json`
   - We need canonical digest validation
   - HEAD manifest for digest resolution

2. **Bundle-specific logic**
   - Layer index creation with our schema
   - Role-based layer selection
   - External storage references

## Implementation Plan

### Phase 1: Add ORAS-py for Blob Operations
Replace placeholder blob operations in HybridOciRegistry:

```python
# In HybridOciRegistry
from oras.provider import Registry as OrasRegistry

def _create_oras_client(self, settings):
    client = OrasRegistry(
        hostname=settings.registry_url,
        insecure=settings.registry_insecure,
        auth_backend="token"
    )
    if settings.registry_user:
        client.login(
            username=settings.registry_user,
            password=settings.registry_pass
        )
    return client

def put_blob(self, repo: str, digest: str, data: bytes):
    # Use ORAS for actual upload
    layer = {"digest": digest, "data": data}
    container = self._parse_container(repo)
    self._oras.upload_blob(layer, container)

def get_blob(self, repo: str, digest: str) -> bytes:
    # Use ORAS for download
    container = self._parse_container(repo)
    return self._oras.get_blob(digest, container)
```

### Phase 2: Implement Push with Hybrid Approach
Complete the push implementation:

```python
def push_bundle():
    # 1. Use ORAS for blob uploads (layer files)
    for file in layer_files:
        oras_client.upload_blob(file_content, container)
    
    # 2. Use our HTTP client for manifest push (custom media types)
    manifest_digest = http_client.put_manifest(
        media_type="application/vnd.modelops.bundle.manifest+json",
        payload=manifest_bytes,
        tag=tag
    )
```

### Phase 3: Keep Custom HTTP for Critical Operations
Maintain our RegistryHTTP for:
- HEAD requests for digest resolution
- Custom media type negotiation
- Bundle manifest operations

## Benefits of This Approach

1. **Clean separation**: ORAS for standard OCI operations, custom for ModelOps-specific
2. **Less code to maintain**: Remove blob upload/download implementations
3. **Better reliability**: ORAS handles retries, streaming, auth refresh
4. **Future-proof**: Can adopt more ORAS features as they mature

## What Remains Custom

1. **Bundle manifest schema** (application/vnd.modelops.bundle.manifest+json)
2. **Layer index format** (application/vnd.modelops.layer+json)
3. **Digest resolution logic** (HEAD manifest for canonical digests)
4. **Role-based materialization**
5. **External storage integration**

## Recommendation

Adopt ORAS-py for blob operations while keeping custom implementations for ModelOps-specific manifest handling. This gives us:
- Cleaner code for standard operations
- Full control over custom media types and schemas
- Reduced maintenance burden
- Better error handling and retries