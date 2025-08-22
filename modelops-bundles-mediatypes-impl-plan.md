
# ModelOps Bundles — **Resolve/Materialize** Spec & Media Types Rationale

**Date:** 2025‑08‑20  
**Scope:** `modelops-bundles`, `modelops`, `modelops-contracts`  
**Audience:** Infra, runtime, and tooling engineers

---

## 0) Executive Summary

We are rebuilding the **bundle** subsystem to cleanly package, publish, and
consume simulation **workspaces** (code + config + small data + pointers to big
data). The runtime surface is minimal and explicit:

```python
from modelops_contracts.artifacts import BundleRef, ResolvedBundle
from modelops_bundles.runtime_types import ContentProvider

def resolve(ref: BundleRef, *, cache: bool = True) -> ResolvedBundle: ...
def materialize(ref: BundleRef, dest: str, *, role: str | None = None, overwrite: bool = False, prefetch_external: bool = False, provider: ContentProvider) -> ResolvedBundle: ...
```

- **`resolve`**: turn a logical reference (`bundle:version`, digest, or local path) into a reproducible object with manifest + content addresses (no filesystem side‑effects beyond optional cache priming).
- **`materialize`**: _mirror_ exactly the layers required for a **role** into a target directory (workdir), with optional **lazy** external data. Uses dependency injection via ContentProvider to stay pure and testable. This is the seam the rest of ModelOps uses, both locally and in-cluster.

Why this shape?
- Aligns with the **contracts-first** architecture (`modelops-contracts` defines the types).
- Clean separation between **artifact identity** and **workspace realization**.
- Plays nicely with **Dask/K8s** workers: they call `materialize(..., role="sim")` in `init` and then `cd $WORKDIR` to run tasks.
- Keeps Calabaria science code blissfully unaware of registries and blob stores.

**Bundle Identity** refers to the essential metadata that uniquely identifies a bundle, consisting of:
- **name**: The bundle's identifier
- **version**: The specific version/release
- **digest**: Content hash (e.g., sha256:abcd...)
- **provenance**: Origin/source information

Example: `epi-sir/1.3.2@sha256:abcd`

Importantly, bundle identity is **content-addressable** — it's determined purely by the bundle's metadata and contents, not by its physical location or directory structure on disk. This identity system ensures we can identify a *specific* bundle with certainty, providing reliable provenance tracking when running models.

---

## 1) Media Types: Why They Matter and What We Use

Media types (MIME) in OCI/ORAS tell registries, scanners, admissions, and tooling **what** a blob or JSON is. We make them explicit and versionable.

```python
MediaType = Literal[
  "application/vnd.modelops.bundle.manifest+json",
  "application/vnd.modelops.layer+json",
  "application/vnd.modelops.external-ref+json",
  "application/vnd.oci.image.manifest.v1+json",
]
```

### 1.1 Benefits

- **Interoperability**: Any OCI-compliant registry or proxy can host our artifacts.
- **Policy/Security**: Cluster policy can allowlist/blocklist types. Validation can flag unknown types.
- **Extensibility**: We can introduce `+v2+json` later without breaking old clients.
- **Clarity**: Humans and machines understand whether a JSON is a top-level bundle, a per-layer index, or an external pointer.

### 1.2 Our Types

| Media type | Payload | Why it exists |
|---|---|---|
| `application/vnd.modelops.bundle.manifest+json` | Top-level bundle manifest (name, version, models, roles, list of layers with content-addresses) | Single source of truth; content-addressable; signed |
| `application/vnd.modelops.layer+json` | Optional per-layer index (list of internal blobs and external refs) | Enables layer-scoped pulls and fine-grained diffs |
| `application/vnd.modelops.external-ref+json` | Pointer records for big data (`az://…`, `s3://…`, `gs://…`) with size and checksums | Keeps the registry small; decouples cold data lifecycle |
| `application/vnd.oci.image.manifest.v1+json` | The wrapper OCI manifest that stitches our JSON into a standards-compliant artifact | Lets ORAS/Docker tooling move our content |

> Engineering note: We **do not** embed big data in OCI layers. We store **external pointers** with checksums and optional storage class (hot/cool/archive). This keeps pushes fast and registries lean, yet preserves integrity.

---

## 2) Contracts (in `modelops-contracts`)

Everything starts in **contracts** to keep clients and servers decoupled.

```python
# modelops_contracts/artifacts.py
@dataclass(frozen=True)
class BundleRef:
    # One of:
    name: str | None                 # "projects/epi-flu-synthetic"
    version: str | None              # "2.3.1" or "latest"
    digest: str | None               # "sha256:…"
    local_path: str | None           # "/path/to/checkedout/bundle"
    role: str | None = None          # default role resolution hint

@dataclass(frozen=True)
class ResolvedBundle:
    ref: BundleRef
    manifest_digest: str             # content address for determinism
    media_type: str                  # modelops bundle manifest type
    roles: dict[str, list[str]]      # role → layer names
    layers: list[str]                # all layer IDs
    external_index_present: bool     # contains external refs
    cache_dir: str | None = None     # where ORAS blobs are primed (if any)
```

**Why in `artifacts` not `bundles`?**  
This sits next to other artifact contracts (e.g., logs, model checkpoints). It communicates “this is a general artifact reference/realization contract”, not a monolith submodule. Clients only import types; implementations live in `modelops-bundles`.

---

## 3) Planner Phases and Responsibilities

We retain a simple **scan → plan → publish** pipeline to keep the push path deterministic and auditable.

### 3.1 `scan()` — deterministic inventory
- Expand glob patterns from `.mops-bundle.yaml` per **layer**.
- Compute SHA‑256, file modes, normalized relative paths.
- Exclude ignores per policy.
- Output: list of `BlobInfo` and candidate `ExternalBlob` (by pattern only; no uploads).

### 3.2 `plan()` — storage decisions
- Apply **HybridStorage** policy: pattern overrides first, then size thresholds.
- Decide **ORAS** vs **external** for each artifact.
- Compute content-based **layer_id** (hash of file path + SHA sorted).
- Output: `Layer` records ready to publish + a draft `BundleManifest`.

### 3.3 `publish()` — push without surprises
- **Atomic publish workflow**:
  1. Upload missing ORAS blobs (content-addressed) 
  2. Upload/verify external objects (if configured as pre-staged)
  3. Upload manifest layers (`bundle.manifest+json`, `layer+json` entries)
  4. Upload top-level OCI manifest (**only after all blobs succeed**)
  5. Tag with `version` and optionally `latest` (**only after manifest succeeds**)
  6. Sign if enabled (final step)
- **Failure safety**: If any step fails, no tags are created (partial state not visible to clients)
- Output: OCI descriptor with `digest` (returned as the canonical identity).

> Why split scan/plan/publish?  
> - **Repeatability**: The same inputs produce the same plan.  
> - **Policy**: Plan is a stable contract to review, diff, or sign.  
> - **Extensibility**: Publish logic may vary by registry/provider, plan remains fixed.

---

## 4) Runtime: `resolve()` and `materialize()`

### 4.1 `resolve(ref, cache=True) -> ResolvedBundle`

**What resolve() does:**
- Accepts `BundleRef` with one of: `(name, version)`, `digest`, or `local_path`
- Makes network calls to registry to resolve manifests/indices
- Computes and returns `ResolvedBundle` (content addresses, roles, layer list, sizes, booleans)
- Optionally prepares cache directory structure under `~/.modelops/bundles/<manifest-digest>/` (directory creation only)

**What resolve() must NOT do:**
- ❌ Create or modify files under any destination workdir (no `dest` parameter)
- ❌ Download ORAS blobs or external data
- ❌ Create pointer files
- ❌ Write content to global caches (beyond minimal directory structure)

**Cache behavior:**
- `resolve(..., cache=True)` may create cache directory structure: `mkdir -p ~/.modelops/bundles/<manifest-digest>/`
- No blob downloads or content writes - cache directory remains empty until `materialize()`
- This minimal preparation enables faster `materialize()` calls later

**Why have `resolve()` separate from `materialize()`?**  

It decouples **identity** (planning, audit, policy) from **filesystem side-effects**. The rest of ModelOps can attach metadata (e.g., provenance in `EvalRequest.provenance`) using only the digest from `resolve()`. Since `resolve()` has no side-effects beyond optional directory creation, it's safe to call repeatedly in schedulers/heads for planning.

### 4.2 `materialize(ref, dest, role=None, overwrite=False, prefetch_external=False, provider=ContentProvider) -> ResolvedBundle`

**Role Selection Process:**
Role is selected using this precedence (highest → lowest priority):
1. **Function argument** `role=...` (if provided)
2. **BundleRef hint** `ref.role` (if set)  
3. **Manifest default** (if role named "default" exists in manifest)
4. **Error** if no role can be determined

**Validation:**
- Selected role must exist in `ResolvedBundle.roles`
- If role doesn't exist → raise `RoleLayerMismatch` (exit code 11)
- If role references non-existent layers → raise `RoleLayerMismatch` during operation
- If both function arg and `ref.role` are present, function arg wins silently (no warning)

**Content Materialization:**
- Pull **only** the layers needed for the selected `role` via the provided ContentProvider
- For ORAS content: write files under `dest/…`. If a **node cache** exists, use hardlinks/symlinks to avoid copying
- For external blobs: behavior is **lazy-by-default**. Write tiny pointer files (JSON) with `{uri, sha256, size, tier}` from provider metadata. Calabaria's data loader can call a helper to fetch on demand. Optional `prefetch_external=True` downloads immediately **to the workdir** (not the cache) via `provider.fetch_external()`
- Populate `.mops-manifest.json` in `dest` for provenance
- Return the same `ResolvedBundle` (same identity) so callers can pass it forward without re-resolving

**Why return the same type for both calls?**  
It keeps call sites simple and encourages **digest-first** logic. The difference is semantic: `materialize` has performed local side effects, `resolve` has not. `ResolvedBundle.cache_dir` vs presence of files in `dest` is the observable difference when needed.

### 4.3 BundleRef Resolution Rules

**Resolution Precedence (mutually exclusive):**
1. **`local_path`** (if provided) → treat as immutable working tree
   - Ignore `name`, `version`, `digest`
   - Path can be absolute or relative
   - Must contain `.mops-bundle.yaml`

2. **`digest`** (if provided) → exact artifact lookup
   - Ignore `name`, `version` 
   - Must be `sha256:` prefixed
   - Immutable content-addressable reference

3. **`name` + `version`** → registry lookup
   - `version` may be "latest"
   - Both required if neither `local_path` nor `digest` provided

**Role Selection Precedence:**
1. **Function argument** `role` parameter (highest priority)
2. **`ref.role`** field in BundleRef
3. **Manifest default role** from bundle manifest
4. **Error** if none specified

**Name Normalization:**
- Allowed chars: `a-z`, `0-9`, `-`, `/` (for namespacing)
- Case: lowercase only
- Namespacing: `org/project/bundle` supported
- Examples: `epi-sir`, `calabria/models/abm`, `data-team/synthetic-flu`

**Validation Rules:**
```python
def validate_bundle_ref(ref: BundleRef) -> BundleRef:
    # Mutual exclusivity check
    provided_fields = [ref.local_path, ref.digest, (ref.name and ref.version)]
    if sum(bool(field) for field in provided_fields) != 1:
        raise ValueError("Must provide exactly one of: local_path, digest, or name+version")
    
    # Digest format validation
    if ref.digest and not ref.digest.startswith("sha256:"):
        raise ValueError("Digest must start with 'sha256:'")
    
    # Name validation
    if ref.name and not re.match(r"^[a-z0-9-/]+$", ref.name):
        raise ValueError("Name must contain only lowercase letters, numbers, hyphens, and slashes")
    
    return ref
```

### 4.4 Portability: Same API Everywhere

**Can `resolve()` and `materialize()` be called on both workstations and cloud workers?**

Yes — that's the design goal. The same API works in both contexts:

**Workstation:**
- `resolve()` hits the local cache first (`~/.modelops/bundles/`), then the registry/object store
- `materialize()` puts just the needed layers into a local working directory (e.g., for dry-runs or quick tests)
- Cache persists across runs in the user's home directory

**Cloud Worker (e.g., K8s pod):**
- Identical call path with the same API
- **`materialize()` is the primary mechanism for workers to bring in the exact data needed for their role** (e.g., just "sim" layers, not "docs")
- Cache strategy is still being refined — may use pod ephemeral storage, node-level cache, or no cache initially
- If no cache is primed, fetches from the same registries/URIs
- Yields the same filesystem layout for the selected role

**Deterministic Outcome:** Given the same `BundleRef` (identity) and role, both environments end up with byte-identical content in the task's sandbox. This consistency makes debugging and reproduction seamless across development and production.

```python
# Same code runs everywhere:
ref = BundleRef(name="epi-abm", version="2.2.1", role="sim")
rb = resolve(ref)                                  # Works on laptop or in K8s
rb = materialize(ref, dest="/workspace", role="sim")  # Cloud workers use this to fetch only what they need
```

### 4.5 Pull Semantics: Role-Aware with Prefetch Options

The CLI `pull` command respects the same role-based materialization as the runtime API:

**Default behavior:**
```bash
modelops bundles pull epi-abm:latest --role sim --dest ./work
```
- Fetches only layers needed for the "sim" role
- External blobs written as JSON pointer files (lazy loading)
- Equivalent to calling `materialize()` from the CLI

**With prefetch flag:**
```bash
modelops bundles pull epi-abm:latest --role sim --dest ./work --prefetch-external
```
- Same role-based layer selection
- **Additionally** downloads all external data immediately to destination
- Use when you know you'll need the external data and want to avoid on-demand fetching

**Why separate options?**
- Most cloud workers don't need external data immediately
- Lazy loading saves time and bandwidth for data that may never be accessed
- Prefetch useful for offline work or when external storage has high latency

### 4.6 Pointer File Layout

When `materialize()` encounters external blobs, it writes **pointer files**
instead of downloading the actual data (unless `prefetch_external=True`).

**Directory Structure:**
```
dest/
├── .mops/
│   └── ptr/
│       ├── fitdata/
│       │   ├── 2022-flu-data.parquet.json
│       │   └── synthetic/
│       │       └── baseline.csv.json
│       └── docs/
│           └── README.md.json
├── src/
│   └── model.py              # ORAS content (actual files)
└── configs/
    └── base.yaml             # ORAS content (actual files)
```

**Pointer File Schema:**
```json
{
  "schema_version": 1,
  "uri": "az://epidata/fit/2022-flu-data.parquet",
  "sha256": "a1b2c3d4e5f6...",
  "size": 2247583616,
  "tier": "cool",
  "created_at": "2025-01-15T10:00:00Z",
  "fulfilled": false,
  "local_path": null,
  "original_path": "data/fit/2022-flu-data.parquet",
  "layer": "fitdata"
}
```

**Schema Field Descriptions:**
- `schema_version`: Integer version of the pointer schema (currently 1)
- `uri`: Full external storage URI where the data is stored
- `sha256`: Hex-encoded SHA-256 hash of the file content (for integrity verification)
- `size`: File size in bytes
- `tier`: Optional storage tier hint ("hot", "cool", "archive")
- `created_at`: ISO 8601 timestamp when the pointer was created
- `fulfilled`: Boolean indicating if the data has been downloaded locally
- `local_path`: Relative path to local copy (when fulfilled=true), otherwise null
- `original_path`: Original file path within the bundle
- `layer`: Layer name this file belongs to

**Atomic Write Requirements:**
- Write to `.tmp` file first: `2022-flu-data.parquet.json.tmp`
- Validate JSON schema
- Atomic rename: `rename(.tmp, .json)`
- Prevents partial pointer files if process is killed

**Usage by Data Loaders:**
```python
# Calabria data loader can check for pointer files
def load_data(path: str) -> pd.DataFrame:
    pointer_path = f".mops/ptr/{path}.json"
    if os.path.exists(pointer_path):
        # Load from external storage on-demand
        with open(pointer_path) as f:
            pointer = json.load(f)
        return fetch_from_uri(pointer["uri"], verify_hash=pointer["sha256"])
    else:
        # Regular file
        return pd.read_parquet(path)
```

The pointer placement rule (locked): Always under
dest/.mops/ptr/<original_dir>/<filename>.json (never a "sidecar" file, e.g.
data/foo.bin.json, alongside the actual data file). Why? 

 - Keeps working trees clean (no extra artifacts mixed with user data).

 - Makes it trivial for loaders to find pointers (one fixed root).

 - Avoids collisions: the pointer filename is the original filename with `.json` 
   appended, scoped under `.mops/ptr/....`


### 4.7 Layers vs Roles: Complementary Concepts

**Layers and roles are not the same thing** — they serve different purposes:

**Layers = Physical grouping of files**
- Think "how we store and transport bytes"
- Example layers: `code`, `config`, `simdata`, `docs`
- Layers own blobs (ORAS or external object store refs)
- Layers are what dedupe, hash, push/pull, and cache operate on

**Roles = Logical view (subset) of layers**
- Think "what this task actually needs"
- A role selects one or more layers by name
- One layer can appear in multiple roles; roles can overlap
- Not one-to-one, and not hierarchical — just a declared mapping

Example roles:
- `runtime`: ["code", "config"] — just what's needed to run
- `training`: ["code", "config", "simdata"] — includes training data
- `docs-only`: ["docs"] — documentation only

In `.mops-bundle.yaml`:
```yaml
roles:
  runtime:   ["code", "config"]
  training:  ["code", "config", "simdata"]
  docs-only: ["docs"]
```

At runtime:
```python
rb = resolve(BundleRef("epi-sir", "1.3.2"))
mat = materialize(rb, dest="/work/sim", role="runtime")
# Only blobs from layers "code" + "config" are fetched; big "simdata" is skipped
```

**Why this separation?**
- Layers provide efficient storage and transport
- Roles provide task-appropriate materialization
- Workers pull only what they need, keeping pods lean and startup fast

### 4.8 Role Selection Algorithm

**Goal:** Determine which role's layers to materialize when multiple signals exist.

**Selection Precedence (highest → lowest priority):**
1. **Function argument** `role=...`
2. **BundleRef hint** `ref.role`
3. **Manifest default** (if role named "default" exists)
4. **Error** (if nothing resolves to a role)

**Implementation Algorithm:**
```python
def _select_role(resolved: ResolvedBundle, ref: BundleRef, role_arg: str | None) -> str:
    if role_arg:
        return _validate_role(resolved, role_arg)
    if ref.role:
        return _validate_role(resolved, ref.role)
    if "default" in resolved.roles:
        return "default"
    available = ", ".join(sorted(resolved.roles.keys()))
    raise RoleLayerMismatch(
        f"No role specified and no default role in manifest. Available: {available}"
    )

def _validate_role(resolved: ResolvedBundle, role: str) -> str:
    if role not in resolved.roles:
        available = ", ".join(sorted(resolved.roles.keys()))
        raise RoleLayerMismatch(
            f"Role '{role}' not found in bundle. Available: {available}"
        )
    return role
```

**Edge Cases:**
- If a valid role references non-existent layers (spec violation) → raise `RoleLayerMismatch` during plan/materialize
- If both function arg and `ref.role` are present and different → function arg wins silently (no warning)
- Empty role name or role with empty layer list → validation error during bundle construction

**Error Messages:**
```
Role 'training' not found in bundle. Available: runtime, docs-only
No role specified and no default role in manifest. Available: runtime, training, docs-only
Role 'sim' references non-existent layers: ['missing-layer']
```

## 4.9) ContentProvider Abstraction

### Purpose

The `materialize()` function's job is **filesystem side-effects only**: pick a role, enforce overwrite semantics, write files atomically, and create pointer files under `.mops/ptr/**`. It must not know how to talk to an OCI/ORAS registry, Azure/S3, or a local working tree.

ContentProvider owns content enumeration and retrieval for a set of layers:
- For ORAS items, it yields the final relative path and the file bytes
- For external items, it yields the final relative path plus the pointer metadata (uri, sha256, size, tier). Optionally, it can fetch the external bytes if prefetch is requested

This keeps runtime pure and deterministic while letting storage strategies evolve.

### Architecture Overview

Here's how the components relate in our layered, protocol-based design:

```
┌─────────────┐
│  Runtime    │ (materialize function)
└─────┬───────┘
      │ uses
      ▼
┌─────────────────────┐
│ ContentProvider     │ (Protocol/Interface)
│ - iter_entries()    │
│ - fetch_external()  │
└─────────────────────┘
      ▲
      │ implements
      │
┌─────────────────────────┐
│ OrasExternalProvider    │ (Hybrid ORAS + External)
│ - Uses storage backends │
└───────┬─────────┬───────┘
        │         │
     uses      uses
        │         │
        ▼         ▼
┌──────────┐  ┌──────────────┐
│OrasStore │  │ExternalStore │ (Storage Protocols)
└──────────┘  └──────────────┘
     ▲              ▲
     │              │
implements     implements
     │              │
┌──────────┐  ┌──────────────┐
│FakeOras  │  │FakeExternal  │ (Test Implementations)
│Store     │  │Store         │
└──────────┘  └──────────────┘
```

### Why "OrasExternalProvider"?

The name reflects its hybrid storage strategy:
- **Oras** - Handles ORAS registry content (small files: code, configs, manifests)
- **External** - Handles external blob storage (large files: data, models, artifacts)

This hybrid approach keeps registries lean while maintaining content integrity. Small files go through the registry for versioning and signing, while large files use cost-effective blob storage with pointer files for discovery.

### Component Roles

- **Runtime**: Pure business logic, only knows ContentProvider protocol
- **ContentProvider**: Protocol defining the runtime's needs (iter_entries, fetch_external)
- **OrasExternalProvider**: Production implementation orchestrating both storage types
- **OrasStore/ExternalStore**: Storage protocols abstracting registry/blob operations
- **Fakes**: In-memory test implementations for deterministic testing without network calls

### Responsibility Split

| Concern                                                   | Runtime (`materialize`) | ContentProvider |
|-----------------------------------------------------------|-------------------------|-----------------|
| Role selection precedence (arg > ref.role > default > err)| ✅                       | —               |
| Validate role exists / layers present                     | ✅                       | —               |
| Idempotency & conflict detection (CREATED/UNCHANGED/…)    | ✅                       | —               |
| Atomic writes, fsync, `os.replace`                        | ✅                       | —               |
| Pointer placement under `.mops/ptr/**`                    | ✅                       | —               |
| Enumerate entries for layers (paths, kinds)               | —                       | ✅ (`iter_entries`) |
| Provide ORAS file bytes                                   | —                       | ✅               |
| Provide external metadata (uri/sha256/size/tier)          | —                       | ✅               |
| Prefetch external bytes                                   | —                       | ✅ (`fetch_external`) |
| Auth, retries, backoff, parallelism                       | —                       | ✅ (impl detail) |
| Caching (Stage 2+)                                        | —                       | ✅ (impl detail) |

### MatEntry: The Bridge Type

ContentProvider yields `MatEntry` objects that carry all necessary information for materialization:

```python
@dataclass(frozen=True, slots=True)
class MatEntry:
    path: str          # "src/model.py" (POSIX-style relative path)
    layer: str         # "code", "data", etc.
    kind: Kind         # "oras" | "external"
    content: bytes | None  # bytes for oras; None for external
    # External-only metadata (required when kind=="external")
    uri: str | None = None        # "az://container/path"
    sha256: str | None = None     # 64-hex, no 'sha256:' prefix
    size: int | None = None       # Size in bytes
    tier: str | None = None       # "hot" | "cool" | "archive"
```

### ContentProvider Protocol

```python
class ContentProvider(Protocol):
    def iter_entries(
        self, 
        resolved: ResolvedBundle, 
        layers: list[str]
    ) -> Iterable[MatEntry]:
        """Enumerate all entries for the requested layers."""
        ...
    
    def fetch_external(self, entry: MatEntry) -> bytes:
        """Fetch external content when prefetch_external=True."""
        ...
```

### Layer Index Format

Each layer has an associated index manifest stored in ORAS with mediaType `LAYER_INDEX`. The OrasExternalProvider reads these indexes to enumerate files for materialization.

**Example layer index for mixed ORAS + external content:**

```json
{
  "mediaType": "application/vnd.modelops.layer+json",
  "entries": [
    {
      "path": "src/model.py",
      "digest": "sha256:abc123def456...",
      "layer": "code"
    },
    {
      "path": "configs/base.yaml", 
      "digest": "sha256:789abc012def...",
      "layer": "code"
    },
    {
      "path": "data/train.csv",
      "external": {
        "uri": "az://epidata/training/train.csv",
        "sha256": "def456abc789012...",
        "size": 2247583616,
        "tier": "cool"
      },
      "layer": "data"
    },
    {
      "path": "data/test.csv",
      "external": {
        "uri": "az://epidata/training/test.csv", 
        "sha256": "012def456abc789...",
        "size": 1048576
      },
      "layer": "data"
    }
  ]
}
```

**Layer Index Rules:**
- Each entry must have `path` and `layer` fields
- Exactly one of `digest` (for ORAS content) or `external` (for external storage)
- External entries require `uri`, `sha256`, and `size`; `tier` is optional
- The `layer` field must match the layer being indexed (validated by provider)
- Paths use POSIX forward slashes and are relative to bundle root

**ResolvedBundle.layer_indexes:**
The `ResolvedBundle` type now includes a `layer_indexes: Dict[str, str]` field mapping layer names to the digest of their index manifest. This enables the provider to find and parse the correct index for each requested layer.

### Design Benefits

As shown in the architecture diagram above, this layered design provides:

1. **Runtime Purity**: `materialize()` contains zero registry/storage-specific code
2. **Testability**: Tests inject `FakeProvider` with deterministic content
3. **Evolution**: New storage backends just implement ContentProvider
4. **Separation**: Storage complexity is isolated from filesystem operations

### Implementation Notes

- **ContentProvider is a runtime extension point, not a contract type**. Tests must inject a fake provider; production uses an ORAS+External provider.
- **Storage layer**: ContentProviders use the Storage Abstractions (Section 4.10) to interact with ORAS registries and external storage systems.
- **Deterministic behavior**: Runtime sorts entries by path and detects duplicates
- **Atomic operations**: All file writes use temp file + `os.replace()` pattern
- **Provider responsibilities**: Auth, retries, caching are implementation details hidden from runtime

---

### 4.9.1 CLI ↔ Runtime Mapping (normative)

The modelops bundles CLI is a thin wrapper over the runtime:
• **resolve** → `resolve(BundleRef, cache=...)`
• **materialize** → `materialize(BundleRef, dest=…, role=…, overwrite=…, prefetch_external=…, provider=…)`
• **export** (see §21) reads a materialized tree and produces a deterministic archive. export never calls providers or fetches bytes; it operates solely on the local filesystem state.

All CLI features MUST preserve the same semantics and exit codes defined in §9.

---

## 4.10) Storage Abstractions (runtime–provider seam)

To keep the runtime pure and testable, storage concerns are factored behind two
minimal interfaces. Providers use these interfaces; the runtime never speaks to
SDKs directly.

### Interfaces 
```python
# modelops_bundles/storage/base.py
from dataclasses import dataclass
from typing import Protocol, Optional

@dataclass(frozen=True)
class ExternalStat:
    uri: str           # e.g., "az://bucket/path/file.parquet"
    size: int          # bytes
    sha256: str        # 64 hex (no 'sha256:' prefix)
    tier: Optional[str] = None  # "hot" | "cool" | "archive" (hint only)

class OrasStore(Protocol):
    def blob_exists(self, digest: str) -> bool: ...
    def get_blob(self, digest: str) -> bytes: ...
    def put_blob(self, digest: str, data: bytes) -> None: ...
    def get_manifest(self, digest_or_ref: str) -> bytes: ...
    def put_manifest(self, media_type: str, payload: bytes) -> str: ...
    # Returns digest of stored manifest

class ExternalStore(Protocol):
    def stat(self, uri: str) -> ExternalStat: ...
    def get(self, uri: str) -> bytes: ...
    def put(self, uri: str, data: bytes, *, sha256: Optional[str]=None) -> ExternalStat: ...
```

### Design notes
- ExternalStat exists so the provider can create pointer files without downloading bytes.
- tier is persisted in pointer JSON as a hint only; it does not affect runtime behavior. The runtime never branches on tier value.
- Interfaces are intentionally small; retries/auth/parallelism are implementation details.

### ExternalStat Contract
- **sha256**: Must be exactly 64 lowercase hex characters (no 'sha256:' prefix)
- **size**: Must be exact byte count (no approximations allowed)
- **Failure semantics**: If ExternalStore.stat() fails, it must raise an exception (never return None)

### ExternalStore.put() Contract
- **SHA256 validation**: If caller provides sha256 parameter that disagrees with computed hash of data, must raise ValueError
- **Return value**: Always returns ExternalStat with computed hash, actual size, and provided tier

### Provider responsibilities vs runtime 

| Concern | Runtime | Provider |
|---------|---------|----------|
| Storage protocols (ORAS, Azure, S3) | ❌ Never | ✅ Via OrasStore/ExternalStore |
| Authentication/credentials | ❌ Never | ✅ Implementation detail |
| Network retries/timeouts | ❌ Never | ✅ Implementation detail |
| Content enumeration | ❌ Never | ✅ Via ContentProvider.iter_entries() |
| Filesystem operations | ✅ Atomic writes | ❌ Never |
| Pointer file placement | ✅ Canonical rules | ❌ Never |

### Canonical fakes (for tests and examples)

To enable fast, deterministic tests and examples, we provide in-memory fakes next to the interfaces:
```
modelops_bundles/storage/
  base.py          # protocols & ExternalStat
  fakes/
    fake_oras.py
    fake_external.py
```

**Why co-locate fakes with protocols?**

- **Drift control**: When someone edits the protocols ExternalStore/OrasStore, the fake breaks in the same module tree—forcing updates and keeping the seam honest.
- **Reusability**: Other packages (CLI, runners) can import the fakes for their unit tests without re-inventing fixtures.
- **Type-safety**: The fake imports the exact protocol types (no circular test-import hacks).
- **Deterministic tests**: In-memory storage lets us test runtime/provider logic with zero external deps.

**Packaging concerns addressed:**
- They are not part of the production surface (excluded from distribution).
- Rationale: keep doubles co-located with the seam so interface changes break the fakes in CI, preventing drift.
- Other repos (CLI, runners) can import the fakes in their tests for end-to-end exercises without real SDKs.

**Drift control requirement:**
All in-tree fakes explicitly subclass their Protocols, so any interface change breaks CI immediately and prevents silent drift.

**Packaging guidance:** exclude `storage/fakes/**` from release wheels via:
```toml
# pyproject.toml
[tool.setuptools.packages.find]
exclude = ["modelops_bundles.storage.fakes*"]
```
Import only in test code.

### Interaction with roles/layers
- Roles select a set of layer names from the manifest.
- The provider enumerates entries for those layers:
  - ORAS entries provide bytes.
  - External entries are populated via ExternalStore.stat() with uri/sha256/size (and optional tier).
- Runtime writes pointer JSON using that metadata; if prefetch_external=True, it calls provider get() to fetch bytes.

### Pointer schema invariants (recap)
- Pointer files are placed at: `dest/.mops/ptr/<original_dir>/<filename>.json`
- Required fields: uri, sha256, size, original_path, layer
- Optional: tier
- Behavioral contract: tier is recorded only; no behavioral changes implied.

---

## 4.11) Operations Facade (Application Service Layer)

### Purpose

To keep CLI commands thin and testable, introduce an **Operations facade** between Typer and the runtime APIs. This facade centralizes command orchestration, error mapping, and output formatting while maintaining clean separation of concerns.

### Responsibilities

**What the facade owns:**
• **Command use-cases**: One method per CLI verb (`resolve`, `materialize`, `pull`, `push`, `scan`, `plan`, `diff`, `export`)
• **Input validation & normalization**: CLI args → contract DTOs  
• **Error mapping**: Exceptions → standardized exit codes (§9)
• **Output shaping**: Human text or JSON formatting (when enabled)
• **Policy centralization**: Determinism toggles, logging boundaries, configuration

**What it must NOT own:**
• Registry/blob logic (delegated to providers/stores)
• Filesystem primitives beyond orchestrating runtime/export calls  
• Global mutable state (config passed explicitly)

### Interface Design

```python
@dataclass(frozen=True)
class OpsConfig:
    cache_dir: Optional[str] = None
    ci: bool = False  
    human: bool = True
    zstd_level: Optional[int] = None  # pinned for determinism

class Operations:
    def __init__(self, config: OpsConfig, provider: Optional[ContentProvider] = None):
        self.cfg = config
        self.provider = provider

    def resolve(self, ref: BundleRef, *, cache: bool = True) -> ResolvedBundle:
        return runtime.resolve(ref, cache=cache)
        
    def materialize(self, ref: BundleRef, dest: str, *,
                    role: Optional[str], overwrite: bool, 
                    prefetch_external: bool) -> ResolvedBundle:
        assert self.provider, "provider required for materialize"
        return runtime.materialize(ref, dest=dest, role=role,
                                   overwrite=overwrite,
                                   prefetch_external=prefetch_external,
                                   provider=self.provider)

    def export(self, src_dir: str, out_path: str, *,
               include_external: bool = False) -> None:
        export.write_deterministic_archive(src_dir, out_path,
                                           include_external=include_external,
                                           zstd_level=self.cfg.zstd_level)
```

### CLI Integration

Typer commands stay thin (5-15 lines each):

```python
@app.command()
def materialize(name: str = None, dest: str = ".", role: str = None):
    ref = BundleRef(name=name)
    try:
        ops().materialize(ref, dest=dest, role=role, overwrite=False, prefetch_external=False)
        typer.echo(f"Materialized {ref.name} to {dest}")
    except Exception as e:
        raise typer.Exit(code=map_errors_to_exit_code(e))
```

### Testing Benefits

• **Unit test Operations methods** with fakes (no Typer/CLI involved)
• **Centralized error policy**: Test `map_errors_to_exit_code()` once  
• **Deterministic export testing**: Call `Operations.export()` twice, compare SHA-256
• **CLI smoke tests**: Patch Operations with minimal fakes

This facade provides the right amount of structure for Stage 5: keeps CLI maintainable, maximizes testability, and centralizes policy without over-engineering.

---

## 5) Minimal Cache 

**Cache Structure:**
- **Where**: `~/.modelops/bundles/<manifest-digest>/layers/<layer-id>/…`
- **What**: ORAS blobs only; no external objects by default
- **Shape**: Content-addressed directory tree, immutable once written
- **Lifecycle**: Created by `resolve()` (directory structure only), populated by `materialize()`

**Cache Behavior:**
- `resolve(..., cache=True)`: May create `~/.modelops/bundles/<manifest-digest>/` directory (empty). No blobs are downloaded in resolve(), even if cache=True.
- `materialize(...)`: Downloads ORAS blobs to cache, then hardlinks/symlinks to destination
- **How**: File locks per `<manifest-digest>`; SHA-256 verified on first write; reused thereafter
- **Why**: Massive reduction in cold-start time and network traffic, especially for many short trials

**MVP Limitations:**
- No cache eviction logic yet (add `modelops bundles cache gc --max-gb N` later)
- External data never cached (always fetched on-demand or to workdir via `prefetch_external=True`)
- Cache sharing safe across users/processes (content-addressed, immutable)

### 5.1 Cache Configuration

**Default path**: `~/.modelops/bundles/`

**Environment override**: `MODEL_OPS_CACHE_DIR=/mnt/fast-ssd/cache`
- Preserves the same content-addressed structure: `$MODEL_OPS_CACHE_DIR/<manifest-digest>/layers/<layer-id>/`
- Cache remains safe to share across users/processes (content-addressed, immutable)

**Use cases for override**:
- **CI/CD systems**: Ephemeral home directories, need persistent cache location
- **Shared team cache**: Network mount for faster cold starts across team members  
- **Performance optimization**: Faster SSD storage for performance-critical workloads
- **Cloud workers**: Pod-specific or node-level cache strategies

> MVP keeps cache **simple**: no eviction. We'll add `modelops bundles cache gc --max-gb N` later.

---

## 6) Integration With the Cloud Execution Stack

### 6.1 In Pods (Drones/Head)

1. **Start-up**: call `resolve(BundleRef(name=..., version=..., role="sim"))` to get the digest; record digest in telemetry.
2. **Workdir**: `materialize(ref, dest="/workspace", role="sim")`.
3. **Run**: Calabaria uses the local path (and its own evaluation/calibration logic).

This satisfies the core requirement: **workspaces are mirrored from the user’s machine to the cloud**—**only** the role’s layers are pulled into the job’s workdir.

### 6.2 Contracts Touchpoints

- `EvalRequest.provenance` includes:
  - `sim_image` (container digest or `local`)
  - `model_signature` (hash of Calabaria model schema/targets)
  - `contract_version` (`v1`)
- `ArtifactRef` entries in `EvalResult.artifacts` may include figures/diagnostics produced under the workdir and uploaded post‑run.

---

## 7) CLI Layout and Wiring

- `modelops-bundles` provides a **standalone Typer** CLI: `mops-bundles ...`.
- `modelops` exposes it under the umbrella CLI as a **plugin**: `modelops bundles ...` via entry points:
- CLI commands are kept thin (5-15 lines each) by delegating to the **Operations facade** (§4.11) for orchestration, error mapping, and policy centralization.

`modelops-bundles/pyproject.toml`:
```toml
[project.entry-points."modelops.plugins"]
bundles = "modelops_bundles.cli:app"
```

`modelops/cli.py`:
```python
# discovers and mounts subcommands from "modelops.plugins" entry points
```

**Commands (MVP):**
- `init` — create `.mops-bundle.yaml` with bundle metadata
- `scan` — dry-run inventory of blobs (no network calls)
- `plan` — show storage plan with optional `--external-preview`
- `diff` — compare local manifest to remote (manifest-only, fast)
- `push` — upload bundle to registry
- `pull` — pull bundle with role-awareness and optional `--prefetch-external`
- `export` — create offline `.tar.zst` archive
- `resolve` — fetch manifest + prime cache (no FS side effects)
- `materialize` — fetch role layers into destination directory
- `show` — pretty-print bundle information
- `gc` — garbage-collect old registry blobs

### 7.1 Enhanced CLI Features

**Storage Decision Preview:**
```bash
modelops bundles plan --external-preview --json
```
Output:
```json
[
  {
    "path": "data/fit/2022.parquet", 
    "size": 22415032145, 
    "decision": "external", 
    "uri": "az://epidata/fit/2022.parquet",
    "reason": "matches pattern: data/fit/**", 
    "layer": "fitdata"
  },
  {
    "path": "src/model.py",
    "size": 45120,
    "decision": "oras",
    "reason": "under size threshold",
    "layer": "code"
  }
]
```

**Note:** External URIs in `plan --external-preview` output are only included if they are resolvable via ExternalStore.stat(), not "guessed" or generated URIs.

**JSON Output Support:**
All commands support `--json` flag for structured output, enabling automation and CI/CD integration.

**Progress Bar Behavior:**
- **Interactive mode**: Show progress bars for long operations (push, pull, materialize)
- **Suppressed when**: `--json` flag is set OR `CI=true` environment variable
- **Rationale**: Keeps JSON output clean for parsing; prevents interference in CI logs

### 7.2 Command Details

**Diff - Manifest-Only Comparison:**
```bash
modelops bundles diff epi-abm:latest
```
Fast comparison without downloading blobs:
```
Added (3 files, 125 MB):
  + data/new_sim_2025.csv (45 MB)
  + configs/updated_params.yaml (80 KB)

Modified (2 files):
  ~ src/model.py (size: 45KB → 52KB, hash changed)
  ~ configs/base.yaml (size unchanged, hash changed)

Removed (1 file):
  - data/old_sim_2024.csv (200 MB)
```

**Export - Offline Archives:**
```bash
# Basic export (ORAS content + external ref metadata)
modelops bundles export epi-abm:latest --output bundle.tar.zst

# Include actual external data
modelops bundles export epi-abm:latest --output bundle.tar.zst --include-external

# Deterministic export of a materialized workdir
modelops bundles export ./workdir --output bundle.tar
# or (if compression is enabled in build)
modelops bundles export ./workdir --output bundle.tar.zst

# Import from archive
modelops bundles import bundle.tar.zst
```

Archives use tar with zstd compression for good balance of speed/compression. Use cases include air-gapped deployments, backup/archival, and sneakernet transfer to isolated systems.

### 7.3 CLI Output & Exit Semantics (normative)

• **Output modes**: human text (default) and structured JSON (`--json`) where defined in this spec.
• **Progress/UI**: suppressed when `--json` or `CI=true`.
• **Exit codes**: as per §9; export additionally uses ValidationError (2) for non-canonical trees (see §21.2).
• **Determinism guarantee**: export MUST produce byte-identical archives for identical input trees across hosts and runs (see §21).

---

## 8) Security, Provenance, and Policy

- **Content addressing**: all ORAS content verified by SHA‑256.
- **External refs**: carry checksums and sizes; runtime can verify after download.
- **Signing**: optional cosign/fulcio at publish time; verify on resolve/materialize (post-MVP).
- **Size caps**: enforce max per-blob and per-manifest; refuse pushes that exceed registry limits.
- **Path safety**: normalized, no absolute paths, no `..` components.

---

## 9) Observability & Errors

- Every `resolve` and `materialize` emits: bundle name/version, **digest**, selected **role**, byte counts, timings.

### 9.1 Error Types and Exit Codes

Standardized error handling for robust automation:

- **Exit code 0**: Success
- **Exit code 1**: `BundleNotFoundError` — Bundle doesn't exist in registry/path
- **Exit code 2**: `ValidationError` — Schema validation, corrupt manifest  
- **Exit code 3**: `BundleDownloadError` — Network issues, auth failures, storage errors
- **Exit code 10**: `UnsupportedMediaType` — Unknown or unsupported media type
- **Exit code 11**: `RoleLayerMismatch` — Role references non-existent layer
- **Exit code 12**: `WorkdirConflict` — Target files exist with different checksums (materialize conflict)

### 9.1.1 BundleNotFoundError Semantics

- **Raised in resolve()**: When registry/local lookup fails for the specified bundle reference
- **Raised in materialize()**: When the underlying resolve() call fails (bubbles up from resolve())
- **Triggers**: Invalid bundle name/version, missing digest, unreachable local path, registry authentication failures

**Actionable error messages** with hints:
```
ERROR: Bundle too large for registry (2.5GB > 2GB limit)
Hint: Add external_storage rules to .mops-bundle.yaml:
  external_storage:
    - pattern: "data/**"
      storage: "az://mybucket/bundles/"
```

### 9.2 Structured JSON Output

All commands support `--json` for machine-readable output:

**Success example** (`resolve --json`):
```json
{
  "manifest_digest": "sha256:abc123...",
  "name": "epi-abm",
  "version": "2.2.1", 
  "roles": {
    "sim": ["code", "config"],
    "fit": ["code", "config", "fitdata"]
  },
  "total_size": 450000000,
  "external_refs": 3
}
```

**Materialize success example** (`materialize --json`):
```json
{
  "manifest_digest": "sha256:abc123...",
  "dest": "/workspace",
  "role": "sim",
  "materialized_files": [
    {"path": "src/model.py", "action": "CREATED", "size": 2048, "type": "oras"},
    {"path": "configs/base.yaml", "action": "UNCHANGED", "size": 512, "type": "oras"},
    {"path": "data/fit/2022.parquet", "action": "CREATED", "size": 0, "type": "pointer"}
  ],
  "total_files": 3,
  "total_bytes_written": 2560,
  "cache_hits": 1,
  "external_pointers_created": 1
}
```

**Materialize conflict example** (exit code 12):
```json
{
  "error": "WorkdirConflict", 
  "message": "3 files conflict with existing content",
  "exit_code": 12,
  "conflicts": [
    {"path": "src/model.py", "expected_sha256": "abc123...", "actual_sha256": "def456..."},
    {"path": "configs/base.yaml", "expected_sha256": "ghi789...", "actual_sha256": "jkl012..."}
  ],
  "conflict_count": 3,
  "hint": "Use --overwrite flag to replace conflicting files, or clean the destination directory"
}
```

**Error example**:
```json
{
  "error": "BundleNotFoundError",
  "message": "Bundle epi-abm:2.2.999 not found in registry",
  "exit_code": 1,
  "hint": "Check bundle name/version or registry configuration"
}
```

- Retriable vs terminal errors clearly labeled; retries are backoff-capped.

### 9.3 Export-Specific Errors (normative)

`modelops bundles export` fails with ValidationError (2) when any invariant in §21.2 is violated, including:
• **Non-POSIX/unsafe paths** (absolute, `..`, or backslashes).
• **Symlinks encountered** in the input tree (unsupported).
• **Paths exceeding USTAR limits** (255 UTF-8 bytes combined; name/prefix limits apply).
• **Disallowed file types** (block/char/fifo/sockets).
• **Non-deterministic metadata** (extended attributes, ACLs) present and not ignored.

Error messages MUST name at least the first 5 offending paths and the violated rule.

---

## 10) Compatibility With Calabaria & Planner/Adapters

- Calabaria defines **scientific tasks** and evaluation functions; it **does not** deal with registries.
- ModelOps runners (Head/Drones) own **ask/tell** and call **`materialize`** to prepare the workdir before invoking Calabaria.
- Trial identity and provenance store the **bundle digest**, keeping runs reproducible regardless of “latest” tags.

---

## 11) MVP vs. Later

**MVP (ship now):**
- Contracts (`BundleRef`, `ResolvedBundle`, `MediaType` constants).
- `scan/plan/publish` in `modelops-bundles` with HybridStorage decisions.
- `resolve` (no side effects beyond optional ORAS cache), `materialize` (role-aware mirror).
- CLI plugin wiring under `modelops`.
- Minimal cache (no eviction).

**Later:**
- Cosign signing & policy admission.
- Prefetch policy for external blobs at `materialize(..., prefetch_external=True)`.
- Layer-delta pushes (rsync-like) and shallow pulls by file subset.
- Cache GC policy and distributed node-level cache hints.
- Streaming materialization (pipe only the files that the simulator touches).

---

## 12) Example Call Flows

### 12.1 Enhanced Local Dev Workflow

```bash
# Initialize config, define roles
modelops bundles init --name epi-abm --version 0.1.0

# Preview storage decisions before pushing
modelops bundles plan --external-preview
# Shows which files go to ORAS vs external storage

# Compare with remote version
modelops bundles diff epi-abm:latest

# Push with version bump
modelops bundles push --bump-patch

# Get structured bundle info for automation
modelops bundles resolve epi-abm:latest --json

# Pull for local testing with external data prefetch
modelops bundles pull epi-abm:latest --role sim --dest ./work --prefetch-external
```

### 12.2 Cloud Worker (K8s Pod)

```python
from modelops_contracts.artifacts import BundleRef
from modelops_bundles.runtime import resolve, materialize
from modelops_bundles.providers.oras_external import default_provider_from_env

# Runtime API - same everywhere
ref = BundleRef(name="epi-abm", version="2.2.1", role="sim")
provider = default_provider_from_env()              # get provider from environment config
rb = resolve(ref)                                    # fetch manifest, maybe prime cache
rb = materialize(ref, dest="/workspace", role="sim", provider=provider) # mirror to FS
# → run Calabaria tasks in /workspace
```

### 12.3 CI/CD Pipeline Integration

```bash
# Set custom cache location
export MODEL_OPS_CACHE_DIR=/mnt/ci-cache

# Automated bundle validation
if ! modelops bundles resolve epi-abm:latest --json > bundle_info.json; then
  echo "Bundle validation failed with exit code $?"
  exit 1
fi

# Extract digest for provenance tracking
BUNDLE_DIGEST=$(jq -r '.manifest_digest' bundle_info.json)
echo "Using bundle digest: $BUNDLE_DIGEST"
```

---

## 13) Rationale Recap (tying to your questions)

- **Why explicit MediaTypes?** To be policyable, interoperable, and evolvable with registries and scanners.
- **Why `resolve` vs `materialize`?** Identity vs side effects. It simplifies orchestration and reproducibility.
- **Why roles?** Minimal workdirs and faster startup; aligns with “only the layers needed for a task”.
- **Why keep a cache?** Huge perf/cost win; simple to implement safely (content-addressed, immutable).
- **Why types live in `modelops-contracts.artifacts`?** Shared artifact vocabulary across subsystems; keeps `modelops-bundles` pluggable.
- **Why external refs instead of shoving big data into OCI?** Cheaper, faster, lifecycle-friendly; checksums preserve integrity.

---

## 14) Appendix — Minimal Type Sketches

```python
# modelops_contracts/artifacts.py
@dataclass(frozen=True)
class BundleRef: ...
@dataclass(frozen=True)
class ResolvedBundle: ...
MediaTypeBundle = "application/vnd.modelops.bundle.manifest+json"
MediaTypeLayer  = "application/vnd.modelops.layer+json"
MediaTypeExtRef = "application/vnd.modelops.external-ref+json"
MediaTypeOCI    = "application/vnd.oci.image.manifest.v1+json"
```

```python
# modelops_bundles/runtime.py
def resolve(ref: BundleRef, *, cache: bool = True) -> ResolvedBundle: ...
def materialize(ref: BundleRef, dest: str, *, role: str | None = None, overwrite: bool = False, prefetch_external: bool = False) -> ResolvedBundle: ...
```

```yaml
# .mops-bundle.yaml (example)
bundle:
  name: epi-abm
  version: 0.2.3
  models:
    sim: "src/models.py:ABM"
  layers:
    - name: code
      paths: ["src/**/*.py"]
    - name: config
      paths: ["configs/**/*.yaml"]
    - name: fitdata
      paths: ["data/fit/**"]
  roles:
    sim: ["code", "config"]
    fit: ["code", "config", "fitdata"]
external_storage:
  - pattern: "data/fit/**"
    storage: "az://epidata/fit/"
    tier: "cool"
```

---

## 15) MVP Testing Strategy

**Target coverage**: 85%+ lines, 90%+ of core modules (operations, storage, runtime, config_manager).

### 15.1 Essential MVP Tests (Must Have)

**Core Functionality**
- **Config & Scanning**: `.mops-bundle.yaml` parsing, glob expansion, path security, deterministic hashing
- **Layering & Manifest Build**: Deterministic layer_id generation, manifest validation
- **Safety**: Path traversal prevention, checksum verification, deterministic output

**Operations Facade (§4.11)**
- **Unit test Operations methods** with fakes (no CLI/Typer involved)
- **Error mapping**: Test `map_errors_to_exit_code()` centralized policy
- **Command orchestration**: Each Operations method with mock runtime/providers
- **CLI smoke tests**: Patch Operations facade, test exit codes and basic output

**Runtime API**  
- **Resolve**: All resolution methods (name:tag, digest, local), correct manifest_digest/role returns
- **Materialize**: Role-based layer selection, external ref pointer creation, idempotency

**Storage Adapters**
- **ORAS Storage**: Basic upload/download/exists, media type attachment verification  
- **Object Storage**: Azure Blob Storage with Azurite for external storage (minimum viable)

**CLI & Integration**
- **Core Commands**: `init`, `push`, `pull`, `resolve`, `materialize` with exit codes and JSON output
- **End-to-End**: Push→Resolve→Materialize against local registry with role verification

### 15.2 Nice to Have for MVP (Defer if Time-Constrained)

- **Cross-Platform**: Linux/macOS/Windows compatibility testing
- **Concurrency**: Parallel materialize and cache locking  
- **Large Files**: Streaming behavior for multi-GB files

### 15.3 Defer to Post-MVP

- Property-based/fuzz testing with hypothesis
- Performance benchmarks and scale testing
- Garbage collection tests
- Additional cloud storage adapters (S3, GCS) beyond Azure MVP
- Distributed cache behavior
- K8s integration tests

**Rationale**: MVP tests focus on correctness, safety, and the core user journey (Push → Resolve → Materialize) while ensuring automation-friendly JSON output. This provides a working, safe system that can be extended with comprehensive testing post-MVP.

### 15.4 Deterministic Export Tests (must-have)

• **Byte identity**: Export the same tree twice → identical SHA-256 of archive bytes. **Test via Operations facade** for clean unit testing.
• **Header invariants**: Parse entries; assert `mtime=0`, `uid=gid=0`, `uname=gname=""`, file `0644`, dir `0755`, directories precede children.
• **Ordering**: Verify strict UTF-8 lexicographic order.
• **Path normalization**: Backslashes in input become `/` in archive; NFC normalization verifies equality against expected.
• **Negative cases (expect ValidationError (2))**:
  • Symlink present.
  • Absolute path or `..`.
  • USTAR overflow (long path).
  • Disallowed file types.

---

## 16) JSON Schemas & Data Models

### 16.1 Pydantic Models

**Core Data Models:**

```python
# modelops_contracts/artifacts.py
from pydantic import BaseModel, Field
from typing import Dict, List, Optional
from datetime import datetime

class BundleRef(BaseModel):
    """Reference to a bundle for resolution."""
    name: Optional[str] = Field(None, description="Bundle name (e.g., 'epi-sir')")
    version: Optional[str] = Field(None, description="Version or 'latest'")
    digest: Optional[str] = Field(None, pattern=r"^sha256:[a-f0-9]{64}$", description="Content digest")
    local_path: Optional[str] = Field(None, description="Local filesystem path")
    role: Optional[str] = Field(None, description="Default role hint")
    
    class Config:
        extra = "forbid"  # Prevent unexpected fields

class ResolvedBundle(BaseModel):
    """Result of bundle resolution with content addresses."""
    ref: BundleRef
    manifest_digest: str = Field(pattern=r"^sha256:[a-f0-9]{64}$")
    media_type: str = Field(default="application/vnd.modelops.bundle.manifest+json")
    roles: Dict[str, List[str]] = Field(description="Role name to layer names mapping")
    layers: List[str] = Field(description="All layer IDs in bundle")
    external_index_present: bool = Field(description="Contains external refs")
    total_size: int = Field(ge=0, description="Total bundle size in bytes")
    cache_dir: Optional[str] = Field(None, description="Local cache directory")
    
class PointerFile(BaseModel):
    """External data pointer file format."""
    schema_version: int = Field(1, description="Pointer file schema version")
    uri: str = Field(description="External storage URI")
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$", description="Content hash")
    size: int = Field(ge=0, description="File size in bytes") 
    tier: Optional[str] = Field(None, description="Storage tier (hot/cool/archive)")
    created_at: datetime = Field(description="Creation timestamp")
```

### 16.2 CLI JSON Output Schemas

**Plan Command Output:**
```python
class PlanEntry(BaseModel):
    """Single file in storage plan."""
    path: str
    size: int
    decision: str = Field(regex=r"^(oras|external)$")
    uri: Optional[str] = None  # For external storage
    reason: str
    layer: str

class PlanOutput(BaseModel):
    """Output of 'plan --external-preview --json'."""
    entries: List[PlanEntry]
    total_oras_size: int
    total_external_size: int
    total_files: int
```

**Diff Command Output:**
```python
class DiffEntry(BaseModel):
    path: str
    change_type: str = Field(regex=r"^(added|removed|modified)$")
    old_size: Optional[int] = None
    new_size: Optional[int] = None
    size_delta: int = 0

class DiffOutput(BaseModel):
    """Output of 'diff --json'."""
    added: List[DiffEntry]
    removed: List[DiffEntry]
    modified: List[DiffEntry]
    total_size_delta: int
```

**Resolve Command Output:**
```python
# Uses ResolvedBundle model directly
# Example: {"ref": {...}, "manifest_digest": "sha256:...", ...}
```

### 16.3 Schema Validation Benefits

- **API Stability**: Breaking changes require schema version bumps
- **Client Safety**: Pydantic validation prevents malformed data
- **Documentation**: Self-documenting with field descriptions
- **IDE Support**: Full type hints and autocompletion
- **Testing**: Schema-based property testing with Hypothesis

---

## 17) OCI Manifest Structure

### 17.1 Top-Level OCI Manifest

Example OCI manifest that wraps our ModelOps bundle:

```json
{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {
    "mediaType": "application/vnd.oci.empty.v1+json",
    "digest": "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
    "size": 2,
    "data": "{}"
  },
  "layers": [
    {
      "mediaType": "application/vnd.modelops.bundle.manifest+json",
      "digest": "sha256:abc123...",
      "size": 2048,
      "annotations": {
        "org.opencontainers.image.title": "epi-sir bundle manifest"
      }
    },
    {
      "mediaType": "application/vnd.modelops.layer+json", 
      "digest": "sha256:def456...",
      "size": 1024,
      "annotations": {
        "org.modelops.layer.name": "code"
      }
    },
    {
      "mediaType": "application/vnd.modelops.external-ref+json",
      "digest": "sha256:ghi789...",
      "size": 512,
      "annotations": {
        "org.modelops.layer.name": "fitdata"
      }
    }
  ],
  "annotations": {
    "org.modelops.bundle.name": "epi-sir",
    "org.modelops.bundle.version": "1.3.2",
    "org.opencontainers.image.created": "2025-01-15T10:00:00Z"
  }
}
```

### 17.2 Media Type Mapping

| Layer Type | MediaType | Contains |
|---|---|---|
| Bundle manifest | `application/vnd.modelops.bundle.manifest+json` | Top-level bundle metadata, roles, layer refs |
| Layer index | `application/vnd.modelops.layer+json` | File listings for a specific layer |
| External refs | `application/vnd.modelops.external-ref+json` | Pointers to external storage |
| OCI wrapper | `application/vnd.oci.image.manifest.v1+json` | Standards-compliant envelope |

### 17.3 Scanning & Policy Integration

This structure enables:
- **Registry scanners** can identify ModelOps bundles by media type
- **Admission controllers** can allowlist/blocklist bundle types
- **Policy engines** can inspect bundle contents without downloading
- **Vulnerability scanners** can analyze layer composition

Example policy rule:
```yaml
# Only allow bundles from trusted namespaces
- if: manifest.annotations["org.modelops.bundle.name"] startsWith "epi-"
  then: allow
- else: deny
```

---

## 18) Materialize Overwrite & Idempotency Semantics

**Goal**: Calling `materialize(ref, dest, …)` repeatedly is safe and deterministic. No silent drift. Conflicts are explicit.

### 18.1 Definitions

- **Target path**: Absolute path under `dest` for each materialized file (ORAS content) or pointer (external content)
- **Expected checksum**: SHA-256 from the manifest/layer index for ORAS files; for external content, the checksum stored in the pointer metadata (`sha256`)
- **Conflict**: A file exists at the target path whose checksum (or file type) does not match the expected state

### 18.2 Overwrite Rules

**Default behavior (`overwrite=False`):**
- If the target does not exist → write it
- If the target exists and checksum/type matches → no-op (keep as-is)
- If the target exists and checksum/type differs → fail fast with `WorkdirConflict` (exit code 12), print the first 20 conflicts and a count of the rest

**Overwrite behavior (`overwrite=True`):**
- If the target exists and checksum/type differs → replace atomically and log `REPLACED`
- If checksum/type matches → no-op

**Directory handling:**
- `materialize` never deletes files that are not part of the role (no pruning in MVP)
- If a directory/file type mismatch occurs (e.g., we expect a file but a directory exists):
  - `overwrite=False` → error (exit 12)
  - `overwrite=True` → remove the conflicting entry and write the expected type

**Symlinks & hardlinks:**
- Symlinks in the workdir are treated as files for conflict detection:
  - If a symlink points to content whose final bytes checksum equals expected → no-op
  - Otherwise, same conflict rules as above
- When using a node cache, implementation MAY create hardlinks or symlinks from cache into dest. From the user's perspective, the path must read as the correct bytes; checksums are computed on content, not inode metadata

### 18.3 Atomicity Requirements

- Every file write uses `*.tmp + fsync + rename` to guarantee atomic replacement
- On failure, `*.tmp` files are cleaned up best-effort; partial state never replaces targets
- Directory creation uses `exist_ok=True` and is race-safe

### 18.4 Logging

Always emit one of: `CREATED`, `UNCHANGED`, `REPLACED`, `CONFLICT` per path (suppressed in `--json` mode but included in structured output arrays).

### 18.5 Algorithm (per file)

```python
def materialize_file(dest: str, relpath: str, expected_sha256: str, content: bytes, overwrite: bool):
    tgt = dest / relpath
    if not exists(tgt):
        write_atomically(tgt, content)
        return "CREATED"
    else:
        if checksum(tgt) == expected_sha256 and type_ok(tgt):
            return "UNCHANGED"
        else:
            if not overwrite:
                return "CONFLICT"
            else:
                write_atomically(tgt, content)
                return "REPLACED"

# If any CONFLICT and not overwrite: exit code 12 (WorkdirConflict) with summary
```

---

## 19) Role Validation Contract

**Goal**: Roles are first-class and must be valid at authoring time and at runtime.

### 19.1 Authoring-time (during plan)

- Validate that every role in `.mops-bundle.yaml` references existing layers by name
- Validate that each listed layer has at least one file or external ref (warn if empty)
- On failure: `ValidationError` (exit code 2) with a list of unknown layer names per role

### 19.2 Runtime (during materialize)

- **Role Selection**: Follow the precedence rules defined in Section 4.7 (Role Selection Algorithm):
  - Function argument `role=...` > `ref.role` > manifest "default" role > error
  - If no role can be determined → `RoleLayerMismatch` (exit 11)
- **Validate the selected role exists**:
  - If role not found in `ResolvedBundle.roles` → `RoleLayerMismatch` (exit 11)
- **Validate that all layers referenced by the selected role are present in the published manifest**:
  - If any are missing → `RoleLayerMismatch` (exit 11) with the exact missing layer names
- Emit role → layers mapping in `--json` output for observability

### 19.3 CLI Error Example

```
ERROR: Role 'training' references unknown layers: ['simdata_v2', 'docs']
Hint: Check '.mops-bundle.yaml' roles or run 'modelops bundles show --layers'
Exit code: 11
```

---

## 20) Determinism Guarantees

**Goal**: Identical inputs produce identical `manifest_digest`. Any content change (or significant metadata change) moves the digest.

### 20.1 Canonicalization Rules

**File Inventory (Scan):**
- Paths are relative to the bundle root with forward slashes
- Normalize Unicode to NFC
- Strip `.` and prohibit `..` or absolute paths
- Record: `{path, mode, size, sha256}` where:
  - `sha256` is of the file bytes only (mtime is ignored)
  - `mode` is a normalized POSIX mode (e.g., files 0644, dirs 0755)

**Layer Identity:**
- For each layer, build a canonical JSON array of entries:
  ```json
  [
    {
      "path": "src/model.py",
      "mode": 420,
      "size": 2048,
      "sha256": "abc123def456789...",
      "type": "oras"
    },
    {
      "path": "data/large.parquet", 
      "mode": 420,
      "size": 2247583616,
      "sha256": "xyz789abc123def...",
      "type": "external",
      "uri": "az://epidata/large.parquet",
      "tier": "cool"
    }
  ]
  ```
- Sorted by path (UTF-8 byte order)
- Encoded as JSON with:
  - Sorted keys
  - No insignificant whitespace (`separators=(',', ':')`)
  - No trailing zeros/floats (integers only for sizes/modes)
- `layer_id = sha256(canonical_json_bytes)`

**Bundle Manifest Identity:**
- Build the identity payload JSON object including only content-defining fields:
  - `layers`: list of `{name, layer_id}`
  - `roles`: mapping `role -> [layer_names]`
  - Optional `external_index_present`
- Exclude fields that do not change bytes resolved at runtime:
  - Human annotations, timestamps, name, version, tags, provenance strings
- Serialize with the same canonical JSON rules
- `manifest_digest = sha256(identity_payload_bytes)`

### 20.2 Determinism Properties

- Reordering files, layers, or JSON object keys does not change the digest
- Changing any byte of a file or adding/removing a file does change the corresponding `layer_id` and thus the bundle `manifest_digest`
- Retagging (version changes) or updating annotations does not change `manifest_digest`

### 20.3 Developer Checklist

- Use a single canonicalizer function for all JSON hashed for identity
- Ensure Python JSON dumps call: `json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False)`
- Unit test: same inputs from different OS/filesystems yield identical `manifest_digest`

### 20.4 Export Determinism Inputs (normative)

Deterministic export (§21) treats the materialized directory as the sole source of truth. It MUST NOT:
• **Re-read registries/object stores/providers**.
• **Modify line endings or transcode bytes**.
• **Synthesize or omit files** beyond canonical directory entries.

---

## 21) Deterministic Export/Import

**Goal**: The same bundle exports to a byte-identical archive every time on every machine.

### 21.1 Archive Contents

**When not including external data:**
- Include all ORAS files at their final relative paths
- Include pointer files at `.mops/ptr/**`
- Include `.mops/.mops-manifest.json` (provenance) and a `BUNDLE.MANIFEST` JSON copy (the canonical identity payload)

**When `--include-external` is set:**
- Also include external data bytes at their final relative paths
- Keep pointer files; update them to `"fulfilled": true` with `"local_path": "./<relpath>"`

### 21.2 Canonicalization & Tar Invariants (normative)

• **Format**: POSIX USTAR (no PAX headers). If a path cannot fit USTAR limits, fail with ValidationError (2) and print the offending path.
• **Path rules**:
  • POSIX relative paths only; no absolute paths, no `..`, no backslashes.
  • Normalize to NFC Unicode; use forward slashes.
• **Ordering**:
  • Emit explicit parent directories.
  • Directories before their children.
  • Strict UTF-8 bytewise lexicographic sort across all entries.
• **Headers (all entries)**:
  • `mtime=0`, `uid=0`, `gid=0`, `uname=""`, `gname=""`
• **Modes**:
  • Directories `0755`
  • Regular files `0644` (runtime already normalizes executable intent; export does not infer).
• **Types allowed**: regular files and directories only.
  • Symlinks: reject (ValidationError 2).
  • Special files (fifo/blk/chr/socket): reject.
• **Extended metadata**:
  • Do not write xattrs, ACLs, SELinux, or platform forks; USTAR does not carry them.
• **Bytes**:
  • Archive the file bytes as-is (no EOL translation).
• **Compression**:
  • If compression is used (e.g., `.tar.zst`), the compressor/version/level MUST be pinned in implementation to keep byte identity. (Zstd level and library version MUST be fixed in CI/build to prevent drift.)
  • Determinism claim is at the final artifact bytes (i.e., deterministic `.tar` or deterministic `.tar.zst`, depending on the chosen output).

### 21.3 Reference Algorithm (normative pseudocode)

```python
def write_deterministic_tar(src_dir: Path, out_tar: BinaryIO):
    entries = collect_all_dirs_and_files(src_dir)
    # normalize & validate
    canon = [
        normalize_entry(e)  # NFC, forward slashes, relative, type check
        for e in entries
    ]
    validate_ustar_fits(canon)       # name/prefix limits
    canon_sorted = sort_utf8_dirs_first(canon)

    with tarfile.open(fileobj=out_tar, mode="w", format=tarfile.USTAR_FORMAT) as tar:
        for e in canon_sorted:
            ti = tarfile.TarInfo(e.relpath)
            ti.uid = 0; ti.gid = 0; ti.uname = ""; ti.gname = ""
            ti.mtime = 0
            if e.is_dir:
                ti.type = tarfile.DIRTYPE; ti.mode = 0o755
                tar.addfile(ti)
            else:
                ti.type = tarfile.REGTYPE; ti.mode = 0o644; ti.size = e.size
                with open(e.abs, "rb") as f:
                    tar.addfile(ti, fileobj=f)
```

### 21.4 Import Validation (normative)

Import MUST:
• **Read the archive** and verify ordering and header invariants (warn if recoverable; fail if canonicality is violated materially).
• **Verify ORAS file bytes** against the bundle manifest; verify external pointer checksums match the pointer JSON (sha256) for included bytes when `--include-external` is used.
• **Fail with ValidationError (2)** on any mismatch; list up to 20 offending entries.

### 21.5 Cross-Platform Notes (informative)

• **Case sensitivity**: determinism assumes the on-disk tree is already stable; scanning/materialization (§20.1) prevents path collisions.
• **Time, locale, TZ** have no effect (`mtime=0`).
• **Filesystem permissions** beyond the normalized modes do not influence output.

---

**End of Spec**

