# Supplement: CLI & UX Extensions for ModelOps Bundles

**Date:** 2025‑08‑21  
**Scope:** `modelops-bundles`, `modelops`, `modelops-contracts`  
**Audience:** Infra, runtime, and tooling engineers

---

## 1) CLI UX Extensions

### 1.1 `plan --external-preview`

- **Purpose**: Show how storage decisions would be applied before pushing.
- **Behavior**: 
  - Scan bundle and apply HybridStorage rules.
  - Output per-file decision: ORAS vs external, with reason string.
  - Default is rich table; `--json` for machine output.
- **MVP Implementation**: No network calls, just deterministic decisions.
- **Failure behavior**: Files exceeding registry max size and not covered by `external_storage` → error with suggested YAML snippet.

Example:
```bash
modelops bundles plan --external-preview --json
```

Output:
```json
[
  {"path": "data/fit/2022.parquet", "size": 22415032145, "decision": "external", "uri": "az://epidata/fit/2022.parquet", "reason": "matches pattern: data/fit/**", "layer": "fitdata"}
]
```

---

## 2) CLI Command Set (Refined)

MVP commands include:
- `init` — create `.mops-bundle.yaml`
- `scan` — dry-run inventory of blobs
- `plan` — show storage plan (with `--external-preview`)
- `diff` — compare local manifest to remote
- `push` — upload to registry
- `pull` — pull bundle (role-aware)
- `resolve` — fetch manifest + cache only
- `materialize` — fetch role layers into dest dir
- `show` — pretty-print bundle info
- `gc` — garbage-collect old registry blobs

---

## 3) UX Choices & Rationale

- **Explicit separation of identity vs side effects**: `resolve` (safe, no writes) vs `materialize` (mirrors to FS).
- **Roles**: Minimal working dirs for tasks. Defined in `.mops-bundle.yaml`.
- **Cache**: MVP keeps immutable content-addressed ORAS cache in `~/.modelops/bundles`. No eviction yet.
- **External preview**: Gives users transparency and safety before committing large pushes.

---

## 4) Missing/Unclear Points (from main spec)

For implementers, clarify:
1. **Diff**: Should operate on manifests only, not blobs; output summary of added/removed/changed files.
2. **Pull semantics**: Role-aware; optional `--prefetch-external` flag to immediately download big data.
3. **Cache path**: Explicit default (`~/.modelops/bundles`), overridable via `MODEL_OPS_CACHE_DIR` env var.
4. **Export**: Optional MVP feature; export to `.tar.zst` archive for offline use.
5. **Observability**: All commands should support `--json` structured output for automation.
6. **Error handling**: 
   - `BundleNotFoundError` → exit code 1
   - `ValidationError` → exit code 2
   - `BundleDownloadError` → exit code 3

---

## 5) Example UX Flows

**Local dev**
```bash
modelops bundles init --name epi-abm --version 0.1.0
modelops bundles plan --external-preview
modelops bundles push --bump-patch
modelops bundles resolve epi-abm:latest --json
modelops bundles materialize epi-abm:latest --role sim --dest ./work
```

**Cloud worker**
```python
from modelops_contracts.artifacts import BundleRef
from modelops_bundles.runtime import materialize

ref = BundleRef(name="epi-abm", version="2.2.1", role="sim")
rb = materialize(ref, dest="/workspace", role="sim")
```

---

**End of Supplement**
