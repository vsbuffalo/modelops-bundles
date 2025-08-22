# ModelOps Bundles – Roadmap to MVP

**Date:** 2025-08-21  
**Audience:** Runtime & infra engineers

---

## Stage 4 — Real Adapters (ORAS + Object Store)

**Scope**
- Implement production adapters behind Stage-2 interfaces.
- Keep implementations *tiny*: only the methods runtime/provider actually use.
- No policy, verify, or cache yet (that’s Stage 6).

**Artifacts**
- `storage/oras.py` — Wraps actual ORAS client (`oras-py` or HTTP client).
- `storage/object_store.py` — Wraps S3/Azure/GCS SDKs via existing auth layer.
- `settings.py` — Registry URI/env overrides; default timeouts.

**Tests**
- Unit tests remain on fakes only.
- Add adapter **conformance tests** with monkeypatched SDKs:
  - `tests/storage/test_oras_adapter_contract.py`
  - `tests/storage/test_object_adapter_contract.py`
- Tests assert correct calls, digests, headers, and media types.
- CI ensures adapters import everywhere; no real network required.

**Done when**
- Runtime still passes entirely on fakes.
- Adapters compile and pass conformance tests.

---

## Stage 5 — CLI + Deterministic Export

**Scope**
- Minimal Typer CLI exposing runtime/publisher:
  - `resolve`, `materialize`, `push`, `pull`, `scan`, `plan`, `diff`, `export`.
- Deterministic tar export (byte-identical re-exports).
- No JSON output in MVP (defer), only human text.

**Artifacts**
- `cli.py` — Typer entrypoint, mounted under umbrella `modelops`.
- `export.py` — Deterministic tar writer:
  - UTF-8 sorted paths
  - Fixed headers (uid/gid=0, mtime=0)
  - Forward slashes only

**Tests**
- `tests/cli/test_cli_smoke.py`
- `tests/export/test_export_determinism.py`

**Done when**
- CLI smoke tests pass with fakes.
- Export produces byte-identical tarballs across runs.

---

## Stage 6 — Cache, Concurrency, Verification & Error Mapping

**Scope**
- **Cache**:
  - `~/.modelops/bundles/<manifest-digest>/layers/<layer-id>/...`
  - Hardlink/symlink optimization from cache → workdir.
  - File locks per `<manifest-digest>` to avoid concurrent clobber.
- **Verification**:
  - `verify()` workflow: pointer JSONs checked with `ExternalStore.stat()`.
  - ORAS blobs checked by SHA256 against cache or registry.
- **Concurrency**:
  - Materialize with safe locking under parallel workers.
- **Error mapping**:
  - Standardize exceptions → exit codes (1/2/3/10/11/12).
- **Observability**:
  - Log one-line summary: name/version, digest, role, bytes, timings.

**Artifacts**
- `cache.py` — cache and lock helpers.
- Extend runtime with `verify()` function.
- Error → exit code map in CLI.

**Tests**
- `tests/cache/test_cache_hardlink_or_copy.py`
- `tests/runtime/test_concurrent_materialize_locking.py`
- `tests/cli/test_exit_codes.py`
- `tests/runtime/test_verify.py`

**Done when**
- No race conditions in tests.
- Exit codes match spec.
- Verify reports correct mismatches.

---

## Stage 7 — MVP Wrap-Up

**Scope**
- Docs: “How to push/pull bundles” guide.
- Polish error messages and CLI help.
- Ensure JSON schema validation (Pydantic) aligned with spec.
- CI: run tests with fakes only; adapters just compile.

**Artifacts**
- Final docs in `docs/` or README.
- Example `.mops-bundle.yaml`.

**Done when**
- Stages 1–6 are merged, all tests green.
- Core guarantees locked:
  - Overwrite/idempotency
  - Pointer placement
  - Role validation
  - Deterministic manifest_digest
  - Deterministic export
  - CI runs on fakes only

---

# Definition of Done (MVP)

- Contracts + runtime + provider + adapters + CLI merged.  
- Deterministic pipeline: identical inputs → identical digest/export.  
- Fakes power the full test suite; adapters have conformance tests.  
- CLI resolves, materializes, pushes, pulls, and exports safely.
