Awesome scope. I read through all the snippets and here’s a focused, actionable review aimed at getting you to a production-ready MVP for **pull/materialize** and a working **push** path, plus some design/contract cleanups.

---

# High-impact issues (fix these first)

### 1) Settings aren’t threaded into runtime `resolve()` during `materialize`
- **What happens:** `Operations.materialize()` passes `registry` but not `settings`; `runtime.materialize()` then calls `resolve(ref, registry=registry)` (without settings), which defaults to `load_settings_from_env()`. If your CLI constructed settings (e.g., using a different `registry_repo` than env) you’ll resolve against the wrong repo.
- **Fix:** Pass settings through.

```python
# runtime/__init__.py  (change function signature)
def materialize(..., provider: ContentProvider, registry: 'OciRegistry' = None, settings=None) -> MaterializeResult:
    ...
    resolved = resolve(ref, registry=registry, settings=settings)
    ...
```

```python
# operations/facade.py
return _materialize(
    ref=ref,
    dest=dest,
    role=role,
    overwrite=overwrite,
    prefetch_external=prefetch_external,
    provider=self.provider,
    registry=self.registry,
    settings=self.settings,  # <-- add
)
```

_Do the same for `pull()` since it delegates to `materialize()`._

---

### 2) Layer index schema mismatch (provider vs planner/models vs fakes)
- **What happens:**  
  - `planner.create_layer_indexes()` emits entries like `{"oras": {"digest": "...", "size": N}}`.
  - The **fake** registry builder writes entries like `{"digest": "sha256:..."}` (flat).
  - `BundleContentProvider.iter_entries()` currently expects **flat** `digest` OR `external`, so it will throw “exactly one of ‘digest’ or ‘external’” for the nested `oras` shape.
- **Fix:** Teach provider to accept **both** shapes; keep producing the nested shape in new code, but stay backward compatible.

```python
# providers/bundle_content.py  inside iter_entries(...):
entry = doc.get("entries", [])
...
# determine storage form
oras_node = entry.get("oras")
external_node = entry.get("external")

if oras_node and external_node:
    raise ValueError(...)

if external_node:
    ext = external_node
    ...
elif oras_node:
    digest = oras_node.get("digest")
    size = int(oras_node.get("size", 0))
    ...
elif "digest" in entry:  # legacy/flat
    digest = entry["digest"]
    size = int(entry.get("size", 0))
    ...
else:
    raise ValueError(f"entry must have either 'oras' or 'external' (or legacy 'digest') for path '{path}'")
```

---

### 3) `push` isn’t wired; registry upload methods are unimplemented
- **Current state:**  
  - `Operations.push()` is a stub returning prose, but the CLI treats it as a **digest**.  
  - Publisher builds a manifest but only prints “Would push…”.  
  - `HybridOciRegistry.put_manifest`, `put_blob`, `ensure_blob`, and `blob_exists` aren’t implemented.  
  - `RegistryHTTP` lacks PUT/UPLOAD helpers.

- **Minimal MVP plan:**  
  A) Wire `Operations.push()` to publisher.  
  B) Implement HTTP blob upload & manifest PUT (Distribution API).  
  C) Add a *working* dry-run that still returns the computed manifest digest.

**A) Replace the stub in `Operations.push()`**

```python
# operations/facade.py
from ..publisher import push_bundle  # top

def push(self, working_dir: str, *, bump: Optional[str] = None, dry_run: bool = False, force: bool = False) -> str:
    # Optionally bump the tag from spec.version
    tag = None
    if bump:
        # Load spec just to read current version, then bump it
        from ..planner import scan_directory
        spec = scan_directory(Path(working_dir))
        tag = _apply_version_bump(spec.version, bump)

    return push_bundle(
        working_dir=working_dir,
        tag=tag,
        registry=self.registry,
        settings=self.settings,
        force=force,
        dry_run=dry_run,
    )
```

**B) Implement upload + PUT manifest (HTTP)**

_Add thin helpers to `RegistryHTTP` (keeps auth, retries, and errors centralized):_

```python
# storage/registry_http.py
def head_blob(self, bundle_name: str, digest: str) -> bool:
    repo = self._build_repo_path(bundle_name)
    try:
        self._request("HEAD", f"/v2/{repo}/blobs/{digest}")
        return True
    except httpx.HTTPStatusError as e:
        return False

def start_upload(self, bundle_name: str) -> str:
    repo = self._build_repo_path(bundle_name)
    r = self._request("POST", f"/v2/{repo}/blobs/uploads/")
    loc = r.headers.get("Location")
    if not loc:
        raise BundleDownloadError("Registry did not return upload Location")
    return loc

def upload_chunk(self, location_url: str, data: bytes) -> str:
    r = self._request("PATCH", location_url, headers={"Content-Type": "application/octet-stream"}, content=data)
    return r.headers.get("Location") or location_url

def finalize_upload(self, location_url: str, digest: str) -> None:
    sep = "&" if "?" in location_url else "?"
    self._request("PUT", f"{location_url}{sep}digest={digest}")

def put_manifest(self, bundle_name: str, media_type: str, payload: bytes, tag: str) -> str:
    repo = self._build_repo_path(bundle_name)
    r = self._request("PUT", f"/v2/{repo}/manifests/{tag}", headers={"Content-Type": media_type}, content=payload)
    digest = r.headers.get("Docker-Content-Digest")
    return digest or ""
```

_Wire `HybridOciRegistry` to those:_

```python
# storage/hybrid_oci_registry.py
def blob_exists(self, repo: str, digest: str) -> bool:
    bundle = self._extract_bundle_name(repo)
    return self._http.head_blob(bundle, digest)

def put_blob(self, repo: str, digest: str, data: Union[bytes, BinaryIO], size: int | None = None) -> None:
    if hasattr(data, "read"):
        data = data.read()
    bundle = self._extract_bundle_name(repo)
    if self.blob_exists(repo, digest):
        return
    loc = self._http.start_upload(bundle)
    loc = self._http.upload_chunk(loc, data)
    self._http.finalize_upload(loc, digest)

def ensure_blob(self, repo: str, digest: str, data: Union[bytes, BinaryIO], size: int | None = None) -> None:
    if not self.blob_exists(repo, digest):
        self.put_blob(repo, digest, data, size)

def put_manifest(self, repo: str, media_type: str, payload: bytes, tag: str) -> str:
    bundle = self._extract_bundle_name(repo)
    digest = self._http.put_manifest(bundle, media_type, payload, tag)
    if not digest:
        # compute and trust local if server didn't echo
        import hashlib
        digest = f"sha256:{hashlib.sha256(payload).hexdigest()}"
    return digest
```

**C) Make publisher actually push (no more “Would push…”)**

```python
# publisher.py (_build_and_push_oci_manifest)
def _build_and_push_oci_manifest(...):
    import hashlib
    descriptors = []
    for file_path, media_type in files_with_types:
        with open(file_path, "rb") as f:
            content = f.read()
        digest = f"sha256:{hashlib.sha256(content).hexdigest()}"
        size = len(content)
        registry.ensure_blob(repo, digest, content)
        descriptors.append({"mediaType": media_type, "digest": digest, "size": size})

    manifest = {
        "schemaVersion": 2,
        "mediaType": OCI_IMAGE_MANIFEST,
        "config": {"mediaType": OCI_EMPTY_CONFIG, "digest": OCI_EMPTY_CONFIG_DIGEST, "size": OCI_EMPTY_CONFIG_SIZE},
        "layers": descriptors,
        "annotations": {"org.modelops.bundle.name": plan.spec.name, "org.modelops.bundle.version": plan.spec.version},
    }
    if plan.spec.description:
        manifest["annotations"]["org.modelops.bundle.description"] = plan.spec.description

    manifest_bytes = json.dumps(manifest, sort_keys=True, separators=(',', ':'), ensure_ascii=True).encode("utf-8")
    digest = registry.put_manifest(repo, OCI_IMAGE_MANIFEST, manifest_bytes, tag)
    _print_push_summary(plan, layer_indexes={}, digest=digest, repo=repo, tag=tag)
    return digest
```

Now `Operations.push()` returns a **real digest**; `typer` output stays correct; `--dry-run` still returns the computed manifest digest without uploading.

---

### 4) Pointer “fulfilled” can get out of sync on prefetch failure
- **What happens:** You write the pointer with `fulfilled=True` *before* downloading. If the download fails or SHA mismatches, you keep a pointer that claims “fulfilled”.
- **Fix:** Write pointer with `fulfilled=False` first, then update (or rewrite) to `True` after successful write.

```python
# runtime/materialize(), in external branch
write_pointer_file(..., fulfilled=False, local_path=None)

if prefetch_external:
    ...
    stream = provider.fetch_external(entry)
    write_stream_atomically(target_path, stream, expected_sha=entry.sha256)
    # success: mark fulfilled
    write_pointer_file(..., fulfilled=True, local_path=entry_path)
```

(If you want to avoid rewriting, add a tiny “update pointer” helper, but rewriting is fine—the writer is deterministic.)

---

### 5) Don’t rely on `assert` for required collaborators
- **What happens:** `assert self.provider is not None` can be optimized out in `-O`.
- **Fix:** Raise a real error.

```python
if self.provider is None:
    raise ValueError("Provider required for materialize operations")
```

---

### 6) Exit-code mapping hides user-actionable details
- **What happens:** `run_and_exit()` maps exceptions to exit codes without surfacing conflict details.
- **Fix:** Special-case `WorkdirConflict` to print a short table of conflicts before exiting. Also map `ValueError` → 2.

```python
# operations/mappers.py
from .printers import print_conflicts
from ..runtime import WorkdirConflict

EXIT_CODES.update({"ValueError": 2})

def run_and_exit(func):
    try:
        return func()
    except WorkdirConflict as e:
        print_conflicts(e.conflicts)
        raise typer.Exit(code=exit_code_for(e)) from e
    except Exception as e:
        raise typer.Exit(code=exit_code_for(e)) from e
```

---

# Design & contract nits (SOLID / consistency)

- **I (Interface Segregation) / Contract drift:** `ContentProvider.iter_entries()` doc says “no I/O”, but your provider must fetch layer index JSON. Update the doc to “no *content* bytes downloaded; metadata fetches allowed” to align with reality.
- **S (Single Responsibility):** `Operations` owns config & orchestration; bump logic is fine here, but the semver helper could live next to publisher to keep responsibilities tight. Optional.
- **D (Dependency Inversion):** You already use `Protocol`s (`OciRegistry`, `ExternalStore`, `ContentProvider`). Good. The provider takes concrete `Settings`; that’s OK for now.
- **Manifest schema consistency:** Your *remote* manifest uses `layer_indexes` mapping + `layers` (list), while your *local model* uses `layers: Dict[str, str]`. Pick one schema and stick with it (I’d keep `layer_indexes` for digests and `layers` as a list of names used by roles; it reads cleanly).
- **CLI duplication:** `materialize` and `pull` are near duplicates; consider a shared helper to reduce drift.

---

# Smaller correctness/robustness tweaks

- **Conflict short-circuit:** When checking existing files, consider comparing file size before SHA to avoid hashing big unchanged files unnecessarily.
- **Progress output:** You already have `print_materialize_progress`; thread a simple callback (or pass `ci`/`verbose`) into runtime to display CREATED/UNCHANGED/REPLACED in non-CI mode.
- **`print_resolved_bundle` “Size”:** Currently always `0`. Either compute from indexes when available or hide when unknown.
- **`providers/__init__` stray quotes:** There’s a dangling `"""""` docstring—harmless but noisy; delete it.
- **Windows paths in `_parse_bundle_ref`:** Looks good (you guard absolute drive letters before `name:version`), just keep tests for `C:\dir:name.txt` edge cases.
- **Export tar determinism:** Great overall. If you plan to support symlinks later, decide whether to dereference or store link targets deterministically and document it.

---

# Quick test checklist for MVP

- **Pull/materialize**
  - Name+version, name@sha256, and local path refs.
  - Role resolution precedence (arg > ref.role > default).
  - External pointers written under `.mops/ptr/**` and skipped during export unless `--include-external`.
  - Overwrite semantics (unchanged vs conflict vs replace).

- **Push**
  - Small ORAS files become blobs.
  - Layer index JSON and bundle manifest uploaded with *preserved media types*.
  - Tag created; returned digest matches `Docker-Content-Digest` or local hash.
  - Dry run prints plan and still returns computed digest.

- **Error UX**
  - Missing role → exit 11 with helpful list.
  - Workdir conflicts → exit 12 with a table of first few paths.
  - Validation issues (bad ref, bad YAML) → exit 2.

---

If you want, I can draft the updated files for `runtime/materialize`, `operations/facade`, `providers/bundle_content`, `storage/hybrid_oci_registry`, and `storage/registry_http` in one go so you can paste them in.
