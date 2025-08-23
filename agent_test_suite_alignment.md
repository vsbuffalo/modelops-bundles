Short take: your plan is solid, but the pasted test suite doesn’t match it yet. There are several hard mismatches (APIs, helpers, and parser behavior) plus a few outright test bugs. Here’s the punch-list to align tests with the plan and get them green.

# What must change in tests


## 1) CLI bundle-ref parsing (plan says: no bare digests)
- **Remove/flip** tests that still accept bare digests:
  - In `TestCLIReferenceParsing`: `test_sha256_digest_parsing` and `test_oci_digest_parsing` should now **expect** `ValueError("Bare digests not supported")`.
  - In the same class, `test_slash_in_name_rejected` is wrong now—names with slashes are **allowed**. Delete or invert to accept slashes (you already have a correct version in `TestBundleRefParsing.test_name_with_slashes_accepted`).
- Keep the later suite `TestBundleRefParsing` (it already rejects bare digests and accepts `name@sha256:…`, windows/unix paths, complex names). That suite is the one to keep.

## 2) Fake registry API usage (now repo-aware)
Your `FakeOciRegistry` signature is:
```py
put_blob(repo, digest, data)
get_blob(repo, digest)
put_manifest(repo, media_type, payload, tag) -> digest
get_manifest(repo, ref) -> bytes
head_manifest(repo, ref) -> digest
```
Fix tests that call these without `repo`:

- In `TestRuntimeWithOrasExternal`:
  - `test_materialize_runtime_role_excludes_data`: change  
    `oras.put_blob(d_code, code_py)` →  
    `oras.put_blob("testns/bundles/test-bundle", d_code, code_py)` and set `repo = "testns/bundles/test-bundle"` like you do in the first test.
  - `test_deterministic_materialization`: same fix for `put_blob`.
  - `test_reserved_prefix_via_provider`: same fix for `put_blob`.
  - In `test_materialize_prefetch_external_with_conflicts` and `test_deterministic_materialization` you use `repo` but never define it. Add `repo = "testns/bundles/test-bundle"` at the top of each test.

- In `TestTotalSizeCalculation` you call `put_manifest` with just `(media_type, payload)`. Rewrite to use the repo-aware fake or drop this test (it’s testing logic that moved into `resolve()`/`build_repo()` anyway).

## 3) `resolve()` signature changed
Plan: `def resolve(ref, *, registry=None, settings=None, cache=True)`

- Update **all** monkey-patches of `runtime.resolve` in tests to accept the new signature, e.g.:
  ```py
  import modelops_bundles.runtime as rt
  original_resolve = rt.resolve
  rt.resolve = lambda ref, registry=None, settings=None, cache=True: resolved
  ...
  rt.resolve = original_resolve
  ```
- Where tests passed `repository=...` to `resolve()`, **remove** that arg. If determinism matters, pass a `settings` fixture instead:
  ```py
  resolved = resolve(ref, registry=oras, settings=settings)
  ```

## 4) Operations/CLI repository coupling
Your plan removes `repository=` everywhere and derives it from `Settings`. Update tests accordingly:

- **Delete or rewrite** `TestOperationsSecurityFixes` that asserts “repository must be provided” when a registry is injected. That’s no longer true—repository comes from `settings.registry_repo` inside runtime.
- In `TestOperationsFacade`, verify calls like:
  ```py
  with patch('modelops_bundles.operations.facade._resolve') as mock_resolve:
      ops.resolve(ref)
      # new signature passed through:
      mock_resolve.assert_called_once_with(ref, registry=registry, settings=None, cache=True)
  ```
  (If your facade now passes settings down, assert that instead.)

## 5) CLI helper changes
- You’re deleting `_create_registry_store()` and adding `_create_fake_registry()`.  
  In `TestCLISmokeTests.test_provider_injection`, stop patching `_create_registry_store`. Either:
  - patch `_create_fake_registry` to return your fake, or
  - don’t patch; just run with `--provider fake` and let the CLI build the fake by itself.

## 6) Provider factory tests (ditch direct ORAS adapters)
Your plan switches to the registry factory. Update `TestDefaultProviderFromEnv`:

- Replace patches of `modelops_bundles.storage.oras.OrasAdapter` with:
  ```py
  with patch('modelops_bundles.storage.registry_factory.make_registry') as mock_make_registry,        patch('modelops_bundles.storage.object_store.AzureExternalAdapter') as mock_azure:
      provider = default_provider_from_env()
      mock_make_registry.assert_called_once()
      mock_azure.assert_called_once()
  ```
- Keep the Azure-missing-config negative tests as-is.

## 7) Small, concrete test bugs
- A bunch of tests reference undefined variables or wrong digests:
  - In `TestRuntimeWithOrasExternal`, make sure every test defines the same `repo = "testns/bundles/test-bundle"` used in its `put_*` calls.
  - Anywhere you used `oras.put_blob(digest, bytes)` change to `oras.put_blob(repo, digest, bytes)`.
  - Ensure every `put_manifest` has `(repo, media_type, payload, tag)` and you feed the returned **digest** back into manifests/indexes properly.
- In tests that patch `materialize` kwargs, add the new `settings` kw if your facade/runtime now forwards it (or accept `**kwargs` in your test doubles).

## 8) Docstring/tests parity
- After fixing the parser docstring per plan, keep only this acceptance set in tests:
  - `name:version`
  - `name@sha256:<digest>`
  - absolute or relative local paths (`/…`, `C:\…`, `./…`, `../…`)
  - names **may** contain `/` (org/project/bundle)
  - **No** bare digest (with or without leading `@`)  
  You already have this covered in `TestBundleRefParsing`; delete the older contradictory suite.

## 9) Azure adapter tests
- You have both a heavy mocked suite and a simplified suite. Keep one (the simplified one is fine for unit CI). If you keep the heavy one, ensure the import shims land **before** importing the adapter (they currently do).

---

## Quick examples of fixes

### Fix a resolve monkeypatch
```py
import modelops_bundles.runtime as rt
original_resolve = rt.resolve
rt.resolve = lambda ref, registry=None, settings=None, cache=True: resolved
try:
    # test body
finally:
    rt.resolve = original_resolve
```

### Fix FakeOciRegistry usage
```py
repo = "testns/bundles/test-bundle"
d = "sha256:" + hashlib.sha256(b"data").hexdigest()
oras.put_blob(repo, d, b"data")
idx = oras.put_manifest(repo, LAYER_INDEX, _layer_index_doc([...]), "layer")
```

### Update provider-injection CLI test
```py
from modelops_bundles.cli import _add_fake_manifests_oci
with patch('modelops_bundles.cli._create_fake_registry') as mk_fake:
    fake = FakeOciRegistry()
    _add_fake_manifests_oci(fake)
    mk_fake.return_value = fake
    result = self.runner.invoke(app, ["resolve", "bundle:v1.0.0", "--provider", "fake"])
    assert result.exit_code == 0
```

---

## Final sanity checklist

- ✔ Conftest: your `test_env` fixture sets `MODELOPS_REGISTRY_*`; paired with the new `settings` plumbing, tests won’t depend on the developer’s env.
- ✔ Publisher duplicate import test removal — good.
- ✔ CLI: remove `repository=` in ops construction in smoke tests.
- ✔ Anywhere you compare `rt.resolve` call args in patched tests: adapt to the new signature.

Do the above and your plan + tests line up. If you want, I can draft a minimal PR diff that applies the test changes in one sweep.
