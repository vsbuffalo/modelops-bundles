# ModelOps Bundles – Roadmap to MVP

**Date:** 2025-08-24  
**Last Updated After:** Major refactoring (dependency injection, protocol removal, media type simplification)  
**Audience:** Runtime & infra engineers

---

## ✅ Completed (Architecture & Core Runtime)

### Runtime Operations
- ✅ **resolve()** - Bundle reference to canonical digest resolution
- ✅ **materialize()/pull** - Download and materialize bundles to filesystem
- ✅ **export()** - Create deterministic tar.zst archives from workdir
- ✅ Role-based materialization with precedence rules
- ✅ External file pointer system (lazy loading)
- ✅ Comprehensive error handling with exit codes

### Storage & Registry
- ✅ **OrasBundleRegistry** - Direct ORAS implementation (no protocol abstraction)
- ✅ **ExternalStore** - Protocol for Azure/S3/GCS blob storage
- ✅ **AzureExternalAdapter** - Azure Blob Storage integration
- ✅ Content-addressable storage with SHA256 digests
- ✅ Standard JSON media types (removed custom `application/vnd.modelops.*`)

### Architecture & Testing
- ✅ **Dependency Injection** - CLIContext eliminates global state
- ✅ **Test Infrastructure** - 230 passing tests with FakeOrasBundleRegistry/FakeExternalStore
- ✅ **CLI Framework** - Typer-based CLI with all commands defined
- ✅ **Settings System** - Environment-based configuration with validation
- ✅ **Path Safety** - Protection against directory traversal attacks

---

## 🚧 Phase 1: Critical MVP Functionality (1-2 weeks)

### 1. Push Command Implementation (CRITICAL - Week 1)
**Status:** Stub only - returns fake message  
**Priority:** P0 - Blocks round-trip workflow  
**Implementation needed:**
- [ ] Scan working directory for files (use existing planner.py scan logic)
- [ ] Apply storage policy (ORAS vs external based on size/patterns)
- [ ] Upload missing ORAS blobs to registry
- [ ] Upload external files to blob storage
- [ ] Create bundle manifest with layer indexes
- [ ] Upload manifest layers to registry
- [ ] Create and tag OCI manifest
- [ ] Return canonical digest

**Files to implement:**
- `src/modelops_bundles/operations/facade.py:279` (remove TODO)
- `src/modelops_bundles/publisher.py:274,324,539,544` (multiple TODOs)

### 2. Cache System (Week 1-2)
**Status:** Directory exists but unused  
**Priority:** P0 - Required for performance  
**Implementation needed:**
- [ ] Content-addressable blob cache under `~/.modelops/bundles/<digest>/`
- [ ] Hardlink optimization for materialize (avoid copies)
- [ ] Cache population during resolve/materialize
- [ ] Cache cleanup and GC policies

**Files to implement:**
- `src/modelops_bundles/storage/resolve_oci.py:192` (cache directory creation)
- Add cache logic to materialize workflow

### 3. Change Detection (Week 2)
**Status:** Multiple TODOs throughout codebase  
**Priority:** P1 - Avoids unnecessary uploads  
**Implementation needed:**
- [ ] Compare local file digests with remote bundle manifest
- [ ] Skip uploads when content unchanged
- [ ] Force flag to override change detection

**Files to implement:**
- `src/modelops_bundles/publisher.py:112,559,564` (change detection TODOs)
- `src/modelops_bundles/planner.py:400,406,420` (digest comparison TODOs)

---

## 🔧 Phase 2: Production Hardening (1 week)

### 4. Verify Command (Week 3)
**Status:** Not implemented  
**Priority:** P1 - Integrity checking  
**Implementation needed:**
- [ ] Verify ORAS blob checksums against manifest
- [ ] Verify external file checksums against pointer files
- [ ] Report corruption or missing files
- [ ] Support for partial verification (specific layers/roles)

### 5. Observability & Progress (Week 3)
**Status:** Basic error handling exists  
**Priority:** P1 - User feedback  
**Implementation needed:**
- [ ] Progress bars for large downloads/uploads
- [ ] Structured logging with levels
- [ ] Operation timing and metrics
- [ ] Better error messages with suggested fixes

### 6. Concurrency Safety (Week 3)
**Status:** No locking implemented  
**Priority:** P2 - Multi-process safety  
**Implementation needed:**
- [ ] File locks for cache operations
- [ ] Atomic writes for manifests
- [ ] Cleanup on interruption (SIGINT/SIGTERM)

---

## 🎯 Phase 3: Developer Experience (1 week)

### 7. Scan & Plan Commands (Week 4)
**Status:** Stubs only  
**Priority:** P2 - Development workflow  
**Implementation needed:**
- [ ] **scan** - Analyze working directory, show file inventory
- [ ] **plan** - Show storage decisions (ORAS vs external, sizes, costs)
- [ ] JSON output mode for tooling integration
- [ ] Validation of .mops-bundle.yaml configuration

### 8. Enhanced CLI Features (Week 4)
**Status:** Basic CLI works  
**Priority:** P2 - Usability  
**Implementation needed:**
- [ ] JSON output mode for all commands (`--output json`)
- [ ] Verbose/debug logging levels (`-v`, `-vv`)
- [ ] Shell completion (bash, zsh, fish)
- [ ] Better help text and examples

---

## 🚀 Phase 4: Advanced Features (Future)

### 9. Diff Command
**Status:** Stub only  
**Implementation needed:**
- [ ] Compare two bundle references
- [ ] Compare working directory to bundle
- [ ] Show file-level diffs and changes
- [ ] Support for semantic diff (ignore timestamps, etc.)

### 10. Performance Optimizations
**Implementation needed:**
- [ ] Parallel downloads/uploads
- [ ] Resumable transfers for large files
- [ ] Delta compression for similar bundles
- [ ] Smart prefetching based on access patterns

### 11. Security & Compliance
**Implementation needed:**
- [ ] Bundle signing with Cosign
- [ ] Signature verification in resolve/materialize
- [ ] SBOM (Software Bill of Materials) generation
- [ ] Audit logging for compliance

---

## 📊 Current Status Summary

| Component | Status | Tests | Priority |
|-----------|---------|--------|----------|
| **Core Runtime** | ✅ Complete | 230 passing | Done |
| **Registry Operations** | ✅ Complete | ✅ Tested | Done |
| **Push Command** | ❌ Stub only | ❌ Blocked | **P0** |
| **Cache System** | 🚧 Partial | 🚧 Basic | **P0** |
| **Change Detection** | ❌ TODOs only | ❌ Missing | **P1** |
| **Verify Command** | ❌ Not started | ❌ Missing | **P1** |
| **Scan/Plan Commands** | ❌ Stubs only | ❌ Missing | **P2** |
| **Diff Command** | ❌ Stub only | ❌ Missing | **P3** |

## 🎯 Next Steps for MVP

1. **Week 1**: Implement push command - enables full round-trip workflow
2. **Week 2**: Add cache system and change detection - performance and efficiency  
3. **Week 3**: Add verify command and observability - production readiness

**Critical Path:** Push command is the blocker for MVP. Once that's done, the system provides complete bundle lifecycle management.