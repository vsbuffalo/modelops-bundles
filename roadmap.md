# ModelOps Bundles – Roadmap to MVP

**Date:** 2025-08-21  
**Audience:** Runtime & infra engineers

---

Phase 1: Core Functionality (1-2 weeks)

1. Implement push command - critical for round-trip workflow
2. Add cache system with hardlink optimization
3. Implement verify() for integrity checking

Phase 2: Production Hardening (1 week)

4. Add concurrency safety with file locks
5. Implement progress/observability logging
6. Complete error handling edge cases

Phase 3: Developer Experience (1 week)

7. Implement scan and plan commands
8. Add JSON output mode
9. Write comprehensive documentation

Phase 4: Nice-to-haves

10. Implement diff command for bundle comparison
11. Add bundle signing/verification with Cosign
12. Performance optimizations (parallel downloads, etc.)
