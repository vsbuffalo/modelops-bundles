"""
End-to-end integration tests for ModelOps Bundles.

These tests verify the complete workflows against real services:
- Docker registry (localhost:5555)
- Azure storage (Azurite on localhost:10000)
- Full push/pull/materialize cycles
"""