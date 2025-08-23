# Fake implementations for testing

from .fake_oras import FakeBundleRegistryStore
from .fake_external import FakeExternalStore

__all__ = ["FakeBundleRegistryStore", "FakeExternalStore"]