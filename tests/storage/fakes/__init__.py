# Fake implementations for testing

from .fake_external import FakeExternalStore
from .fake_oras_bundle_registry import FakeOrasBundleRegistry

__all__ = ["FakeExternalStore", "FakeOrasBundleRegistry"]