# Fake implementations for testing

from .fake_oras import FakeOrasStore
from .fake_external import FakeExternalStore

__all__ = ["FakeOrasStore", "FakeExternalStore"]