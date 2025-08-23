# Fake implementations for testing

from .fake_external import FakeExternalStore
from .fake_oci_registry import FakeOciRegistry

__all__ = ["FakeExternalStore", "FakeOciRegistry"]