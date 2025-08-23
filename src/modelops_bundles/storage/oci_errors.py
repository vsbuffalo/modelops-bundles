"""
OCI registry error classes.

Provides a clear taxonomy of errors that can occur during OCI operations.
These errors are mapped from HTTP status codes and SDK exceptions to provide
a consistent error interface regardless of the underlying implementation.
"""
from __future__ import annotations


class OciError(Exception):
    """
    Base class for all OCI registry errors.
    
    This replaces various registry-specific errors with a unified hierarchy
    that makes error handling consistent across different registry implementations.
    """
    pass


class OciAuthError(OciError):
    """
    Authentication or authorization error.
    
    Raised when:
    - HTTP 401 Unauthorized (invalid credentials)
    - HTTP 403 Forbidden (insufficient permissions)
    - SDK authentication failures
    """
    pass


class OciNotFound(OciError):
    """
    Resource not found in registry.
    
    Raised when:
    - HTTP 404 Not Found (manifest, blob, or repository doesn't exist)
    - SDK resource not found exceptions
    """
    pass


class OciDigestMismatch(OciError):
    """
    Content digest validation failed.
    
    Raised when:
    - put_manifest: server digest != locally computed digest
    - put_blob: blob content doesn't match expected digest
    - Registry returns inconsistent digest values
    """
    
    def __init__(self, message: str, expected: str | None = None, actual: str | None = None):
        super().__init__(message)
        self.expected = expected
        self.actual = actual


class OciUnsupportedMediaType(OciError):
    """
    Media type not supported by registry or client.
    
    Raised when:
    - Registry rejects manifest due to unsupported media type
    - Client encounters unknown manifest format
    """
    pass


class OciTooLarge(OciError):
    """
    Content too large for registry limits.
    
    Raised when:
    - HTTP 413 Payload Too Large
    - Registry enforces size limits on manifests or blobs
    """
    pass


class OciRateLimited(OciError):
    """
    Rate limit exceeded.
    
    Raised when:
    - HTTP 429 Too Many Requests
    - Registry enforces rate limiting
    """
    pass


__all__ = [
    "OciError",
    "OciAuthError", 
    "OciNotFound",
    "OciDigestMismatch",
    "OciUnsupportedMediaType",
    "OciTooLarge",
    "OciRateLimited",
]