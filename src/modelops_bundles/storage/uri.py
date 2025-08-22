"""
URI parsing utilities for external storage.

Provides consistent parsing and validation of external storage URIs
across different cloud providers (Azure, S3, GCS).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
import re

__all__ = ["ParsedURI", "parse_external_uri"]


@dataclass(frozen=True)
class ParsedURI:
    """
    Parsed components of an external storage URI.
    
    Attributes:
        scheme: Storage provider scheme (az, s3, gs)
        container_or_bucket: Container/bucket name
        key: Object key/path within container
        original: Original URI string for error messages
    """
    scheme: Literal["az", "s3", "gs"]
    container_or_bucket: str
    key: str
    original: str


def parse_external_uri(uri: str) -> ParsedURI:
    """
    Parse and validate an external storage URI.
    
    Accepts URIs in the form: {az|s3|gs}://container/path
    
    Validation:
    - Rejects URIs containing ".." (path traversal)
    - Rejects URIs with backslashes (non-POSIX paths)
    - Rejects URIs starting with "//" after scheme
    - Rejects empty container/bucket or key parts
    
    Args:
        uri: External storage URI to parse
        
    Returns:
        ParsedURI with validated components
        
    Raises:
        ValueError: If URI format is invalid or contains unsafe patterns
        
    Examples:
        >>> parse_external_uri("az://mycontainer/data/file.csv")
        ParsedURI(scheme='az', container_or_bucket='mycontainer', key='data/file.csv', original='...')
        
        >>> parse_external_uri("s3://mybucket/models/model.pkl")
        ParsedURI(scheme='s3', container_or_bucket='mybucket', key='models/model.pkl', original='...')
    """
    if not uri:
        raise ValueError("URI cannot be empty")
    
    # Check for unsafe patterns first
    if ".." in uri:
        raise ValueError(f"URI contains path traversal: {uri}")
    
    if "\\" in uri:
        raise ValueError(f"URI contains backslashes (use forward slashes): {uri}")
    
    # Parse scheme
    uri_pattern = r"^(az|s3|gs)://(.+)$"
    match = re.match(uri_pattern, uri)
    if not match:
        raise ValueError(f"Invalid URI format, expected scheme://container/key: {uri}")
    
    scheme, remainder = match.groups()
    
    # Check for weird leading forms like "//something"
    if remainder.startswith("/"):
        raise ValueError(f"URI path cannot start with '/': {uri}")
    
    # Split container and key
    if "/" not in remainder:
        raise ValueError(f"URI missing key part, expected container/key: {uri}")
    
    parts = remainder.split("/", 1)
    container = parts[0]
    key = parts[1]
    
    # Validate parts are not empty
    if not container:
        raise ValueError(f"Container/bucket name cannot be empty: {uri}")
    
    if not key:
        raise ValueError(f"Key/path cannot be empty: {uri}")
    
    return ParsedURI(
        scheme=scheme,  # type: ignore  # We validated it's one of the literals
        container_or_bucket=container,
        key=key,
        original=uri
    )