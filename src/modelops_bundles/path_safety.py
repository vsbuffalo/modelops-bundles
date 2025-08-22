"""
Path safety utilities for ModelOps Bundles.

This module provides shared validation for user-provided paths to prevent
directory traversal attacks and protect reserved areas.
"""
from __future__ import annotations

from pathlib import PurePosixPath


def safe_relpath(path: str) -> str:
    """
    Validate and normalize a user-provided path to prevent traversal attacks.
    
    This function enforces the following safety rules:
    - No empty strings or "." (prevents root directory access)
    - No absolute paths (starting with '/')
    - No parent directory references ('..' components)
    - No paths starting with '.mops/' or equal to '.mops' (reserved metadata area)
    
    Args:
        path: User-provided path string
        
    Returns:
        Normalized relative path safe for use
        
    Raises:
        ValueError: If path violates safety rules
        
    Examples:
        >>> safe_relpath("src/model.py")
        'src/model.py'
        
        >>> safe_relpath("")
        ValueError: unsafe path: 
        
        >>> safe_relpath(".")
        ValueError: unsafe path: .
        
        >>> safe_relpath("../secrets.txt")
        ValueError: unsafe path: ../secrets.txt
        
        >>> safe_relpath(".mops")
        ValueError: unsafe path: .mops
        
        >>> safe_relpath(".mops/hijack.json")
        ValueError: unsafe path: .mops/hijack.json
    """
    rel = PurePosixPath(path)
    s = str(rel)
    if not s or s == ".":
        raise ValueError(f"unsafe path: {path}")
    if "\\" in s:
        raise ValueError(f"unsafe path: {path}")
    if rel.is_absolute() or ".." in rel.parts or s == ".mops" or s.startswith(".mops/"):
        raise ValueError(f"unsafe path: {path}")
    return s