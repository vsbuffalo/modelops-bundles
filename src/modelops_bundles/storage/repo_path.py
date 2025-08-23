"""
Repository path construction helpers.

Centralizes the logic for building OCI repository paths from settings and bundle names.
"""
from __future__ import annotations

from ..settings import Settings


def build_repo(settings: Settings, bundle_name: str) -> str:
    """
    Build OCI repository path from settings and bundle name.
    
    Args:
        settings: Settings containing registry_repo
        bundle_name: Bundle name (e.g., "my-bundle", "org/project")
        
    Returns:
        Full repository path: "<registry_repo>/bundles/<bundle_name>"
        
    Examples:
        >>> settings = Settings(registry_repo="modelops/production")
        >>> build_repo(settings, "epi-sir")
        "modelops/production/bundles/epi-sir"
        
        >>> build_repo(settings, "org/project-bundle")
        "modelops/production/bundles/org/project-bundle"
    """
    if not bundle_name:
        raise ValueError("bundle_name cannot be empty")
    
    return f"{settings.registry_repo}/bundles/{bundle_name}"


def parse_repo(repo_path: str) -> tuple[str, str]:
    """
    Parse repository path back into namespace and bundle name.
    
    Args:
        repo_path: Full repository path
        
    Returns:
        Tuple of (namespace, bundle_name)
        
    Raises:
        ValueError: If repo_path doesn't match expected format
        
    Examples:
        >>> parse_repo("modelops/production/bundles/epi-sir")
        ("modelops/production", "epi-sir")
        
        >>> parse_repo("modelops/production/bundles/org/project-bundle") 
        ("modelops/production", "org/project-bundle")
    """
    if not repo_path:
        raise ValueError("repo_path cannot be empty")
    
    parts = repo_path.rsplit("/bundles/", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid repo path format: {repo_path}. Expected <namespace>/bundles/<bundle_name>")
    
    namespace, bundle_name = parts
    if not namespace or not bundle_name:
        raise ValueError(f"Invalid repo path format: {repo_path}. Both namespace and bundle_name must be non-empty")
    
    return namespace, bundle_name


__all__ = ["build_repo", "parse_repo"]