"""
Settings and configuration for ModelOps Bundles.

Centralizes configuration values and provides validation with fail-fast behavior.
Loads settings from environment variables at adapter construction time.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional

__all__ = ["Settings", "create_settings_from_env"]


@dataclass(frozen=True)
class Settings:
    """
    Configuration settings for ModelOps Bundles adapters.
    
    ORAS/Registry Settings:
        registry_url: OCI registry URL (required)
        registry_repo: OCI repository name (required)
        registry_insecure: Allow HTTP connections for local/dev use
        registry_user: Username for registry authentication
        registry_pass: Password for registry authentication
        http_timeout_s: HTTP request timeout in seconds
        http_retry: Number of retries for failed requests (0=no retry)
        
    External Storage Settings (Azure MVP):
        az_connection_string: Azure storage connection string
        az_account: Azure storage account name
        az_key: Azure storage account key
        az_blob_endpoint: Custom Azure blob endpoint (for Azurite/private endpoints)
        ext_timeout_s: External storage operation timeout
        allow_stat_without_sha: Allow stat() without SHA256 in metadata
    """
    # ORAS/Registry settings
    registry_url: str
    registry_repo: str
    registry_insecure: bool = False
    registry_user: Optional[str] = None
    registry_pass: Optional[str] = None
    http_timeout_s: float = 30.0
    http_retry: int = 0
    
    # External storage settings (Azure MVP)
    az_connection_string: Optional[str] = None
    az_account: Optional[str] = None
    az_key: Optional[str] = None
    az_blob_endpoint: Optional[str] = None
    ext_timeout_s: float = 60.0
    allow_stat_without_sha: bool = False
    
    def __post_init__(self):
        """Validate settings on construction."""
        # Validate registry URL format
        if not self.registry_url:
            raise ValueError("registry_url is required")
        
        # Basic URL validation - should be host[:port] or https://host[:port]
        url_pattern = r"^(?:https?://)?[a-zA-Z0-9.-]+(?::[0-9]+)?(?:/.*)?$"
        if not re.match(url_pattern, self.registry_url):
            raise ValueError(f"Invalid registry_url format: {self.registry_url}")
        
        # Validate registry repository format
        if not self.registry_repo:
            raise ValueError("registry_repo is required")
        
        # Repository name must follow OCI naming conventions
        repo_pattern = r"^[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)*$"
        if not re.match(repo_pattern, self.registry_repo):
            raise ValueError(f"Invalid registry_repo format: {self.registry_repo}. Must follow OCI naming conventions.")
        
        # Validate timeouts are positive
        if self.http_timeout_s <= 0:
            raise ValueError(f"http_timeout_s must be positive, got {self.http_timeout_s}")
        
        if self.ext_timeout_s <= 0:
            raise ValueError(f"ext_timeout_s must be positive, got {self.ext_timeout_s}")
        
        # Validate retry count is non-negative
        if self.http_retry < 0:
            raise ValueError(f"http_retry must be non-negative, got {self.http_retry}")
        
        # Validate Azure auth: require either connection string OR (account + key)
        has_conn_str = bool(self.az_connection_string)
        has_account_key = bool(self.az_account and self.az_key)
        
        if has_conn_str and has_account_key:
            raise ValueError("Specify either az_connection_string OR (az_account + az_key), not both")
        
        # For now, we don't require Azure auth to be configured (some deployments may only use ORAS)
        # But if partially configured, it must be complete
        if self.az_account and not self.az_key:
            raise ValueError("az_account specified but az_key is missing")
        if self.az_key and not self.az_account:
            raise ValueError("az_key specified but az_account is missing")


# Settings loading functions (no caching)


def create_settings_from_env() -> Settings:
    """
    Load settings from environment variables with memoization.
    
    Environment Variables:
        ORAS/Registry:
        - MODELOPS_REGISTRY_URL (required)
        - MODELOPS_REGISTRY_REPO (required)
        - MODELOPS_REGISTRY_INSECURE (default: false)
        - MODELOPS_REGISTRY_USERNAME (optional)
        - MODELOPS_REGISTRY_PASSWORD (optional)
        - MODELOPS_HTTP_TIMEOUT (default: 30.0)
        - MODELOPS_HTTP_RETRY (default: 0)
        
        External Storage (Azure):
        - AZURE_STORAGE_CONNECTION_STRING (optional)
        - AZURE_STORAGE_ACCOUNT (optional)
        - AZURE_STORAGE_KEY (optional)
        - MODELOPS_AZURE_BLOB_ENDPOINT (optional, for Azurite/custom endpoints)
        - MODELOPS_EXT_TIMEOUT (default: 60.0)
        - MODELOPS_ALLOW_STAT_WITHOUT_SHA (default: false)
    
    Returns:
        Settings object with validated configuration
        
    Raises:
        ValueError: If configuration is invalid or required values missing
        
    Note:
        Creates a fresh Settings instance every time (no caching).
        This ensures test isolation and eliminates global state.
    """
    return _load_settings_impl()




def _load_settings_impl() -> Settings:
    """Internal implementation of settings loading."""
    # Helper to convert string to bool
    def str_to_bool(value: str) -> bool:
        return value.lower() in ('true', '1', 'yes', 'on')
    
    # Helper to get float from env
    def get_float(key: str, default: float) -> float:
        value = os.getenv(key)
        return float(value) if value else default
    
    # Helper to get int from env
    def get_int(key: str, default: int) -> int:
        value = os.getenv(key)
        return int(value) if value else default
    
    # Registry settings
    registry_url = os.getenv("MODELOPS_REGISTRY_URL")
    registry_repo = os.getenv("MODELOPS_REGISTRY_REPO")
    
    if not registry_url:
        raise ValueError("MODELOPS_REGISTRY_URL environment variable is required")
    if not registry_repo:
        raise ValueError("MODELOPS_REGISTRY_REPO environment variable is required")
    
    registry_insecure = str_to_bool(os.getenv("MODELOPS_REGISTRY_INSECURE", "false"))
    registry_user = os.getenv("MODELOPS_REGISTRY_USERNAME")
    registry_pass = os.getenv("MODELOPS_REGISTRY_PASSWORD")
    
    http_timeout_s = get_float("MODELOPS_HTTP_TIMEOUT", 30.0)
    http_retry = get_int("MODELOPS_HTTP_RETRY", 0)
    
    # External storage settings (Azure)
    az_connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    az_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    az_key = os.getenv("AZURE_STORAGE_KEY")
    az_blob_endpoint = os.getenv("MODELOPS_AZURE_BLOB_ENDPOINT")
    
    ext_timeout_s = get_float("MODELOPS_EXT_TIMEOUT", 60.0)
    allow_stat_without_sha = str_to_bool(os.getenv("MODELOPS_ALLOW_STAT_WITHOUT_SHA", "false"))
    
    return Settings(
        registry_url=registry_url,
        registry_repo=registry_repo,
        registry_insecure=registry_insecure,
        registry_user=registry_user,
        registry_pass=registry_pass,
        http_timeout_s=http_timeout_s,
        http_retry=http_retry,
        az_connection_string=az_connection_string,
        az_account=az_account,
        az_key=az_key,
        az_blob_endpoint=az_blob_endpoint,
        ext_timeout_s=ext_timeout_s,
        allow_stat_without_sha=allow_stat_without_sha,
    )