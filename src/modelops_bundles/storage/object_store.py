"""
Object store adapters for external storage.

Implements ExternalStore protocol for cloud providers, starting with Azure Blob Storage.
S3 and GCS adapters are stubbed for future implementation.
"""
from __future__ import annotations

import hashlib
import logging
from typing import Optional

from ..settings import Settings
from ..storage.base import ExternalStore, ExternalStat
from ..storage.uri import parse_external_uri

__all__ = ["AzureExternalAdapter", "external_adapter_for"]

logger = logging.getLogger(__name__)


class AzureExternalAdapter(ExternalStore):
    """
    ExternalStore adapter for Azure Blob Storage.
    
    Uses azure-storage-blob SDK with connection string or account+key authentication.
    Supports custom endpoints for Azurite and private Azure clouds.
    Implements stat() without downloading content by checking blob metadata.
    """
    
    def __init__(self, *, settings: Settings) -> None:
        """
        Initialize Azure adapter with settings.
        
        Args:
            settings: Settings containing Azure authentication and configuration
            
        Raises:
            ValueError: If Azure authentication is not properly configured
        """
        self._settings = settings
        self._validate_azure_auth()
        
        # Log configuration (without secrets)
        if settings.az_connection_string:
            if settings.az_blob_endpoint:
                logger.debug(f"Azure adapter using connection string auth with custom endpoint: {settings.az_blob_endpoint}")
            else:
                logger.debug("Azure adapter using connection string auth")
        elif settings.az_account and settings.az_key:
            if settings.az_blob_endpoint:
                logger.debug(f"Azure adapter using account+key auth for {settings.az_account} with custom endpoint: {settings.az_blob_endpoint}")
            else:
                logger.debug(f"Azure adapter using account+key auth for {settings.az_account}")
        else:
            logger.debug("Azure adapter initialized without authentication")
        
        logger.debug(f"Azure adapter timeout: {settings.ext_timeout_s}s, allow_stat_without_sha: {settings.allow_stat_without_sha}")
    
    def _validate_azure_auth(self) -> None:
        """Validate Azure authentication configuration."""
        has_conn_str = bool(self._settings.az_connection_string)
        has_account_key = bool(self._settings.az_account and self._settings.az_key)
        
        if not has_conn_str and not has_account_key:
            raise ValueError("Azure authentication not configured: need AZURE_STORAGE_CONNECTION_STRING or (AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY)")
    
    def _get_blob_client(self, uri: str):
        """
        Get Azure blob client for the given URI.
        
        Handles multiple connection patterns for Azure Blob Storage:
        
        1. Connection String + Production (AZURE_STORAGE_CONNECTION_STRING only):
           - Uses BlobServiceClient.from_connection_string()
           - Standard Azure cloud endpoints
           - Example: DefaultEndpointsProtocol=https;AccountName=myacct;AccountKey=...
        
        2. Connection String + Custom Endpoint (+ AZURE_BLOB_ENDPOINT):
           - Extracts account name from connection string
           - Overrides endpoint for Azurite/private clouds
           - Example: http://localhost:10000 for Azurite development
        
        3. Account+Key + Production (AZURE_STORAGE_ACCOUNT + AZURE_STORAGE_KEY):
           - Uses account URL: https://{account}.blob.core.windows.net
           - Standard Azure cloud with explicit credentials
        
        4. Account+Key + Custom Endpoint (+ AZURE_BLOB_ENDPOINT):
           - Uses custom endpoint: {endpoint}/{account}
           - For private clouds or development environments
        
        All patterns include retry configuration (5 retries, 0.4s backoff) for resilience
        against transient network issues and Azure throttling.
        """
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            raise ImportError("azure-storage-blob package required for Azure external storage")
        
        parsed = parse_external_uri(uri)
        if parsed.scheme != "az":
            raise ValueError(f"Expected az:// URI, got {parsed.scheme}://{parsed.container_or_bucket}/{parsed.key}")
        
        # Create service client using one of four connection patterns
        if self._settings.az_connection_string:
            # Pattern 1 & 2: Connection string-based authentication
            if self._settings.az_blob_endpoint:
                # Pattern 2: Connection string + custom endpoint (Azurite/private cloud)
                import re
                conn_str = self._settings.az_connection_string
                account_match = re.search(r'AccountName=([^;]+)', conn_str)
                if account_match:
                    account_name = account_match.group(1)
                    # Build custom endpoint URL: {endpoint}/{account}
                    endpoint_url = f"{self._settings.az_blob_endpoint.rstrip('/')}/{account_name}"
                    service_client = BlobServiceClient(
                        account_url=endpoint_url,
                        credential=None,  # Azurite typically doesn't need real credentials
                        connection_timeout=self._settings.ext_timeout_s,
                        retry_total=5,
                        retry_backoff_factor=0.4
                    )
                else:
                    # Fallback: Can't extract account name, use connection string as-is
                    service_client = BlobServiceClient.from_connection_string(
                        self._settings.az_connection_string,
                        connection_timeout=self._settings.ext_timeout_s,
                        retry_total=5,
                        retry_backoff_factor=0.4
                    )
            else:
                # Pattern 1: Standard connection string (Azure cloud)
                service_client = BlobServiceClient.from_connection_string(
                    self._settings.az_connection_string,
                    connection_timeout=self._settings.ext_timeout_s,
                    retry_total=5,
                    retry_backoff_factor=0.4
                )
        else:
            # Pattern 3 & 4: Account name + key authentication
            if self._settings.az_blob_endpoint:
                # Pattern 4: Account+key + custom endpoint
                account_url = f"{self._settings.az_blob_endpoint.rstrip('/')}/{self._settings.az_account}"
                service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self._settings.az_key,
                    connection_timeout=self._settings.ext_timeout_s,
                    retry_total=5,
                    retry_backoff_factor=0.4
                )
            else:
                # Pattern 3: Account+key + standard Azure cloud
                account_url = f"https://{self._settings.az_account}.blob.core.windows.net"
                service_client = BlobServiceClient(
                    account_url=account_url,
                    credential=self._settings.az_key,
                    connection_timeout=self._settings.ext_timeout_s,
                    retry_total=5,
                    retry_backoff_factor=0.4
                )
        
        # Get blob client for the specific container/blob
        blob_client = service_client.get_blob_client(
            container=parsed.container_or_bucket,
            blob=parsed.key
        )
        
        return blob_client, parsed
    
    def stat(self, uri: str) -> ExternalStat:
        """
        Get metadata for external object without downloading content.
        
        Args:
            uri: Azure blob URI (az://container/blob)
            
        Returns:
            ExternalStat with size, SHA256, and tier from blob properties
            
        Raises:
            FileNotFoundError: If blob does not exist
            OSError: If SHA256 missing from metadata and allow_stat_without_sha=False
        """
        try:
            from azure.core.exceptions import ResourceNotFoundError
        except ImportError:
            raise ImportError("azure-storage-blob package required for Azure external storage")
        
        blob_client, parsed = self._get_blob_client(uri)
        
        try:
            properties = blob_client.get_blob_properties()
        except ResourceNotFoundError:
            raise FileNotFoundError(f"Blob not found: {uri}")
        except Exception as e:
            raise OSError(f"Azure blob properties error: {e}")
        
        # Extract metadata
        size = properties.size
        
        # Look for SHA256 in blob metadata
        # Azure stores custom metadata with lowercase keys
        metadata = properties.metadata or {}
        sha256 = metadata.get('sha256') or metadata.get('modelops-sha256')
        
        if not sha256 and not self._settings.allow_stat_without_sha:
            raise OSError(f"SHA256 missing in blob metadata for {uri}")
        
        # Validate SHA256 format if present
        if sha256 and (len(sha256) != 64 or not all(c in '0123456789abcdef' for c in sha256)):
            raise OSError(f"Invalid SHA256 format in blob metadata for {uri}: {sha256}")
        
        # Extract tier from access tier property
        tier = None
        if hasattr(properties, 'blob_tier') and properties.blob_tier:
            tier_mapping = {
                'Hot': 'hot',
                'Cool': 'cool', 
                'Archive': 'archive'
            }
            tier = tier_mapping.get(properties.blob_tier.capitalize())
        
        return ExternalStat(
            uri=uri,
            size=size,
            sha256=sha256,
            tier=tier
        )
    
    def get(self, uri: str) -> bytes:
        """
        Retrieve external object content.
        
        Args:
            uri: Azure blob URI (az://container/blob)
            
        Returns:
            Blob content as bytes
            
        Raises:
            FileNotFoundError: If blob does not exist
            OSError: For other Azure/network errors
        """
        try:
            from azure.core.exceptions import ResourceNotFoundError
        except ImportError:
            raise ImportError("azure-storage-blob package required for Azure external storage")
        
        blob_client, parsed = self._get_blob_client(uri)
        
        try:
            download_stream = blob_client.download_blob()
            return download_stream.readall()
        except ResourceNotFoundError:
            raise FileNotFoundError(f"Blob not found: {uri}")
        except Exception as e:
            raise OSError(f"Azure blob download error: {e}")
    
    def put(
        self, 
        uri: str, 
        data: bytes, 
        *, 
        sha256: Optional[str] = None,
        tier: Optional[str] = None
    ) -> ExternalStat:
        """
        Store external object and return metadata.
        
        Args:
            uri: Azure blob URI (az://container/blob)
            data: Object content bytes
            sha256: Expected SHA256 hash (64 hex chars, no prefix)
            tier: Storage tier hint ("hot", "cool", "archive")
            
        Returns:
            ExternalStat with computed hash and size
            
        Raises:
            ValueError: If provided sha256 does not match computed hash
            OSError: For Azure/network errors
        """
        try:
            from azure.storage.blob import StandardBlobTier
        except ImportError:
            raise ImportError("azure-storage-blob package required for Azure external storage")
        
        blob_client, parsed = self._get_blob_client(uri)
        
        # Compute SHA256
        computed_sha = hashlib.sha256(data).hexdigest()
        
        # Validate provided hash if given
        if sha256 and sha256 != computed_sha:
            raise ValueError(f"SHA256 mismatch for {uri}: expected {sha256}, got {computed_sha}")
        
        # Prepare metadata
        metadata = {
            'modelops-sha256': computed_sha,
            'modelops-size': str(len(data))
        }
        
        # Map tier to Azure blob tier
        azure_tier = None
        if tier:
            tier_mapping = {
                'hot': StandardBlobTier.Hot,
                'cool': StandardBlobTier.Cool,
                'archive': StandardBlobTier.Archive
            }
            azure_tier = tier_mapping.get(tier.lower())
        
        try:
            # Upload blob with metadata
            blob_client.upload_blob(
                data,
                metadata=metadata,
                standard_blob_tier=azure_tier,
                overwrite=True
            )
        except Exception as e:
            raise OSError(f"Azure blob upload error: {e}")
        
        return ExternalStat(
            uri=uri,
            size=len(data),
            sha256=computed_sha,
            tier=tier
        )


# Stub implementations for S3 and GCS
class S3ExternalAdapter(ExternalStore):
    """S3 external storage adapter (not yet implemented)."""
    
    def __init__(self, *, settings: Settings) -> None:
        raise NotImplementedError("S3 external storage adapter not yet implemented")
    
    def stat(self, uri: str) -> ExternalStat:
        raise NotImplementedError("S3 external storage adapter not yet implemented")
    
    def get(self, uri: str) -> bytes:
        raise NotImplementedError("S3 external storage adapter not yet implemented")
    
    def put(self, uri: str, data: bytes, *, sha256: Optional[str] = None, tier: Optional[str] = None) -> ExternalStat:
        raise NotImplementedError("S3 external storage adapter not yet implemented")


class GCSExternalAdapter(ExternalStore):
    """Google Cloud Storage external storage adapter (not yet implemented)."""
    
    def __init__(self, *, settings: Settings) -> None:
        raise NotImplementedError("GCS external storage adapter not yet implemented")
    
    def stat(self, uri: str) -> ExternalStat:
        raise NotImplementedError("GCS external storage adapter not yet implemented")
    
    def get(self, uri: str) -> bytes:
        raise NotImplementedError("GCS external storage adapter not yet implemented")
    
    def put(self, uri: str, data: bytes, *, sha256: Optional[str] = None, tier: Optional[str] = None) -> ExternalStat:
        raise NotImplementedError("GCS external storage adapter not yet implemented")


def external_adapter_for(uri: str, settings: Settings) -> ExternalStore:
    """
    Create appropriate external storage adapter based on URI scheme.
    
    Args:
        uri: External storage URI
        settings: Settings for adapter configuration
        
    Returns:
        ExternalStore adapter for the URI scheme
        
    Raises:
        NotImplementedError: For unsupported schemes (S3, GCS)
        ValueError: For invalid URI format
    """
    parsed = parse_external_uri(uri)
    
    if parsed.scheme == "az":
        return AzureExternalAdapter(settings=settings)
    elif parsed.scheme == "s3":
        return S3ExternalAdapter(settings=settings)
    elif parsed.scheme == "gs":
        return GCSExternalAdapter(settings=settings)
    else:
        # This shouldn't happen since parse_external_uri validates schemes
        raise ValueError(f"Unsupported URI scheme: {parsed.scheme}")