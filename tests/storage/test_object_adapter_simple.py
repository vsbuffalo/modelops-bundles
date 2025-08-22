"""
Simplified conformance tests for external storage adapters.

Tests key contracts without heavy Azure SDK mocking.
"""
from __future__ import annotations

import pytest

from modelops_bundles.settings import Settings
from modelops_bundles.storage.object_store import AzureExternalAdapter, external_adapter_for, S3ExternalAdapter, GCSExternalAdapter


class TestAzureExternalAdapter:
    """Test basic AzureExternalAdapter functionality."""
    
    def test_constructor_requires_azure_auth(self):
        """Test that Azure authentication is required for construction."""
        # No Azure auth configured
        settings = Settings(registry_url='localhost:5000', registry_repo='test/repo')
        
        with pytest.raises(ValueError, match="Azure authentication not configured"):
            AzureExternalAdapter(settings=settings)
    
    def test_constructor_with_connection_string(self):
        """Test constructor with connection string auth."""
        settings = Settings(
            registry_url='localhost:5000',
            registry_repo='test/repo',
            az_connection_string='DefaultEndpointsProtocol=https;AccountName=test;AccountKey=testkey'
        )
        
        # Should not raise
        adapter = AzureExternalAdapter(settings=settings)
        assert adapter is not None
    
    def test_constructor_with_account_key(self):
        """Test constructor with account+key auth."""
        settings = Settings(
            registry_url='localhost:5000',
            registry_repo='test/repo',
            az_account='testaccount',
            az_key='testkey123'
        )
        
        # Should not raise
        adapter = AzureExternalAdapter(settings=settings)
        assert adapter is not None
    
    def test_methods_require_azure_sdk(self):
        """Test that methods raise ImportError when Azure SDK not available."""
        settings = Settings(
            registry_url='localhost:5000',
            registry_repo='test/repo',
            az_connection_string='DefaultEndpointsProtocol=https;AccountName=test;AccountKey=testkey'
        )
        
        # Patch the import to simulate missing SDK
        import sys
        from unittest.mock import patch
        
        with patch.dict(sys.modules, {'azure.storage.blob': None}):
            adapter = AzureExternalAdapter(settings=settings)
            
            with pytest.raises(ImportError, match="azure-storage-blob package required"):
                adapter.stat("az://container/blob.txt")
            
            with pytest.raises(ImportError, match="azure-storage-blob package required"):
                adapter.get("az://container/blob.txt")
            
            with pytest.raises(ImportError, match="azure-storage-blob package required"):
                adapter.put("az://container/blob.txt", b"data")
    


class TestStubAdapters:
    """Test S3 and GCS stub implementations."""
    
    def test_s3_adapter_not_implemented(self):
        """Test that S3 adapter raises NotImplementedError."""
        settings = Settings(registry_url='localhost:5000', registry_repo='test/repo')
        
        with pytest.raises(NotImplementedError, match="S3 external storage adapter not yet implemented"):
            S3ExternalAdapter(settings=settings)
    
    def test_gcs_adapter_not_implemented(self):
        """Test that GCS adapter raises NotImplementedError.""" 
        settings = Settings(registry_url='localhost:5000', registry_repo='test/repo')
        
        with pytest.raises(NotImplementedError, match="GCS external storage adapter not yet implemented"):
            GCSExternalAdapter(settings=settings)


class TestExternalAdapterFactory:
    """Test external_adapter_for factory function."""
    
    def test_azure_uri_returns_azure_adapter(self):
        """Test factory returns AzureExternalAdapter for az:// URIs."""
        settings = Settings(
            registry_url='localhost:5000',
            registry_repo='test/repo',
            az_connection_string='DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key'
        )
        
        adapter = external_adapter_for("az://container/blob", settings)
        assert isinstance(adapter, AzureExternalAdapter)
    
    def test_s3_uri_raises_not_implemented(self):
        """Test factory raises NotImplementedError for s3:// URIs."""
        settings = Settings(registry_url='localhost:5000', registry_repo='test/repo')
        
        with pytest.raises(NotImplementedError, match="S3 external storage adapter not yet implemented"):
            external_adapter_for("s3://bucket/key", settings)
    
    def test_gcs_uri_raises_not_implemented(self):
        """Test factory raises NotImplementedError for gs:// URIs."""
        settings = Settings(registry_url='localhost:5000', registry_repo='test/repo')
        
        with pytest.raises(NotImplementedError, match="GCS external storage adapter not yet implemented"):
            external_adapter_for("gs://bucket/object", settings)
    
    def test_invalid_uri_raises_value_error(self):
        """Test factory raises ValueError for invalid URI."""
        settings = Settings(registry_url='localhost:5000', registry_repo='test/repo')
        
        with pytest.raises(ValueError, match="Invalid URI format"):
            external_adapter_for("invalid://bad/uri", settings)