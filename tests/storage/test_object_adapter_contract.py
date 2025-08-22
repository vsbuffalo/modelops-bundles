"""
Conformance tests for external storage adapters.

Tests Azure external storage adapter with monkeypatched SDK to verify
correct behavior without real network calls.
"""
from __future__ import annotations

import sys
import hashlib
from unittest.mock import Mock, patch, MagicMock
import pytest

# Mock Azure SDK modules at import time
mock_azure = Mock()
mock_blob = Mock()
mock_exceptions = Mock()

# Create mock exception classes
class MockResourceNotFoundError(Exception):
    pass

mock_exceptions.ResourceNotFoundError = MockResourceNotFoundError
mock_blob.BlobServiceClient = Mock()
mock_blob.StandardBlobTier = Mock()

# Install mocks in sys.modules before any imports
sys.modules['azure'] = mock_azure
sys.modules['azure.storage'] = Mock()
sys.modules['azure.storage.blob'] = mock_blob
sys.modules['azure.core'] = Mock()
sys.modules['azure.core.exceptions'] = mock_exceptions

from modelops_bundles.settings import Settings
from modelops_bundles.storage.object_store import AzureExternalAdapter, external_adapter_for
from modelops_bundles.storage.base import ExternalStat


class TestAzureExternalAdapter:
    """Test AzureExternalAdapter contract compliance."""
    
    def _create_settings(self, **overrides):
        """Create test settings with Azure auth configured."""
        defaults = {
            'registry_url': 'localhost:5000',
            'registry_repo': 'test/repo',
            'az_connection_string': 'DefaultEndpointsProtocol=https;AccountName=test;AccountKey=testkey',
            'ext_timeout_s': 30.0,
            'allow_stat_without_sha': False
        }
        defaults.update(overrides)
        return Settings(**defaults)
    
    def test_azure_auth_validation(self):
        """Test that Azure authentication validation works."""
        # No auth configured should raise
        settings = Settings(registry_url='localhost:5000', registry_repo='test/repo')
        with pytest.raises(ValueError, match="Azure authentication not configured"):
            AzureExternalAdapter(settings=settings)
        
        # Connection string auth should work
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        assert adapter is not None
        
        # Account + key auth should work
        settings = Settings(
            registry_url='localhost:5000',
            registry_repo='test/repo',
            az_account='testaccount',
            az_key='testkey'
        )
        adapter = AzureExternalAdapter(settings=settings)
        assert adapter is not None
    
    def test_stat_with_sha256_in_metadata(self):
        """Test stat() returns metadata when SHA256 is present."""
        # Mock blob properties with SHA256 in metadata
        mock_properties = Mock()
        mock_properties.size = 1024
        mock_properties.metadata = {'modelops-sha256': 'a' * 64}
        mock_properties.blob_tier = 'Hot'
        
        mock_blob_client = Mock()
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        settings = self._create_settings()
        
        with patch('azure.storage.blob.BlobServiceClient', create=True) as mock_blob_service:
            mock_blob_service.from_connection_string.return_value = mock_service_client
            
            adapter = AzureExternalAdapter(settings=settings)
            result = adapter.stat("az://container/blob.txt")
            
            assert result.uri == "az://container/blob.txt"
            assert result.size == 1024
            assert result.sha256 == 'a' * 64
            assert result.tier == 'hot'
            
            # Verify SDK calls
            mock_blob_service.from_connection_string.assert_called_once()
            mock_service_client.get_blob_client.assert_called_once_with(container="container", blob="blob.txt")
            mock_blob_client.get_blob_properties.assert_called_once()
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_stat_missing_sha256_strict_mode_raises(self, mock_blob_service):
        """Test stat() raises when SHA256 missing and allow_stat_without_sha=False."""
        mock_properties = Mock()
        mock_properties.size = 1024
        mock_properties.metadata = {}  # No SHA256
        mock_properties.blob_tier = None
        
        mock_blob_client = Mock()
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings(allow_stat_without_sha=False)
        adapter = AzureExternalAdapter(settings=settings)
        
        with pytest.raises(OSError, match="SHA256 missing in blob metadata"):
            adapter.stat("az://container/blob.txt")
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_stat_missing_sha256_permissive_mode(self, mock_blob_service):
        """Test stat() allows missing SHA256 when allow_stat_without_sha=True."""
        mock_properties = Mock()
        mock_properties.size = 1024
        mock_properties.metadata = {}  # No SHA256
        mock_properties.blob_tier = None
        
        mock_blob_client = Mock()
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings(allow_stat_without_sha=True)
        adapter = AzureExternalAdapter(settings=settings)
        
        result = adapter.stat("az://container/blob.txt")
        
        assert result.uri == "az://container/blob.txt"
        assert result.size == 1024
        assert result.sha256 is None
        assert result.tier is None
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_stat_blob_not_found_raises_file_not_found(self, mock_blob_service):
        """Test stat() raises FileNotFoundError when blob doesn't exist."""
        from azure.core.exceptions import ResourceNotFoundError
        
        mock_blob_client = Mock()
        mock_blob_client.get_blob_properties.side_effect = ResourceNotFoundError("Not found")
        
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        
        with pytest.raises(FileNotFoundError, match="Blob not found"):
            adapter.stat("az://container/missing.txt")
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_stat_invalid_sha256_format_raises(self, mock_blob_service):
        """Test stat() raises when SHA256 has invalid format."""
        mock_properties = Mock()
        mock_properties.size = 1024
        mock_properties.metadata = {'modelops-sha256': 'invalid-hash'}  # Invalid format
        mock_properties.blob_tier = None
        
        mock_blob_client = Mock()
        mock_blob_client.get_blob_properties.return_value = mock_properties
        
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        
        with pytest.raises(OSError, match="Invalid SHA256 format"):
            adapter.stat("az://container/blob.txt")
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_stat_tier_mapping(self, mock_blob_service):
        """Test stat() correctly maps Azure blob tiers."""
        test_cases = [
            ('Hot', 'hot'),
            ('Cool', 'cool'),
            ('Archive', 'archive'),
            ('Unknown', None),
            (None, None)
        ]
        
        for azure_tier, expected_tier in test_cases:
            mock_properties = Mock()
            mock_properties.size = 1024
            mock_properties.metadata = {'modelops-sha256': 'a' * 64}
            mock_properties.blob_tier = azure_tier
            
            mock_blob_client = Mock()
            mock_blob_client.get_blob_properties.return_value = mock_properties
            
            mock_service_client = Mock()
            mock_service_client.get_blob_client.return_value = mock_blob_client
            mock_blob_service.from_connection_string.return_value = mock_service_client
            
            settings = self._create_settings()
            adapter = AzureExternalAdapter(settings=settings)
            
            result = adapter.stat("az://container/blob.txt")
            assert result.tier == expected_tier, f"Failed for tier {azure_tier}"
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_get_blob_content(self, mock_blob_service):
        """Test get() returns blob content."""
        content = b"test blob content"
        
        mock_download_stream = Mock()
        mock_download_stream.readall.return_value = content
        
        mock_blob_client = Mock()
        mock_blob_client.download_blob.return_value = mock_download_stream
        
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        
        result = adapter.get("az://container/blob.txt")
        
        assert result == content
        mock_blob_client.download_blob.assert_called_once()
        mock_download_stream.readall.assert_called_once()
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_get_blob_not_found_raises_file_not_found(self, mock_blob_service):
        """Test get() raises FileNotFoundError when blob doesn't exist."""
        from azure.core.exceptions import ResourceNotFoundError
        
        mock_blob_client = Mock()
        mock_blob_client.download_blob.side_effect = ResourceNotFoundError("Not found")
        
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        
        with pytest.raises(FileNotFoundError, match="Blob not found"):
            adapter.get("az://container/missing.txt")
    
    @patch('azure.storage.blob.BlobServiceClient')
    @patch('azure.storage.blob.StandardBlobTier')
    def test_put_blob_without_validation(self, mock_tier, mock_blob_service):
        """Test put() uploads blob and returns correct metadata."""
        content = b"test content"
        expected_sha = hashlib.sha256(content).hexdigest()
        
        mock_blob_client = Mock()
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        
        result = adapter.put("az://container/blob.txt", content)
        
        assert result.uri == "az://container/blob.txt"
        assert result.size == len(content)
        assert result.sha256 == expected_sha
        assert result.tier is None
        
        # Verify upload call
        mock_blob_client.upload_blob.assert_called_once()
        call_args = mock_blob_client.upload_blob.call_args
        assert call_args[0][0] == content  # First positional arg is data
        assert call_args[1]['metadata']['modelops-sha256'] == expected_sha
        assert call_args[1]['overwrite'] is True
    
    @patch('azure.storage.blob.BlobServiceClient')
    @patch('azure.storage.blob.StandardBlobTier')
    def test_put_blob_with_sha256_validation_success(self, mock_tier, mock_blob_service):
        """Test put() validates provided SHA256 successfully."""
        content = b"test content"
        expected_sha = hashlib.sha256(content).hexdigest()
        
        mock_blob_client = Mock()
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        
        result = adapter.put("az://container/blob.txt", content, sha256=expected_sha)
        
        assert result.sha256 == expected_sha
        mock_blob_client.upload_blob.assert_called_once()
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_put_blob_with_sha256_validation_failure(self, mock_blob_service):
        """Test put() raises ValueError when provided SHA256 doesn't match."""
        content = b"test content"
        wrong_sha = "0" * 64
        
        mock_blob_client = Mock()
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        
        with pytest.raises(ValueError, match="SHA256 mismatch"):
            adapter.put("az://container/blob.txt", content, sha256=wrong_sha)
        
        # Upload should not be called when validation fails
        mock_blob_client.upload_blob.assert_not_called()
    
    @patch('azure.storage.blob.BlobServiceClient')
    @patch('azure.storage.blob.StandardBlobTier')
    def test_put_blob_with_tier(self, mock_tier, mock_blob_service):
        """Test put() applies storage tier correctly."""
        content = b"test content"
        
        # Set up tier mocks
        mock_tier.Hot = 'Hot'
        mock_tier.Cool = 'Cool'
        mock_tier.Archive = 'Archive'
        
        mock_blob_client = Mock()
        mock_service_client = Mock()
        mock_service_client.get_blob_client.return_value = mock_blob_client
        mock_blob_service.from_connection_string.return_value = mock_service_client
        
        settings = self._create_settings()
        adapter = AzureExternalAdapter(settings=settings)
        
        test_cases = [
            ('hot', 'Hot'),
            ('cool', 'Cool'),
            ('archive', 'Archive'),
            ('unknown', None),
        ]
        
        for tier_input, expected_azure_tier in test_cases:
            mock_blob_client.reset_mock()
            
            result = adapter.put("az://container/blob.txt", content, tier=tier_input)
            
            if expected_azure_tier:
                assert result.tier == tier_input
                call_args = mock_blob_client.upload_blob.call_args
                assert call_args[1]['standard_blob_tier'] == expected_azure_tier
            else:
                # Unknown tiers should not set Azure tier
                call_args = mock_blob_client.upload_blob.call_args
                assert call_args[1]['standard_blob_tier'] is None
    
    @patch('azure.storage.blob.BlobServiceClient')
    def test_auth_with_account_key(self, mock_blob_service):
        """Test adapter uses account+key authentication correctly."""
        settings = Settings(
            registry_url='localhost:5000',
            registry_repo='test/repo',
            az_account='testaccount',
            az_key='testkey123'
        )
        
        mock_service_client = Mock()
        mock_blob_service.return_value = mock_service_client
        
        adapter = AzureExternalAdapter(settings=settings)
        
        # Trigger SDK usage
        mock_properties = Mock()
        mock_properties.size = 100
        mock_properties.metadata = {'modelops-sha256': 'a' * 64}
        mock_blob_client = Mock()
        mock_blob_client.get_blob_properties.return_value = mock_properties
        mock_service_client.get_blob_client.return_value = mock_blob_client
        
        adapter.stat("az://container/blob.txt")
        
        # Verify account URL and credential were used (not connection string)
        mock_blob_service.assert_called_once()
        call_args = mock_blob_service.call_args
        assert 'account_url' in call_args[1]
        assert 'testaccount.blob.core.windows.net' in call_args[1]['account_url']
        assert call_args[1]['credential'] == 'testkey123'


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