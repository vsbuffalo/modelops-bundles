"""
Tests for OrasBundleRegistry implementation.

Tests the real ORAS integration against mocked ORAS client to verify
correct API usage and error handling.
"""
import json
import pytest
from unittest.mock import Mock, MagicMock, patch

from modelops_bundles.settings import Settings
from modelops_bundles.storage.oras_bundle_registry import OrasBundleRegistry
from modelops_bundles.runtime import BundleDownloadError


@pytest.fixture
def settings():
    """Test settings."""
    return Settings(
        registry_url="https://localhost:5000",
        registry_repo="test/repo",
        registry_user="testuser",
        registry_pass="testpass"
    )


@pytest.fixture
def mock_oras_client():
    """Mock ORAS client with common responses."""
    client = Mock()
    
    # Mock manifest response
    manifest_dict = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {"digest": "sha256:abc123", "size": 123},
        "layers": []
    }
    client.get_manifest.return_value = manifest_dict
    
    # Mock blob response
    blob_response = Mock()
    blob_response.content = b"test blob content"
    client.get_blob.return_value = blob_response
    
    # Mock push response
    push_response = Mock()
    push_response.headers = {'Docker-Content-Digest': 'sha256:def456'}
    client.push.return_value = push_response
    
    return client


@pytest.fixture
def registry(settings, mock_oras_client, monkeypatch):
    """OrasBundleRegistry with mocked ORAS client."""
    reg = OrasBundleRegistry(settings)
    
    # Replace the lazy-loaded client with our mock
    monkeypatch.setattr(reg, '_oras_client', mock_oras_client)
    
    return reg


class TestOrasBundleRegistryInit:
    """Test registry initialization."""
    
    def test_init_with_settings(self, settings):
        """Test basic initialization."""
        registry = OrasBundleRegistry(settings)
        assert registry.settings == settings
        assert registry._oras_client is None  # Lazy loaded
    
    def test_oras_property_creates_client(self, settings):
        """Test that accessing oras property creates client."""
        registry = OrasBundleRegistry(settings)
        
        # Mock the oras.client module
        with patch('oras.client.OrasClient') as mock_client_class:
            mock_instance = Mock()
            mock_client_class.return_value = mock_instance
            
            client = registry.oras
            
            # Should create client with correct parameters
            mock_client_class.assert_called_once_with(
                hostname="localhost:5000",  # Protocol stripped
                insecure=False
            )
            
            # Should login with credentials
            mock_instance.login.assert_called_once_with(
                username="testuser",
                password="testpass", 
                insecure=False
            )
            
            # Should cache the client
            assert registry._oras_client is mock_instance
            assert client is mock_instance


class TestGetManifest:
    """Test get_manifest method."""
    
    def test_get_manifest_with_tag(self, registry, mock_oras_client):
        """Test getting manifest by tag."""
        result = registry.get_manifest("myrepo", "v1.0")
        
        # Should call ORAS with repo:tag format
        mock_oras_client.get_manifest.assert_called_once_with("myrepo:v1.0")
        
        # Should return JSON bytes
        expected = json.dumps(mock_oras_client.get_manifest.return_value,
                             separators=(',', ':'), sort_keys=True).encode()
        assert result == expected
    
    def test_get_manifest_with_digest(self, registry, mock_oras_client):
        """Test getting manifest by digest."""
        digest = "sha256:abc123def456"
        result = registry.get_manifest("myrepo", digest)
        
        # Should call ORAS with repo@digest format
        mock_oras_client.get_manifest.assert_called_once_with(f"myrepo@{digest}")
        
        # Should return JSON bytes
        expected = json.dumps(mock_oras_client.get_manifest.return_value,
                             separators=(',', ':'), sort_keys=True).encode()
        assert result == expected
    
    def test_get_manifest_returns_bytes_if_not_dict(self, registry, mock_oras_client):
        """Test manifest returned as-is if not a dict."""
        mock_oras_client.get_manifest.return_value = b"raw manifest bytes"
        
        result = registry.get_manifest("myrepo", "v1.0")
        
        assert result == b"raw manifest bytes"


class TestGetBlob:
    """Test get_blob method."""
    
    def test_get_blob_success(self, registry, mock_oras_client):
        """Test successful blob retrieval."""
        digest = "sha256:abc123def456"
        result = registry.get_blob("myrepo", digest)
        
        # Should call ORAS with repo@digest format
        mock_oras_client.get_blob.assert_called_once_with(f"myrepo@{digest}", digest)
        
        # Should return response content
        assert result == b"test blob content"
    
    def test_get_blob_raises_on_error(self, registry, mock_oras_client):
        """Test error handling in blob retrieval."""
        mock_oras_client.get_blob.side_effect = Exception("Network error")
        
        with pytest.raises(BundleDownloadError, match="Failed to fetch blob"):
            registry.get_blob("myrepo", "sha256:abc123")


class TestPushBundle:
    """Test push_bundle method."""
    
    def test_push_bundle_with_digest_header(self, registry, mock_oras_client):
        """Test push when response has digest header."""
        files = ["test.txt", "other.txt"]
        
        result = registry.push_bundle(files, "myrepo", "v1.0", {"key": "value"})
        
        # Should call ORAS push with correct parameters
        mock_oras_client.push.assert_called_once_with(
            files=files,
            target="myrepo:v1.0",
            manifest_annotations={"key": "value"},
            disable_path_validation=True
        )
        
        # Should return digest from header
        assert result == "sha256:def456"
    
    def test_push_bundle_fallback_to_manifest(self, registry, mock_oras_client):
        """Test push fallback when no digest header."""
        # Remove digest header
        mock_oras_client.push.return_value.headers = {}
        
        files = [{"path": "test.txt"}]
        result = registry.push_bundle(files, "myrepo", "v1.0")
        
        # Should call get_manifest for fallback
        mock_oras_client.get_manifest.assert_called_with("myrepo:v1.0")
        
        # Should compute digest from manifest
        manifest_bytes = json.dumps(mock_oras_client.get_manifest.return_value,
                                   separators=(',', ':'), sort_keys=True).encode()
        import hashlib
        expected_digest = f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}"
        assert result == expected_digest


class TestHeadManifest:
    """Test head_manifest method."""
    
    def test_head_manifest_computes_digest(self, registry, mock_oras_client):
        """Test head_manifest computes digest from get_manifest."""
        result = registry.head_manifest("myrepo", "v1.0")
        
        # Should call get_manifest
        mock_oras_client.get_manifest.assert_called_once_with("myrepo:v1.0")
        
        # Should compute SHA256 of manifest bytes
        manifest_bytes = json.dumps(mock_oras_client.get_manifest.return_value,
                                   separators=(',', ':'), sort_keys=True).encode()
        import hashlib
        expected_digest = f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}"
        assert result == expected_digest


class TestBlobExists:
    """Test blob_exists method."""
    
    def test_blob_exists_true(self, registry, mock_oras_client):
        """Test blob_exists returns True when blob exists."""
        result = registry.blob_exists("myrepo", "sha256:abc123")
        
        # Should try to get blob
        assert result is True
    
    def test_blob_exists_false_on_error(self, registry, mock_oras_client):
        """Test blob_exists returns False on error."""
        mock_oras_client.get_blob.side_effect = BundleDownloadError("Not found")
        
        result = registry.blob_exists("myrepo", "sha256:abc123")
        
        assert result is False


class TestPullBundle:
    """Test pull_bundle method."""
    
    def test_pull_bundle_returns_file_list(self, registry, mock_oras_client):
        """Test pull_bundle returns list of files."""
        mock_oras_client.pull.return_value = ["file1.txt", "file2.txt"]
        
        result = registry.pull_bundle("myrepo", "v1.0", "/tmp/dest")
        
        mock_oras_client.pull.assert_called_once_with(
            target="myrepo:v1.0",
            outdir="/tmp/dest"
        )
        
        assert result == ["file1.txt", "file2.txt"]
    
    def test_pull_bundle_scans_directory_fallback(self, registry, mock_oras_client, tmp_path):
        """Test pull_bundle scans directory when no file list returned."""
        mock_oras_client.pull.return_value = None
        
        # Create some test files
        (tmp_path / "file1.txt").write_text("content1")
        (tmp_path / "file2.txt").write_text("content2")
        
        # Mock Path.glob to return our test files
        with patch('pathlib.Path.glob') as mock_glob:
            mock_glob.return_value = [tmp_path / "file1.txt", tmp_path / "file2.txt"]
            
            result = registry.pull_bundle("myrepo", "v1.0", str(tmp_path))
        
        assert len(result) == 2
        assert str(tmp_path / "file1.txt") in result
        assert str(tmp_path / "file2.txt") in result