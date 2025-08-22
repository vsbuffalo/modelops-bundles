"""
Simplified conformance tests for ORAS adapter.

Tests key contracts without heavy SDK mocking.
"""
from __future__ import annotations

import hashlib
import pytest

from modelops_bundles.settings import Settings
from modelops_bundles.storage.oras import OrasAdapter


class TestOrasAdapter:
    """Test basic OrasAdapter functionality."""
    
    def _create_settings(self, **overrides):
        """Create test settings with registry configured."""
        defaults = {
            'registry_url': 'localhost:5000',
            'registry_repo': 'test/repo',
            'http_timeout_s': 30.0,
            'http_retry': 0
        }
        defaults.update(overrides)
        return Settings(**defaults)
    
    def test_constructor_requires_oras_py(self):
        """Test constructor requires oras-py (HTTP fallback disabled for OCI compliance)."""
        # Mock missing oras package
        import sys
        from unittest.mock import patch
        
        with patch.dict(sys.modules, {'oras': None, 'oras.client': None}):
            settings = self._create_settings()
            
            # This should now raise since HTTP fallback is disabled
            with pytest.raises(ImportError, match="oras-py package is required for ORAS operations"):
                OrasAdapter(settings=settings)
    
    def test_digest_format_validation(self):
        """Test that digest format validation works."""
        settings = self._create_settings()
        
        # Mock oras to avoid import issues but allow adapter creation
        from unittest.mock import patch, Mock
        
        mock_oras_client = Mock()
        with patch('oras.client.OrasClient', return_value=mock_oras_client):
            adapter = OrasAdapter(settings=settings)
            
            # Test invalid digest formats raise ValueError
            invalid_digests = [
                "invalid-digest",
                "sha256:invalid-hex-GGGG",
                "sha256:tooshort",
                "sha256:" + "a" * 63,  # 63 chars instead of 64
                "sha1:" + "a" * 40,   # Wrong algorithm
            ]
            
            for bad_digest in invalid_digests:
                with pytest.raises(ValueError, match="Invalid digest format"):
                    # This will fail validation before trying to access registry
                    adapter.get_blob(bad_digest)
    
    def test_media_type_validation(self):
        """Test that media type validation works for put_manifest."""
        settings = self._create_settings()
        
        from unittest.mock import patch, Mock
        
        mock_oras_client = Mock()
        with patch('oras.client.OrasClient', return_value=mock_oras_client):
            adapter = OrasAdapter(settings=settings)
            
            # Test invalid media type raises ValueError
            with pytest.raises(ValueError, match="Unsupported media type"):
                adapter.put_manifest("application/invalid+json", b'{"test": true}')
    
    def test_put_blob_digest_validation(self):
        """Test that put_blob validates digest matches data."""
        settings = self._create_settings()
        
        from unittest.mock import patch, Mock
        
        mock_oras_client = Mock()
        with patch('oras.client.OrasClient', return_value=mock_oras_client):
            adapter = OrasAdapter(settings=settings)
            
            data = b"test content"
            wrong_digest = "sha256:" + "0" * 64
            
            with pytest.raises(ValueError, match="Digest mismatch"):
                    adapter.put_blob(wrong_digest, data)
    
    def test_blob_exists_never_raises(self):
        """Test that blob_exists never raises exceptions per contract."""
        settings = self._create_settings()
        
        from unittest.mock import patch, Mock
        
        # Mock OrasClient that always raises
        mock_oras_client = Mock()
        mock_oras_client.blob_exists.side_effect = Exception("Network error")
        
        with patch('oras.client.OrasClient', return_value=mock_oras_client):
            adapter = OrasAdapter(settings=settings)
            
            # Should return False, not raise
            result = adapter.blob_exists("sha256:" + "a" * 64)
            assert result is False
    
    def test_docker_config_auth_parsing(self):
        """Test Docker config authentication parsing."""
        settings = self._create_settings()
        
        from unittest.mock import patch, Mock, mock_open
        
        # Mock oras-py since HTTP fallback was removed
        mock_oras_client = Mock()
        with patch('oras.client.OrasClient', return_value=mock_oras_client):
            with patch('builtins.open', mock_open(read_data='{"auths": {"localhost:5000": {"auth": "dGVzdDp0ZXN0cGFzc3dvcmQ="}}}')):
                with patch('pathlib.Path.exists', return_value=True):
                    adapter = OrasAdapter(settings=settings)
                    
                    # Should not raise - auth setup should work
                    assert adapter is not None
    
    def test_registry_url_normalization(self):
        """Test registry URL normalization - removed as HTTP fallback was disabled."""
        # This test is no longer relevant since HTTP fallback was disabled for OCI compliance
        # oras-py handles URL normalization internally
        pass
    
    def test_insecure_registry_http(self):
        """Test insecure registry configuration."""
        from unittest.mock import patch, Mock
        
        mock_oras_client = Mock()
        with patch('oras.client.OrasClient', return_value=mock_oras_client) as mock_oras_class:
            settings = self._create_settings(registry_url="localhost:5000", registry_insecure=True)
            adapter = OrasAdapter(settings=settings)
            
            # Verify oras-py was initialized with insecure=True
            mock_oras_class.assert_called_once_with(
                hostname="localhost:5000",
                insecure=True
            )
    
    def test_retry_configuration(self):
        """Test retry configuration is stored in settings."""
        settings = self._create_settings(http_retry=3)
        
        from unittest.mock import patch, Mock
        
        mock_oras_client = Mock()
        with patch('oras.client.OrasClient', return_value=mock_oras_client):
            adapter = OrasAdapter(settings=settings)
            
            # Verify retry setting is stored (oras-py handles retries internally)
            assert adapter._settings.http_retry == 3
    
    def test_known_media_types_accepted(self):
        """Test that known media types are accepted."""
        from modelops_bundles.storage.oras import KNOWN_MEDIA_TYPES
        
        settings = self._create_settings()
        
        from unittest.mock import patch, Mock
        
        mock_oras_client = Mock()
        with patch('oras.client.OrasClient', return_value=mock_oras_client):
            adapter = OrasAdapter(settings=settings)
            
            # These should not raise
            for media_type in KNOWN_MEDIA_TYPES:
                adapter._validate_media_type(media_type)  # Should not raise