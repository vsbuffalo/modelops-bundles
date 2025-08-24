"""
Tests for settings module.

Tests settings validation, environment variable loading, and caching behavior.
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import patch

from modelops_bundles.settings import Settings, create_settings_from_env


class TestSettings:
    """Test Settings dataclass validation."""
    
    def test_minimal_valid_settings(self):
        """Test creating settings with minimal required values."""
        settings = Settings(registry_url="localhost:5000", registry_repo="test/repo")
        assert settings.registry_url == "localhost:5000"
        assert settings.registry_insecure is False
        assert settings.http_timeout_s == 30.0
        assert settings.http_retry == 0
        assert settings.ext_timeout_s == 60.0
        assert settings.allow_stat_without_sha is False
    
    def test_full_settings_with_azure_connection_string(self):
        """Test settings with all values including Azure connection string."""
        settings = Settings(
            registry_url="https://myregistry.azurecr.io",
            registry_repo="test/repo",
            registry_insecure=True,
            registry_user="testuser",
            registry_pass="testpass",
            http_timeout_s=45.0,
            http_retry=3,
            az_connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key",
            ext_timeout_s=120.0,
            allow_stat_without_sha=True,
        )
        assert settings.registry_url == "https://myregistry.azurecr.io"
        assert settings.registry_insecure is True
        assert settings.registry_user == "testuser"
        assert settings.registry_pass == "testpass"
        assert settings.http_timeout_s == 45.0
        assert settings.http_retry == 3
        assert settings.az_connection_string == "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key"
        assert settings.ext_timeout_s == 120.0
        assert settings.allow_stat_without_sha is True
    
    def test_azure_account_key_auth(self):
        """Test settings with Azure account + key authentication."""
        settings = Settings(
            registry_url="localhost:5000",
            registry_repo="test/repo",
            az_account="testaccount",
            az_key="testkey123=="
        )
        assert settings.az_account == "testaccount"
        assert settings.az_key == "testkey123=="
        assert settings.az_connection_string is None
    
    def test_empty_registry_url_raises(self):
        """Test that empty registry URL raises ValueError."""
        with pytest.raises(ValueError, match="registry_url is required"):
            Settings(registry_url="", registry_repo="test/repo")
    
    def test_missing_registry_url_raises(self):
        """Test that missing registry URL raises ValueError."""
        with pytest.raises(ValueError, match="registry_url is required"):
            Settings(registry_url=None, registry_repo="test/repo")  # type: ignore
    
    def test_invalid_registry_url_format_raises(self):
        """Test that invalid registry URL format raises ValueError."""
        with pytest.raises(ValueError, match="Invalid registry_url format"):
            Settings(registry_url="not a url", registry_repo="test/repo")
        
        with pytest.raises(ValueError, match="Invalid registry_url format"):
            Settings(registry_url="://missing-scheme", registry_repo="test/repo")
    
    def test_valid_registry_url_formats(self):
        """Test various valid registry URL formats."""
        # Plain host
        Settings(registry_url="localhost", registry_repo="test/repo")
        Settings(registry_url="registry.example.com", registry_repo="test/repo")
        
        # Host with port
        Settings(registry_url="localhost:5000", registry_repo="test/repo")
        Settings(registry_url="registry.example.com:443", registry_repo="test/repo")
        
        # HTTP URLs
        Settings(registry_url="http://localhost:5000", registry_repo="test/repo")
        Settings(registry_url="https://myregistry.azurecr.io", registry_repo="test/repo")
        
        # URLs with paths
        Settings(registry_url="https://registry.example.com/v2", registry_repo="test/repo")
    
    def test_negative_timeout_raises(self):
        """Test that negative timeouts raise ValueError."""
        with pytest.raises(ValueError, match="http_timeout_s must be positive"):
            Settings(registry_url="localhost:5000", registry_repo="test/repo", http_timeout_s=-1.0)
        
        with pytest.raises(ValueError, match="ext_timeout_s must be positive"):
            Settings(registry_url="localhost:5000", registry_repo="test/repo", ext_timeout_s=-5.0)
    
    def test_zero_timeout_raises(self):
        """Test that zero timeouts raise ValueError."""
        with pytest.raises(ValueError, match="http_timeout_s must be positive"):
            Settings(registry_url="localhost:5000", registry_repo="test/repo", http_timeout_s=0.0)
    
    def test_negative_retry_raises(self):
        """Test that negative retry count raises ValueError."""
        with pytest.raises(ValueError, match="http_retry must be non-negative"):
            Settings(registry_url="localhost:5000", registry_repo="test/repo", http_retry=-1)
    
    def test_azure_auth_both_methods_raises(self):
        """Test that specifying both Azure auth methods raises ValueError."""
        with pytest.raises(ValueError, match="Specify either az_connection_string OR"):
            Settings(
                registry_url="localhost:5000",
                registry_repo="test/repo",
                az_connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key",
                az_account="testaccount",
                az_key="testkey"
            )
    
    def test_azure_account_without_key_raises(self):
        """Test that Azure account without key raises ValueError."""
        with pytest.raises(ValueError, match="az_account specified but az_key is missing"):
            Settings(
                registry_url="localhost:5000",
                registry_repo="test/repo",
                az_account="testaccount"
            )
    
    def test_azure_key_without_account_raises(self):
        """Test that Azure key without account raises ValueError."""
        with pytest.raises(ValueError, match="az_key specified but az_account is missing"):
            Settings(
                registry_url="localhost:5000",
                registry_repo="test/repo",
                az_key="testkey"
            )
    
    def test_no_azure_auth_allowed(self):
        """Test that no Azure auth is allowed (some deployments use ORAS only)."""
        settings = Settings(registry_url="localhost:5000", registry_repo="test/repo")
        assert settings.az_connection_string is None
        assert settings.az_account is None
        assert settings.az_key is None


class TestCreateSettingsFromEnv:
    """Test creating settings from environment variables."""
    
    def setup_method(self):
        """No setup needed - using dependency injection now."""
        pass
    
    def test_minimal_env_settings(self):
        """Test loading with minimal required environment variables."""
        env = {
            "MODELOPS_REGISTRY_URL": "localhost:5000",
            "MODELOPS_REGISTRY_REPO": "test/repo"
        }
        
        with patch.dict(os.environ, env, clear=True):
            settings = create_settings_from_env()
            assert settings.registry_url == "localhost:5000"
            assert settings.registry_insecure is False
            assert settings.registry_user is None
            assert settings.registry_pass is None
            assert settings.http_timeout_s == 30.0
            assert settings.http_retry == 0
    
    def test_full_env_settings_with_connection_string(self):
        """Test loading all settings with Azure connection string."""
        env = {
            "MODELOPS_REGISTRY_URL": "https://myregistry.azurecr.io",
            "MODELOPS_REGISTRY_REPO": "test/repo",
            "MODELOPS_REGISTRY_INSECURE": "true",
            "MODELOPS_REGISTRY_USERNAME": "testuser",
            "MODELOPS_REGISTRY_PASSWORD": "testpass",
            "MODELOPS_HTTP_TIMEOUT": "45.5",
            "MODELOPS_HTTP_RETRY": "3",
            "AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key",
            "MODELOPS_EXT_TIMEOUT": "120.0",
            "MODELOPS_ALLOW_STAT_WITHOUT_SHA": "true"
        }
        
        with patch.dict(os.environ, env, clear=True):
            settings = create_settings_from_env()
            assert settings.registry_url == "https://myregistry.azurecr.io"
            assert settings.registry_insecure is True
            assert settings.registry_user == "testuser"
            assert settings.registry_pass == "testpass"
            assert settings.http_timeout_s == 45.5
            assert settings.http_retry == 3
            assert settings.az_connection_string == "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key"
            assert settings.ext_timeout_s == 120.0
            assert settings.allow_stat_without_sha is True
    
    def test_env_settings_with_account_key(self):
        """Test loading settings with Azure account + key."""
        env = {
            "MODELOPS_REGISTRY_URL": "localhost:5000",
            "MODELOPS_REGISTRY_REPO": "test/repo",
            "AZURE_STORAGE_ACCOUNT": "testaccount",
            "AZURE_STORAGE_KEY": "testkey123=="
        }
        
        with patch.dict(os.environ, env, clear=True):
            settings = create_settings_from_env()
            assert settings.az_account == "testaccount"
            assert settings.az_key == "testkey123=="
            assert settings.az_connection_string is None
    
    def test_missing_registry_url_env_raises(self):
        """Test that missing registry URL environment variable raises ValueError."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="MODELOPS_REGISTRY_URL environment variable is required"):
                create_settings_from_env()
    
    def test_bool_env_parsing(self):
        """Test parsing of boolean environment variables."""
        test_cases = [
            ("true", True),
            ("True", True), 
            ("TRUE", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("False", False),
            ("0", False),
            ("no", False),
            ("off", False),
            ("anything", False),  # Default to False for unknown values
        ]
        
        for env_value, expected in test_cases:
            env = {
                "MODELOPS_REGISTRY_URL": "localhost:5000",
                "MODELOPS_REGISTRY_REPO": "test/repo",
                "MODELOPS_REGISTRY_INSECURE": env_value
            }
            
            # No need to reset cache - using dependency injection now
            with patch.dict(os.environ, env, clear=True):
                settings = create_settings_from_env()
                assert settings.registry_insecure == expected, f"Failed for {env_value}"
    
    def test_numeric_env_parsing(self):
        """Test parsing of numeric environment variables."""
        env = {
            "MODELOPS_REGISTRY_URL": "localhost:5000",
            "MODELOPS_REGISTRY_REPO": "test/repo",
            "MODELOPS_HTTP_TIMEOUT": "25.5",
            "MODELOPS_HTTP_RETRY": "5",
            "MODELOPS_EXT_TIMEOUT": "90.0"
        }
        
        with patch.dict(os.environ, env, clear=True):
            settings = create_settings_from_env()
            assert settings.http_timeout_s == 25.5
            assert settings.http_retry == 5
            assert settings.ext_timeout_s == 90.0
    
    def test_invalid_numeric_env_raises(self):
        """Test that invalid numeric values raise appropriate errors."""
        env = {
            "MODELOPS_REGISTRY_URL": "localhost:5000",
            "MODELOPS_REGISTRY_REPO": "test/repo",
            "MODELOPS_HTTP_TIMEOUT": "not-a-number"
        }
        
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(ValueError):  # From float() conversion
                create_settings_from_env()
    
    def test_no_caching_behavior(self):
        """Test that settings are NOT cached - fresh instance every time."""
        env = {"MODELOPS_REGISTRY_URL": "localhost:5000", "MODELOPS_REGISTRY_REPO": "test/repo"}
        
        with patch.dict(os.environ, env, clear=True):
            # First call loads from env
            settings1 = create_settings_from_env()
            
            # Second call should create new instance
            settings2 = create_settings_from_env()
            
            assert settings1 is not settings2  # Different objects
            assert settings2.registry_url == "localhost:5000"  # Same values
    
    def test_env_change_isolation(self):
        """Test that environment changes are reflected without cache issues."""
        env1 = {"MODELOPS_REGISTRY_URL": "localhost:5000", "MODELOPS_REGISTRY_REPO": "test/repo"}
        env2 = {"MODELOPS_REGISTRY_URL": "localhost:6000", "MODELOPS_REGISTRY_REPO": "test/repo"}
        
        with patch.dict(os.environ, env1, clear=True):
            settings1 = create_settings_from_env()
            assert settings1.registry_url == "localhost:5000"
        
        # Change env - should pick up new values immediately
        with patch.dict(os.environ, env2, clear=True):
            settings2 = create_settings_from_env()
            assert settings2.registry_url == "localhost:6000"
        
        assert settings1 is not settings2  # Different objects