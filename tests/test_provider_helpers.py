"""
Tests for provider helper functions.

Tests the default_provider_from_env() factory function.
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import patch

from modelops_bundles.providers.oras_external import default_provider_from_env, OrasExternalProvider


class TestDefaultProviderFromEnv:
    """Test default_provider_from_env() factory function."""
    
    def test_creates_provider_with_real_adapters(self):
        """Test that factory creates provider with real adapters."""
        # Mock environment with required settings
        env = {
            "MODEL_OPS_REGISTRY_URL": "localhost:5000",
            "MODEL_OPS_REGISTRY_REPO": "test/modelops-bundles",
            "AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key"
        }
        
        from modelops_bundles.settings import reset_settings_cache
        reset_settings_cache()
        
        # Mock the adapter imports to avoid dependency issues
        with patch.dict(os.environ, env, clear=True):
            with patch('modelops_bundles.storage.oras.OrasAdapter') as mock_oras:
                with patch('modelops_bundles.storage.object_store.AzureExternalAdapter') as mock_azure:
                    
                    provider = default_provider_from_env()
                    
                    # Should return OrasExternalProvider
                    assert isinstance(provider, OrasExternalProvider)
                    
                    # Should have created both adapters
                    mock_oras.assert_called_once()
                    mock_azure.assert_called_once()
                    
                    # Adapters should receive settings
                    oras_call_args = mock_oras.call_args
                    azure_call_args = mock_azure.call_args
                    
                    assert 'settings' in oras_call_args.kwargs
                    assert 'settings' in azure_call_args.kwargs
    
    def test_raises_on_missing_registry_config(self):
        """Test that factory raises when registry config missing."""
        from modelops_bundles.settings import reset_settings_cache
        reset_settings_cache()
        
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="MODEL_OPS_REGISTRY_URL environment variable is required"):
                default_provider_from_env()
    
    def test_raises_on_missing_azure_config(self):
        """Test that factory raises when Azure config missing."""
        env = {
            "MODEL_OPS_REGISTRY_URL": "localhost:5000",
            "MODEL_OPS_REGISTRY_REPO": "test/modelops-bundles"
            # No Azure config
        }
        
        from modelops_bundles.settings import reset_settings_cache
        reset_settings_cache()
        
        with patch.dict(os.environ, env, clear=True):
            with patch('modelops_bundles.storage.oras.OrasAdapter'):
                with pytest.raises(ValueError, match="Azure authentication not configured"):
                    default_provider_from_env()