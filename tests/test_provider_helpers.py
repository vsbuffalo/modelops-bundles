"""
Tests for provider helper functions.

Tests the create_provider_from_env() factory function.
"""
from __future__ import annotations

import os
import pytest
from unittest.mock import patch

from modelops_bundles.providers.bundle_content import create_provider_from_env, BundleContentProvider


class TestCreateProviderFromEnv:
    """Test create_provider_from_env() factory function."""
    
    def test_creates_provider_with_real_adapters(self):
        """Test that factory creates provider with real adapters."""
        # Mock environment with required settings
        env = {
            "MODELOPS_REGISTRY_URL": "localhost:5000",
            "MODELOPS_REGISTRY_REPO": "test/modelops-bundles",
            "AZURE_STORAGE_CONNECTION_STRING": "DefaultEndpointsProtocol=https;AccountName=test;AccountKey=key"
        }
        
        # No need to reset cache - using dependency injection now
        
        # Mock the adapter imports to avoid dependency issues
        with patch.dict(os.environ, env, clear=True):
            with patch('modelops_bundles.storage.oras_bundle_registry.OrasBundleRegistry') as mock_registry:
                with patch('modelops_bundles.storage.object_store.AzureExternalAdapter') as mock_azure:
                    
                    provider = create_provider_from_env()
                    
                    # Should return BundleContentProvider
                    assert isinstance(provider, BundleContentProvider)
                    
                    # Should have created both adapters
                    mock_registry.assert_called_once()
                    mock_azure.assert_called_once()
                    
                    # Adapters should receive settings
                    registry_call_args = mock_registry.call_args
                    azure_call_args = mock_azure.call_args
                    
                    assert 'settings' in azure_call_args.kwargs
                    assert len(registry_call_args.args) == 1  # make_registry takes settings as first arg
    
    def test_raises_on_missing_registry_config(self):
        """Test that factory raises when registry config missing."""
        # No need to reset cache - using dependency injection now
        
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="MODELOPS_REGISTRY_URL environment variable is required"):
                create_provider_from_env()
    
    def test_raises_on_missing_azure_config(self):
        """Test that factory raises when Azure config missing."""
        env = {
            "MODELOPS_REGISTRY_URL": "localhost:5000",
            "MODELOPS_REGISTRY_REPO": "test/modelops-bundles"
            # No Azure config
        }
        
        # No need to reset cache - using dependency injection now
        
        with patch.dict(os.environ, env, clear=True):
            with patch('modelops_bundles.storage.oras_bundle_registry.OrasBundleRegistry'):
                with pytest.raises(ValueError, match="Azure authentication not configured"):
                    create_provider_from_env()