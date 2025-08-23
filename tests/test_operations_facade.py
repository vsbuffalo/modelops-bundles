"""
Test Operations facade wiring and integration.

Validates that the Operations facade correctly orchestrates runtime calls,
applies configuration policies, and integrates with providers as expected.
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, call

import pytest
from modelops_contracts.artifacts import BundleRef, ResolvedBundle

from modelops_bundles.operations import Operations, OpsConfig
from modelops_bundles.runtime import MaterializeResult
from tests.fakes.fake_provider import FakeProvider
from tests.storage.fakes.fake_oci_registry import FakeOciRegistry


class TestOperationsFacade:
    """Test Operations facade orchestration."""
    
    def test_facade_initialization(self):
        """Test facade initializes with correct configuration."""
        config = OpsConfig(ci=True, cache=False, zstd_level=10)
        provider = FakeProvider()
        registry = FakeOciRegistry()
        
        ops = Operations(config=config, provider=provider, registry=registry)
        
        assert ops.cfg is config
        assert ops.provider is provider
        assert ops.registry is registry

    def test_resolve_delegates_to_runtime(self):
        """Test resolve method delegates to runtime with correct parameters."""
        config = OpsConfig(cache=True)
        registry = FakeOciRegistry()
        ops = Operations(config=config, registry=registry)
        
        ref = BundleRef(name="test/bundle", version="v1.0.0")
        
        with patch('modelops_bundles.operations.facade._resolve') as mock_resolve:
            mock_resolve.return_value = Mock(spec=ResolvedBundle)
            
            result = ops.resolve(ref)
            
            mock_resolve.assert_called_once_with(ref, registry=registry, cache=True)
            assert result is mock_resolve.return_value

    def test_resolve_respects_cache_config(self):
        """Test resolve method respects cache configuration."""
        config = OpsConfig(cache=False)
        registry = FakeOciRegistry()
        ops = Operations(config=config, registry=registry)
        
        ref = BundleRef(name="test/bundle", version="v1.0.0")
        
        with patch('modelops_bundles.operations.facade._resolve') as mock_resolve:
            ops.resolve(ref)
            mock_resolve.assert_called_once_with(ref, registry=registry, cache=False)

    def test_materialize_requires_provider(self):
        """Test materialize method requires provider to be configured."""
        config = OpsConfig()
        registry = FakeOciRegistry()
        ops = Operations(config=config, provider=None, registry=registry)
        
        ref = BundleRef(name="test/bundle", version="v1.0.0")
        
        with pytest.raises(AssertionError, match="Provider required for materialize"):
            ops.materialize(ref, "/tmp/dest")

    def test_materialize_delegates_to_runtime(self):
        """Test materialize method delegates to runtime with all parameters."""
        config = OpsConfig()
        provider = FakeProvider()
        registry = FakeOciRegistry()
        ops = Operations(config=config, provider=provider, registry=registry)
        
        ref = BundleRef(name="test/bundle", version="v1.0.0")
        
        with patch('modelops_bundles.operations.facade._materialize') as mock_materialize:
            mock_resolved = Mock(spec=ResolvedBundle)
            mock_result = MaterializeResult(bundle=mock_resolved, selected_role="runtime", dest_path="/tmp/dest")
            mock_materialize.return_value = mock_result
            
            result = ops.materialize(
                ref=ref,
                dest="/tmp/dest",
                role="runtime",
                overwrite=True,
                prefetch_external=True
            )
            
            mock_materialize.assert_called_once_with(
                ref=ref,
                dest="/tmp/dest", 
                role="runtime",
                overwrite=True,
                prefetch_external=True,
                provider=provider,
                registry=registry,
                            )
            assert result is mock_result
            assert result.bundle is mock_resolved
            assert result.selected_role == "runtime"

    def test_pull_is_alias_for_materialize(self):
        """Test pull method is correct alias for materialize."""
        config = OpsConfig()
        provider = FakeProvider()
        registry = FakeOciRegistry()
        ops = Operations(config=config, provider=provider, registry=registry)
        
        ref = BundleRef(name="test/bundle", version="v1.0.0")
        
        with patch.object(ops, 'materialize') as mock_materialize:
            mock_resolved = Mock(spec=ResolvedBundle)
            mock_result = MaterializeResult(bundle=mock_resolved, selected_role="runtime", dest_path="/tmp/dest")
            mock_materialize.return_value = mock_result
            
            result = ops.pull(
                ref=ref,
                dest="/tmp/dest",
                role="runtime",
                overwrite=False,
                prefetch_external=False
            )
            
            mock_materialize.assert_called_once_with(
                ref=ref,
                dest="/tmp/dest",
                role="runtime", 
                overwrite=False,
                prefetch_external=False
            )
            assert result is mock_result

    def test_export_delegates_to_export_module(self):
        """Test export method delegates to export module with config."""
        config = OpsConfig(zstd_level=15)
        registry = FakeOciRegistry()
        ops = Operations(config=config, registry=registry)
        
        with patch('modelops_bundles.export.write_deterministic_archive') as mock_export:
            ops.export("/src/dir", "/output.tar.zst", include_external=True)
            
            mock_export.assert_called_once_with(
                src_dir="/src/dir",
                out_path="/output.tar.zst", 
                include_external=True,
                zstd_level=15
            )

    def test_export_uses_default_zstd_level(self):
        """Test export uses default zstd level from config."""
        config = OpsConfig()  # Default zstd_level=19
        registry = FakeOciRegistry()
        ops = Operations(config=config, registry=registry)
        
        with patch('modelops_bundles.export.write_deterministic_archive') as mock_export:
            ops.export("/src/dir", "/output.tar")
            
            mock_export.assert_called_once_with(
                src_dir="/src/dir",
                out_path="/output.tar",
                include_external=False,
                zstd_level=19
            )

    def test_stubbed_commands_return_descriptive_messages(self):
        """Test stubbed commands return descriptive messages."""
        config = OpsConfig()
        registry = FakeOciRegistry()
        ops = Operations(config=config, registry=registry)
        
        assert "Scanned /work/dir (stub)" == ops.scan("/work/dir")
        assert "Storage plan for /work/dir (stub)" == ops.plan("/work/dir")
        assert "Storage plan for /work/dir with external preview (stub)" == ops.plan("/work/dir", external_preview=True)
        assert "Diff for bundle:v1.0.0 (stub)" == ops.diff("bundle:v1.0.0")
        assert "Pushed /work/dir (stub)" == ops.push("/work/dir")
        assert "Pushed /work/dir with patch bump (stub)" == ops.push("/work/dir", bump="patch")

    def test_config_policies_are_applied(self):
        """Test that configuration policies are consistently applied."""
        # Test CI mode configuration
        config = OpsConfig(ci=True, cache=False, zstd_level=5, human=True)
        registry = FakeOciRegistry()
        ops = Operations(config=config, registry=registry)
        
        assert ops.cfg.ci is True
        assert ops.cfg.cache is False 
        assert ops.cfg.zstd_level == 5
        assert ops.cfg.human is True

    def test_provider_injection_for_testing(self):
        """Test provider injection enables testing with fakes."""
        fake_provider = FakeProvider()
        config = OpsConfig()
        registry = FakeOciRegistry()
        ops = Operations(config=config, provider=fake_provider, registry=registry)
        
        # Should be able to call provider-dependent methods
        ref = BundleRef(name="test/bundle", version="v1.0.0")
        
        with patch('modelops_bundles.operations.facade._materialize') as mock_materialize:
            mock_resolved = Mock(spec=ResolvedBundle)
            mock_result = MaterializeResult(bundle=mock_resolved, selected_role="default", dest_path="/tmp/dest")
            mock_materialize.return_value = mock_result
            
            ops.materialize(ref, "/tmp/dest")
            
            # Verify fake provider was passed to runtime
            mock_materialize.assert_called_once()
            args, kwargs = mock_materialize.call_args
            assert kwargs['provider'] is fake_provider


class TestOpsConfig:
    """Test OpsConfig configuration object."""
    
    def test_default_values(self):
        """Test OpsConfig has sensible defaults."""
        config = OpsConfig()
        
        assert config.ci is False
        assert config.cache is True
        assert config.zstd_level == 19
        assert config.human is True

    def test_custom_values(self):
        """Test OpsConfig accepts custom values."""
        config = OpsConfig(
            ci=True,
            cache=False,
            zstd_level=10,
            human=False
        )
        
        assert config.ci is True
        assert config.cache is False
        assert config.zstd_level == 10
        assert config.human is False

    def test_frozen_dataclass(self):
        """Test OpsConfig is immutable."""
        config = OpsConfig()
        
        with pytest.raises(Exception):  # FrozenInstanceError in Python
            config.ci = True

    def test_configuration_consistency(self):
        """Test configuration values are used consistently."""
        config = OpsConfig(cache=False, zstd_level=5)
        provider = FakeProvider()
        registry = FakeOciRegistry()
        ops = Operations(config=config, provider=provider, registry=registry)
        
        # Verify cache setting is passed through
        with patch('modelops_bundles.operations.facade._resolve') as mock_resolve:
            ref = BundleRef(name="test/bundle", version="v1.0.0")
            ops.resolve(ref)
            mock_resolve.assert_called_once_with(ref, registry=registry, cache=False)
        
        # Verify zstd level is passed through
        with patch('modelops_bundles.export.write_deterministic_archive') as mock_export:
            ops.export("/src", "/out.tar.zst")
            args, kwargs = mock_export.call_args
            assert kwargs['zstd_level'] == 5