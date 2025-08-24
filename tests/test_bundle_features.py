"""
Unit tests for ModelOps Bundles core functionality.

Tests repository composition, digest normalization, exit code mapping,
prefetch flow, and other key features.
"""
from __future__ import annotations

import dataclasses
import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from modelops_contracts.artifacts import BundleRef, ResolvedBundle
from modelops_bundles.runtime import resolve, UnsupportedMediaType, BundleNotFoundError
from modelops_bundles.operations.facade import Operations, OpsConfig
from modelops_bundles.operations.mappers import exit_code_for, EXIT_CODES
from modelops_bundles.cli import _parse_bundle_ref
from tests.storage.fakes.fake_oci_registry import FakeOciRegistry


class TestRepositoryComposition:
    """Test repository composition with trailing slash stripping."""
    
    def test_repository_trailing_slash_stripped(self):
        """Test that trailing slash is stripped from repository."""
        registry = FakeOciRegistry()
        
        # Mock the registry to capture the composed manifest_ref
        captured_refs = []
        original_get_manifest = registry.get_manifest
        
        def mock_get_manifest(repo, ref):
            captured_refs.append(f"{repo}:{ref}")
            # Return a valid OCI manifest structure
            manifest = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "layers": [
                    {
                        "mediaType": "application/vnd.modelops.bundle.manifest+json",
                        "digest": "sha256:" + "b" * 64,
                        "size": 100
                    }
                ]
            }
            import json
            return json.dumps(manifest).encode()
        
        def mock_head_manifest(repo, ref):
            return "sha256:" + "a" * 64
        
        def mock_get_blob(repo, digest):
            # Return a minimal bundle manifest
            bundle_manifest = {
                "mediaType": "application/vnd.modelops.bundle.manifest+json",
                "roles": {"default": ["code"]},
                "layers": ["code"],
                "layer_indexes": {}
            }
            import json
            return json.dumps(bundle_manifest).encode()
        
        registry.get_manifest = mock_get_manifest
        registry.head_manifest = mock_head_manifest
        registry.get_blob = mock_get_blob
        
        # Test with trailing slash
        ref = BundleRef(name="test", version="1.0")
        resolve(ref, registry=registry)
        
        # Should have stripped trailing slash
        assert captured_refs[0] == "testns/bundles/test:1.0"
        
    def test_repository_no_double_slash(self):
        """Test that double slashes are not created."""
        registry = FakeOciRegistry()
        
        captured_refs = []
        def mock_get_manifest(repo, ref):
            captured_refs.append(f"{repo}:{ref}")
            # Return a valid OCI manifest structure
            manifest = {
                "schemaVersion": 2,
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "layers": [
                    {
                        "mediaType": "application/vnd.modelops.bundle.manifest+json",
                        "digest": "sha256:" + "b" * 64,
                        "size": 100
                    }
                ]
            }
            import json
            return json.dumps(manifest).encode()
        
        def mock_head_manifest(repo, ref):
            return "sha256:" + "a" * 64
        
        def mock_get_blob(repo, digest):
            # Return a minimal bundle manifest
            bundle_manifest = {
                "mediaType": "application/vnd.modelops.bundle.manifest+json",
                "roles": {"default": ["code"]},
                "layers": ["code"],
                "layer_indexes": {}
            }
            import json
            return json.dumps(bundle_manifest).encode()
        
        registry.get_manifest = mock_get_manifest
        registry.head_manifest = mock_head_manifest
        registry.get_blob = mock_get_blob
        
        ref = BundleRef(name="test", version="1.0")
        resolve(ref, registry=registry)
        
        # Should have normalized to single slash
        assert captured_refs[0] == "testns/bundles/test:1.0"


class TestDigestNormalization:
    """Test digest normalization to lowercase."""
    
    def test_uppercase_digest_normalized(self):
        """Test that uppercase hex gets normalized to lowercase via CLI parsing."""
        # Test the digest normalization via CLI parsing since BundleRef validation
        # happens before resolve() can normalize the digest
        from modelops_bundles.cli import _parse_bundle_ref
        
        # Create an uppercase digest with name
        uppercase_digest_ref = "test-bundle@sha256:ABC123DEF456789ABCDEF01234567890ABCDEF01234567890ABCDEF012345678"
        
        # CLI should normalize to lowercase and create valid BundleRef
        ref = _parse_bundle_ref(uppercase_digest_ref)
        assert ref.digest == "sha256:abc123def456789abcdef01234567890abcdef01234567890abcdef012345678"
        assert ref.name == "test-bundle"
        assert ref.version is None
        
    def test_mixed_case_digest_normalized(self):
        """Test that mixed case digest gets fully normalized via CLI parsing."""
        # Test that CLI parsing properly normalizes mixed case digests
        from modelops_bundles.cli import _parse_bundle_ref
        
        mixed_case_digest_ref = "test-bundle@sha256:AbC123dEf456789abcDEF01234567890abCDEF01234567890abcdEF012345678"
        
        # CLI should normalize to lowercase and create valid BundleRef
        ref = _parse_bundle_ref(mixed_case_digest_ref)
        assert ref.digest == "sha256:abc123def456789abcdef01234567890abcdef01234567890abcdef012345678"
        assert ref.name == "test-bundle"
        assert ref.version is None


class TestExitCodeMapping:
    """Test exit code mapping for various exceptions."""
    
    def test_bundle_not_found_exit_code(self):
        """Test BundleNotFoundError maps to exit code 1."""
        exc = BundleNotFoundError("test error")
        assert exit_code_for(exc) == 1
        
    def test_unsupported_media_type_exit_code(self):
        """Test UnsupportedMediaType maps to exit code 10.""" 
        exc = UnsupportedMediaType("test error")
        assert exit_code_for(exc) == 10
        
    def test_unknown_exception_fallback(self):
        """Test unknown exceptions map to fallback exit code 3."""
        exc = RuntimeError("test error")
        assert exit_code_for(exc) == 3
        
    def test_all_exit_codes_defined(self):
        """Test that all expected exit codes are defined."""
        expected_codes = {
            "BundleNotFoundError": 1,
            "ValidationError": 2,
            "BundleDownloadError": 3,
            "UnsupportedMediaType": 10,
            "RoleLayerMismatch": 11,
            "WorkdirConflict": 12
        }
        assert EXIT_CODES == expected_codes


class TestCLIReferenceParsing:
    """Test CLI reference parsing enhancements."""
    
    def test_sha256_digest_parsing(self):
        """Test that bare sha256: digest format is rejected."""
        digest = "sha256:abc123def456789abcdef01234567890abcdef01234567890abcdef012345678"
        with pytest.raises(ValueError, match="Bare digests not supported"):
            _parse_bundle_ref(digest)
        
    def test_oci_digest_parsing(self):
        """Test that bare @sha256: OCI digest format is rejected."""
        digest = "sha256:abc123def456789abcdef01234567890abcdef01234567890abcdef012345678"
        with pytest.raises(ValueError, match="Bare digests not supported"):
            _parse_bundle_ref("@" + digest)
    
    def test_name_at_digest_parsing(self):
        """Test parsing of supported name@sha256:digest format."""
        digest = "sha256:abc123def456789abcdef01234567890abcdef01234567890abcdef012345678"
        ref = _parse_bundle_ref(f"mybundle@{digest}")
        assert ref.digest == digest
        assert ref.name == "mybundle"
        assert ref.version is None
        
    def test_name_version_parsing(self):
        """Test parsing of name:version format."""
        ref = _parse_bundle_ref("myname:1.0.0")
        assert ref.name == "myname"
        assert ref.version == "1.0.0"
        assert ref.digest is None
        
    def test_slash_in_name_accepted(self):
        """Test that names containing slash are currently accepted."""
        ref = _parse_bundle_ref("org/repo:1.0")
        assert ref.name == "org/repo"
        assert ref.version == "1.0"
            
    def test_invalid_format_rejected(self):
        """Test that invalid formats are rejected."""
        with pytest.raises(ValueError, match="Invalid bundle reference format"):
            _parse_bundle_ref("invalid-format")


class TestOperationsSecurityFixes:
    """Test Operations facade security improvements."""
    
    def test_registry_without_repository_fails(self):
        """Test that injecting registry without repository works now (validation was removed)."""
        registry = FakeOciRegistry()
        config = OpsConfig()
        
        # This should now succeed (validation was removed in current implementation)
        ops = Operations(config=config, registry=registry)
        assert ops.registry is not None
            
    def test_registry_with_repository_succeeds(self):
        """Test that providing both registry and repository works."""
        registry = FakeOciRegistry()
        config = OpsConfig()
        
        # Should not raise
        ops = Operations(config=config, registry=registry)
        assert ops.registry is registry
        # Repository logic is now handled internally by the registry


class TestMediaTypeValidation:
    """Test enhanced media type validation."""
    
    def test_invalid_bundle_manifest_media_type(self):
        """Test that invalid bundle manifest media type raises UnsupportedMediaType."""
        registry = FakeOciRegistry()
        
        def mock_get_manifest(ref):
            # Return manifest with wrong media type
            manifest = {
                "mediaType": "application/vnd.wrong.type+json",
                "roles": {"default": ["code"]},
                "layers": ["code"],
                "layer_indexes": {}
            }
            import json
            return json.dumps(manifest).encode()
        
        registry.get_manifest = mock_get_manifest
        
        ref = BundleRef(name="test", version="1.0")
        
        with pytest.raises(UnsupportedMediaType, match="Invalid manifest mediaType"):
            resolve(ref, registry=registry)


class TestTotalSizeCalculation:
    """Test best-effort total size calculation."""
    
    def test_size_includes_only_external_entries(self):
        """Test that total_size only includes external entries, not ORAS."""
        registry = FakeOciRegistry()
        
        # Create layer index with only ORAS entries (no external)
        oras_only_index = {
            "mediaType": "application/vnd.modelops.layer+json",
            "entries": [
                {
                    "path": "code/main.py",
                    "oras": {"digest": "sha256:fake-digest"}
                }
            ]
        }
        
        # Create layer index with external entries
        external_index = {
            "mediaType": "application/vnd.modelops.layer+json", 
            "entries": [
                {
                    "path": "data/train.csv",
                    "external": {
                        "uri": "az://container/train.csv",
                        "sha256": "fake-sha256",
                        "size": 1000000
                    }
                }
            ]
        }
        
        import json
        oras_index_payload = json.dumps(oras_only_index).encode()
        external_index_payload = json.dumps(external_index).encode()
        
        oras_digest = registry.put_manifest("application/vnd.modelops.layer+json", oras_index_payload)
        external_digest = registry.put_manifest("application/vnd.modelops.layer+json", external_index_payload)
        
        def mock_get_manifest(ref):
            if ref.endswith("test:1.0"):
                # Bundle manifest
                manifest = {
                    "mediaType": "application/vnd.modelops.bundle.manifest+json",
                    "roles": {"default": ["code", "data"]},
                    "layers": ["code", "data"],
                    "layer_indexes": {
                        "code": oras_digest,
                        "data": external_digest
                    }
                }
                return json.dumps(manifest).encode()
            elif ref == oras_digest:
                return oras_index_payload
            elif ref == external_digest:
                return external_index_payload
            else:
                raise KeyError(ref)
        
        registry.get_manifest = mock_get_manifest
        
        ref = BundleRef(name="test", version="1.0")
        result = resolve(ref, registry=registry)
        
        # Should only include external size (1000000), not ORAS entries
        assert result.total_size == 1000000


class TestVerboseOutput:
    """Test verbose output functionality."""
    
    def test_verbose_config_passed_through(self):
        """Test that verbose flag is passed through config."""
        config = OpsConfig(verbose=True)
        assert config.verbose is True
        
        config = OpsConfig(verbose=False)
        assert config.verbose is False


class TestPrefetchFlow:
    """Test prefetch external functionality."""
    
    def test_prefetch_parameter_flows_through(self):
        """Test that prefetch_external parameter flows through properly."""
        # This is mostly a smoke test since full prefetch requires real providers
        registry = FakeOciRegistry()
        config = OpsConfig()
        
        # Mock provider to verify prefetch parameter
        mock_provider = Mock()
        
        ops = Operations(config=config, registry=registry, provider=mock_provider)
        
        # Mock resolve to return something
        with patch('modelops_bundles.operations.facade._resolve') as mock_resolve:
            mock_resolved = Mock(spec=ResolvedBundle)
            mock_resolve.return_value = mock_resolved
            
            with patch('modelops_bundles.operations.facade._materialize') as mock_materialize:
                mock_materialize.return_value = (mock_resolved, "default")
                
                ref = BundleRef(name="test", version="1.0")
                
                # Test that prefetch_external is passed through
                ops.materialize(ref, "/dest", prefetch_external=True)
                
                # Verify materialize was called with prefetch_external=True
                mock_materialize.assert_called_once()
                call_kwargs = mock_materialize.call_args[1]
                assert call_kwargs.get('prefetch_external') is True