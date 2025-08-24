"""
Tests for CLI bundle reference parsing.

Tests the _parse_bundle_ref function to ensure proper handling of:
- name:version format with slashes allowed
- name@sha256:digest format
- bare digest rejection
- Windows absolute paths
- Complex valid names
"""
import os
import pytest

from modelops_contracts.artifacts import BundleRef
from modelops_bundles.cli import _parse_bundle_ref


class TestBundleRefParsing:
    """Test bundle reference parsing logic."""
    
    def test_bare_digest_rejected(self):
        """Test that bare digests are rejected."""
        with pytest.raises(ValueError, match="Bare digests not supported"):
            _parse_bundle_ref("sha256:abc123def456789abcdef0123456789abcdef0123456789abcdef0123456789a")
        
        with pytest.raises(ValueError, match="Bare digests not supported"):
            _parse_bundle_ref("@sha256:abc123def456789abcdef0123456789abcdef0123456789abcdef0123456789a")
    
    def test_name_at_digest_accepted(self):
        """Test that name@sha256:digest format is accepted."""
        ref = _parse_bundle_ref("my-bundle@sha256:abc123def456789abcdef0123456789abcdef0123456789abcdef0123456789a")
        assert ref.name == "my-bundle"
        assert ref.digest == "sha256:abc123def456789abcdef0123456789abcdef0123456789abcdef0123456789a"
    
    def test_name_with_slashes_accepted(self):
        """Test that names with slashes are accepted."""
        ref = _parse_bundle_ref("org/proj/bundle:1.0.0")
        assert ref.name == "org/proj/bundle"
        assert ref.version == "1.0.0"
    
    def test_simple_name_version(self):
        """Test simple name:version format."""
        ref = _parse_bundle_ref("simple-bundle:2.1.0")
        assert ref.name == "simple-bundle"
        assert ref.version == "2.1.0"
    
    def test_windows_absolute_path(self):
        """Test Windows absolute path detection."""
        ref = _parse_bundle_ref("C:\\Users\\test\\bundle")
        assert ref.local_path == "C:\\Users\\test\\bundle"
        assert ref.name is None
        assert ref.version is None
    
    def test_unix_absolute_path(self):
        """Test Unix absolute path detection."""
        ref = _parse_bundle_ref("/home/user/bundle")
        assert ref.local_path == "/home/user/bundle"
        assert ref.name is None
        assert ref.version is None
    
    def test_relative_paths(self):
        """Test relative path detection."""
        ref = _parse_bundle_ref("./bundle")
        assert ref.local_path == "./bundle"
        
        ref = _parse_bundle_ref("../bundle")
        assert ref.local_path == "../bundle"
    
    def test_complex_valid_names(self):
        """Test complex but valid bundle names."""
        # Name with hyphens and slashes (dots/underscores not allowed by BundleRef validation)
        ref = _parse_bundle_ref("foo-bar/baz-qux:1.2.3")
        assert ref.name == "foo-bar/baz-qux"
        assert ref.version == "1.2.3"
        
        # Name with multiple slashes
        ref = _parse_bundle_ref("org/team/project/bundle:v2.0.0-alpha")
        assert ref.name == "org/team/project/bundle"
        assert ref.version == "v2.0.0-alpha"
        
        # Name with hyphens and numbers
        ref = _parse_bundle_ref("my-org-123/bundle-456:1.0.0-beta.1")
        assert ref.name == "my-org-123/bundle-456"
        assert ref.version == "1.0.0-beta.1"
    
    def test_digest_case_normalization(self):
        """Test that digests are normalized to lowercase."""
        ref = _parse_bundle_ref("bundle@sha256:ABC123DEF456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789A")
        assert ref.name == "bundle"
        assert ref.digest == "sha256:abc123def456789abcdef0123456789abcdef0123456789abcdef0123456789a"
    
    def test_version_with_colons(self):
        """Test version strings that contain colons."""
        # Only split on first colon
        ref = _parse_bundle_ref("bundle:v1.0:special")
        assert ref.name == "bundle"
        assert ref.version == "v1.0:special"
    
    def test_invalid_formats_rejected(self):
        """Test that invalid formats are rejected."""
        with pytest.raises(ValueError, match="Invalid bundle reference"):
            _parse_bundle_ref("")
        
        with pytest.raises(ValueError, match="Invalid bundle reference"):
            _parse_bundle_ref("   ")
        
        with pytest.raises(ValueError, match="Invalid bundle reference"):
            _parse_bundle_ref("no-colon-no-path-no-digest")
    
    def test_path_detection_with_os_isabs(self):
        """Test that os.path.isabs is used correctly for path detection."""
        # Mock different OS behaviors if needed
        if os.name == 'nt':  # Windows
            ref = _parse_bundle_ref("D:\\workspace\\bundle")
            assert ref.local_path == "D:\\workspace\\bundle"
        else:  # Unix-like
            ref = _parse_bundle_ref("/workspace/bundle")
            assert ref.local_path == "/workspace/bundle"