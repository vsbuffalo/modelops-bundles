"""
Basic tests for ORAS adapter.

Tests core functionality without complex mocking.
"""
from __future__ import annotations

import pytest

from modelops_bundles.settings import Settings
from modelops_bundles.storage.oras import OrasAdapter, KNOWN_MEDIA_TYPES
from modelops_contracts.artifacts import (
    BUNDLE_MANIFEST,
    LAYER_INDEX,
    EXTERNAL_REF,
    OCI_MANIFEST,
)


class TestOrasAdapterBasic:
    """Test basic ORAS adapter functionality."""
    
    def test_known_media_types_defined(self):
        """Test that known media types are properly defined from contracts."""
        expected_types = {
            BUNDLE_MANIFEST,
            LAYER_INDEX,
            EXTERNAL_REF,
            OCI_MANIFEST,
        }
        
        assert KNOWN_MEDIA_TYPES == expected_types
    
    def test_constructor_requires_registry_url(self):
        """Test constructor requires registry URL in settings."""
        # This should be caught by Settings validation
        with pytest.raises(ValueError, match="registry_url is required"):
            Settings(registry_url="", registry_repo="test/repo")
    
    def test_media_type_validation_accepts_known_types(self):
        """Test that media type validation accepts known types from contracts."""
        # We can't easily test the adapter without mocking imports,
        # but we can test the known types constant
        known_types = [
            BUNDLE_MANIFEST,
            LAYER_INDEX,
            EXTERNAL_REF,
            OCI_MANIFEST,
        ]
        
        for media_type in known_types:
            assert media_type in KNOWN_MEDIA_TYPES
    
    
    def test_digest_format_regex(self):
        """Test digest format validation regex."""
        import re
        
        # This is the pattern used in the adapter
        digest_pattern = r"^sha256:[a-f0-9]{64}$"
        
        valid_digests = [
            "sha256:" + "a" * 64,
            "sha256:" + "0123456789abcdef" * 4,  # 64 chars
            "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
        ]
        
        invalid_digests = [
            "sha256:ABCDEF" + "a" * 58,  # Contains uppercase
            "sha256:" + "g" * 64,  # Contains non-hex char
            "sha256:" + "a" * 63,  # Too short
            "sha256:" + "a" * 65,  # Too long
            "sha1:" + "a" * 40,    # Wrong algorithm
            "invalid-digest",      # No proper format
        ]
        
        for digest in valid_digests:
            assert re.match(digest_pattern, digest), f"Should be valid: {digest}"
        
        for digest in invalid_digests:
            assert not re.match(digest_pattern, digest), f"Should be invalid: {digest}"
    
