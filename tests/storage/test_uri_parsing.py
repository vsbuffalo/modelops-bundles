"""
Tests for URI parsing utilities.

Tests the parse_external_uri function with valid and invalid URIs,
edge cases, and security validation.
"""
from __future__ import annotations

import pytest

from modelops_bundles.storage.uri import parse_external_uri, ParsedURI


class TestParseExternalURI:
    """Test parse_external_uri function."""
    
    def test_valid_azure_uri(self):
        """Test parsing valid Azure blob storage URIs."""
        uri = "az://mycontainer/data/file.csv"
        result = parse_external_uri(uri)
        
        assert result.scheme == "az"
        assert result.container_or_bucket == "mycontainer"
        assert result.key == "data/file.csv"
        assert result.original == uri
    
    def test_valid_s3_uri(self):
        """Test parsing valid S3 URIs."""
        uri = "s3://mybucket/models/model.pkl"
        result = parse_external_uri(uri)
        
        assert result.scheme == "s3"
        assert result.container_or_bucket == "mybucket"
        assert result.key == "models/model.pkl"
        assert result.original == uri
    
    def test_valid_gcs_uri(self):
        """Test parsing valid Google Cloud Storage URIs."""
        uri = "gs://my-bucket/deep/nested/path/file.json"
        result = parse_external_uri(uri)
        
        assert result.scheme == "gs"
        assert result.container_or_bucket == "my-bucket"
        assert result.key == "deep/nested/path/file.json"
        assert result.original == uri
    
    def test_minimal_valid_uri(self):
        """Test parsing minimal valid URI with single-char components."""
        uri = "az://a/b"
        result = parse_external_uri(uri)
        
        assert result.scheme == "az"
        assert result.container_or_bucket == "a"
        assert result.key == "b"
        assert result.original == uri
    
    def test_empty_uri_raises(self):
        """Test that empty URI raises ValueError."""
        with pytest.raises(ValueError, match="URI cannot be empty"):
            parse_external_uri("")
    
    def test_invalid_scheme_raises(self):
        """Test that invalid scheme raises ValueError."""
        with pytest.raises(ValueError, match="Invalid URI format"):
            parse_external_uri("invalid://container/key")
        
        with pytest.raises(ValueError, match="Invalid URI format"):
            parse_external_uri("http://container/key")
    
    def test_missing_scheme_raises(self):
        """Test that missing scheme raises ValueError."""
        with pytest.raises(ValueError, match="Invalid URI format"):
            parse_external_uri("container/key")
        
        with pytest.raises(ValueError, match="Invalid URI format"):
            parse_external_uri("//container/key")
    
    def test_missing_container_raises(self):
        """Test that missing container raises ValueError."""
        with pytest.raises(ValueError, match="URI missing key part"):
            parse_external_uri("az://containeronly")
    
    def test_empty_container_raises(self):
        """Test that empty container name raises ValueError."""
        with pytest.raises(ValueError, match="URI path cannot start with '/'"):
            parse_external_uri("az:///key")
    
    def test_empty_after_scheme_raises(self):
        """Test that empty content after scheme raises ValueError."""
        with pytest.raises(ValueError, match="Invalid URI format"):
            parse_external_uri("az://")
    
    def test_empty_key_raises(self):
        """Test that empty key raises ValueError."""
        with pytest.raises(ValueError, match="Key/path cannot be empty"):
            parse_external_uri("az://container/")
    
    def test_path_traversal_rejected(self):
        """Test that path traversal attempts are rejected."""
        with pytest.raises(ValueError, match="URI contains path traversal"):
            parse_external_uri("az://container/../evil")
        
        with pytest.raises(ValueError, match="URI contains path traversal"):
            parse_external_uri("s3://bucket/good/../evil")
        
        with pytest.raises(ValueError, match="URI contains path traversal"):
            parse_external_uri("gs://bucket/path/../../../etc/passwd")
    
    def test_backslashes_rejected(self):
        """Test that backslashes are rejected."""
        with pytest.raises(ValueError, match="URI contains backslashes"):
            parse_external_uri("az://container\\file.txt")
        
        with pytest.raises(ValueError, match="URI contains backslashes"):
            parse_external_uri("s3://bucket/path\\to\\file")
    
    def test_leading_slash_rejected(self):
        """Test that leading slashes after scheme are rejected."""
        with pytest.raises(ValueError, match="URI path cannot start with '/'"):
            parse_external_uri("az:///container/key")
        
        with pytest.raises(ValueError, match="URI path cannot start with '/'"):
            parse_external_uri("s3:////bucket/key")
    
    def test_complex_valid_paths(self):
        """Test parsing URIs with complex but valid paths."""
        # Underscores, hyphens, numbers
        uri = "az://my-container_123/data_2023/model-v1.2.pkl"
        result = parse_external_uri(uri)
        assert result.container_or_bucket == "my-container_123"
        assert result.key == "data_2023/model-v1.2.pkl"
        
        # Dots in filename (but not .. traversal)
        uri = "s3://bucket/file.tar.gz"
        result = parse_external_uri(uri)
        assert result.key == "file.tar.gz"
        
        # Multiple forward slashes in path
        uri = "gs://bucket/very/deep/nested/structure/file.json"
        result = parse_external_uri(uri)
        assert result.key == "very/deep/nested/structure/file.json"
    
    def test_original_preserved_for_error_messages(self):
        """Test that original URI is preserved for error context."""
        uri = "az://test-container/some/long/path/to/file.data"
        result = parse_external_uri(uri)
        assert result.original == uri
        
        # Test it's used in error messages by triggering an error
        bad_uri = "az://container/../evil"
        try:
            parse_external_uri(bad_uri)
            assert False, "Should have raised"
        except ValueError as e:
            assert bad_uri in str(e)
    
    def test_case_sensitivity(self):
        """Test that schemes are case-sensitive (lowercase only)."""
        with pytest.raises(ValueError, match="Invalid URI format"):
            parse_external_uri("AZ://container/key")
        
        with pytest.raises(ValueError, match="Invalid URI format"):
            parse_external_uri("S3://container/key")
        
        with pytest.raises(ValueError, match="Invalid URI format"):
            parse_external_uri("GS://container/key")