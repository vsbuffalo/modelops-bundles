"""
Tests for runtime types (MatEntry, ContentProvider protocol).

Tests the validation logic, constraints, and proper usage of the types
that bridge the runtime and content providers.
"""
import pytest

from modelops_bundles.runtime_types import MatEntry


class TestMatEntry:
    """Test MatEntry validation and constraints."""

    def test_oras_entry_valid(self):
        """Test valid ORAS entry creation."""
        entry = MatEntry(
            path="src/model.py",
            layer="code",
            kind="oras",
            content=b"print('hello')"
        )
        assert entry.path == "src/model.py"
        assert entry.kind == "oras"
        assert entry.content == b"print('hello')"

    def test_oras_entry_missing_content(self):
        """Test that ORAS entries require content."""
        with pytest.raises(ValueError, match="ORAS entries must have content bytes"):
            MatEntry(
                path="src/model.py",
                layer="code",
                kind="oras",
                content=None
            )

    def test_external_entry_valid(self):
        """Test valid external entry creation."""
        entry = MatEntry(
            path="data/train.csv",
            layer="data",
            kind="external",
            content=None,
            uri="az://container/path",
            sha256="a" * 64,
            size=1024,
            tier="cool"
        )
        assert entry.path == "data/train.csv"
        assert entry.kind == "external"
        assert entry.content is None
        assert entry.uri == "az://container/path"
        assert entry.size == 1024

    def test_external_entry_zero_size_allowed(self):
        """Test that external entries can have size=0."""
        entry = MatEntry(
            path="empty.txt",
            layer="data", 
            kind="external",
            content=None,
            uri="az://container/empty.txt",
            sha256="e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",  # SHA256 of empty string
            size=0,
            tier="hot"
        )
        assert entry.size == 0

    def test_external_entry_with_content_fails(self):
        """Test that external entries cannot have content bytes."""
        with pytest.raises(ValueError, match="External entries must have content=None"):
            MatEntry(
                path="data/train.csv",
                layer="data",
                kind="external",
                content=b"some,data",
                uri="az://container/path",
                sha256="a" * 64,
                size=1024
            )

    def test_external_entry_missing_uri(self):
        """Test that external entries require URI."""
        with pytest.raises(ValueError, match="External entries require fields: \\['uri'\\]"):
            MatEntry(
                path="data/train.csv",
                layer="data",
                kind="external",
                content=None,
                uri=None,
                sha256="a" * 64,
                size=1024
            )

    def test_external_entry_missing_sha256(self):
        """Test that external entries require SHA256."""
        with pytest.raises(ValueError, match="External entries require fields: \\['sha256'\\]"):
            MatEntry(
                path="data/train.csv",
                layer="data",
                kind="external",
                content=None,
                uri="az://container/path",
                sha256=None,
                size=1024
            )

    def test_external_entry_missing_size(self):
        """Test that external entries require size."""
        with pytest.raises(ValueError, match="External entries require fields: \\['size'\\]"):
            MatEntry(
                path="data/train.csv", 
                layer="data",
                kind="external",
                content=None,
                uri="az://container/path",
                sha256="a" * 64,
                size=None
            )

    def test_external_entry_missing_multiple_fields(self):
        """Test error message for multiple missing fields."""
        with pytest.raises(ValueError, match="External entries require fields: \\['uri', 'sha256', 'size'\\]"):
            MatEntry(
                path="data/train.csv",
                layer="data", 
                kind="external",
                content=None
            )

    def test_external_entry_invalid_sha256_length(self):
        """Test that external entries validate SHA256 length."""
        with pytest.raises(ValueError, match="External sha256 must be 64 hex chars"):
            MatEntry(
                path="data/train.csv",
                layer="data",
                kind="external", 
                content=None,
                uri="az://container/path",
                sha256="abc123",  # Too short
                size=1024
            )

    def test_external_entry_invalid_sha256_chars(self):
        """Test that external entries validate SHA256 format."""
        with pytest.raises(ValueError, match="External sha256 must be 64 hex chars"):
            MatEntry(
                path="data/train.csv",
                layer="data",
                kind="external",
                content=None,
                uri="az://container/path", 
                sha256="g" + "a" * 63,  # Invalid hex char
                size=1024
            )

    def test_external_entry_valid_sha256(self):
        """Test that valid SHA256 is accepted."""
        entry = MatEntry(
            path="data/train.csv",
            layer="data",
            kind="external",
            content=None,
            uri="az://container/path",
            sha256="abcdef0123456789" * 4,  # 64 valid hex chars
            size=1024
        )
        assert len(entry.sha256) == 64