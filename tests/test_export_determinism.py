"""
Test deterministic archive export functionality.

Validates that write_deterministic_archive produces byte-identical output
from identical input trees and handles edge cases properly.
"""
from __future__ import annotations

import os
import tarfile
import tempfile
import zstandard as zstd
from pathlib import Path

import pytest

from modelops_bundles.export import (
    write_deterministic_archive, normalize_relpath, _apply_canonical_headers,
    _is_external_data_file
)


class TestDeterministicExport:
    """Test deterministic archive creation."""

    def test_identical_trees_produce_identical_archives(self, tmp_path):
        """Test that identical source trees produce byte-identical archives."""
        # Create two identical directory trees
        tree1 = tmp_path / "tree1"
        tree2 = tmp_path / "tree2"
        
        for tree in [tree1, tree2]:
            tree.mkdir()
            (tree / "file1.txt").write_text("content1")
            (tree / "file2.txt").write_text("content2")
            subdir = tree / "subdir"
            subdir.mkdir()
            (subdir / "nested.txt").write_text("nested content")
        
        # Export both trees
        archive1 = tmp_path / "archive1.tar"
        archive2 = tmp_path / "archive2.tar"
        
        write_deterministic_archive(str(tree1), str(archive1))
        write_deterministic_archive(str(tree2), str(archive2))
        
        # Archives should be byte-identical
        assert archive1.read_bytes() == archive2.read_bytes()

    def test_zstd_compression_deterministic(self, tmp_path):
        """Test that zstd compression produces deterministic output."""
        # Create source tree
        src = tmp_path / "src"
        src.mkdir()
        (src / "test.txt").write_text("test content")
        
        # Export twice with zstd compression
        archive1 = tmp_path / "archive1.tar.zst"
        archive2 = tmp_path / "archive2.tar.zst"
        
        write_deterministic_archive(str(src), str(archive1), zstd_level=3)
        write_deterministic_archive(str(src), str(archive2), zstd_level=3)
        
        # Archives should be byte-identical
        assert archive1.read_bytes() == archive2.read_bytes()

    def test_external_data_filtering(self, tmp_path):
        """Test that external data files are filtered correctly."""
        # Create source tree with external data
        src = tmp_path / "src"
        src.mkdir()
        (src / "regular.txt").write_text("regular file")
        
        # Create external data directory
        external_dir = src / "dataset.data"
        external_dir.mkdir()
        (external_dir / "data.bin").write_bytes(b"binary data")
        
        # Export without external data
        archive = tmp_path / "archive.tar"
        write_deterministic_archive(str(src), str(archive), include_external=False)
        
        # Verify external data is excluded
        with tarfile.open(archive, 'r') as tar:
            names = tar.getnames()
            assert "regular.txt" in names
            assert "dataset.data" in names  # Directory included
            assert "dataset.data/data.bin" not in names  # File excluded

    def test_path_normalization_windows_compatibility(self, tmp_path):
        """Test that paths are normalized for Windows compatibility."""
        # Test path normalization directly
        assert normalize_relpath("path/to/file") == "path/to/file"
        assert normalize_relpath("path\\to\\file") == "path/to/file"
        assert normalize_relpath("path\\mixed/slash") == "path/mixed/slash"

    def test_canonical_headers_applied(self, tmp_path):
        """Test that tar headers are canonicalized for determinism."""
        # Create source file with specific permissions
        src = tmp_path / "src"
        src.mkdir()
        test_file = src / "test.txt"
        test_file.write_text("content")
        test_file.chmod(0o644)
        
        # Export to archive
        archive = tmp_path / "archive.tar"
        write_deterministic_archive(str(src), str(archive))
        
        # Verify canonical headers
        with tarfile.open(archive, 'r') as tar:
            for member in tar.getmembers():
                assert member.uid == 0
                assert member.gid == 0
                assert member.uname == ""
                assert member.gname == ""
                assert member.mtime == 0
                
                if member.isdir():
                    assert member.mode == 0o755
                elif member.isreg() and not (member.mode & 0o100):
                    assert member.mode == 0o644

    def test_ustar_format_enforced(self, tmp_path):
        """Test that USTAR format is used for compatibility."""
        src = tmp_path / "src"
        src.mkdir()
        (src / "test.txt").write_text("content")
        
        archive = tmp_path / "archive.tar"
        write_deterministic_archive(str(src), str(archive))
        
        # Verify USTAR format by checking tar file directly
        with tarfile.open(archive, 'r') as tar:
            # USTAR format uses specific header structure
            assert tar.format == tarfile.USTAR_FORMAT

    def test_atomic_write_on_failure(self, tmp_path):
        """Test that failed writes don't leave partial files."""
        src = tmp_path / "nonexistent"  # Doesn't exist
        archive = tmp_path / "archive.tar"
        
        with pytest.raises(ValueError, match="Source directory does not exist"):
            write_deterministic_archive(str(src), str(archive))
        
        # Archive file should not exist
        assert not archive.exists()

    def test_deterministic_ordering(self, tmp_path):
        """Test that entries are ordered deterministically."""
        # Create directory with files in various orders
        src = tmp_path / "src"
        src.mkdir()
        
        # Create files in specific order that might not be filesystem order
        (src / "zebra.txt").write_text("z")
        (src / "alpha.txt").write_text("a")
        subdir = src / "middle"
        subdir.mkdir()
        (subdir / "beta.txt").write_text("b")
        
        # Export twice and verify order is consistent
        archive1 = tmp_path / "archive1.tar"
        archive2 = tmp_path / "archive2.tar"
        
        write_deterministic_archive(str(src), str(archive1))
        write_deterministic_archive(str(src), str(archive2))
        
        # Should be identical
        assert archive1.read_bytes() == archive2.read_bytes()
        
        # Verify actual ordering in archive
        with tarfile.open(archive1, 'r') as tar:
            names = tar.getnames()
            # Should be sorted alphabetically
            assert names == sorted(names)


class TestHelperFunctions:
    """Test export helper functions."""
    
    def test_normalize_relpath(self):
        """Test path normalization function."""
        assert normalize_relpath("simple") == "simple"
        assert normalize_relpath("path/to/file") == "path/to/file"
        assert normalize_relpath("path\\to\\file") == "path/to/file"
        
        # Test unsafe path rejection
        with pytest.raises(ValueError):
            normalize_relpath("../escape")
        with pytest.raises(ValueError):
            normalize_relpath("/absolute")
        with pytest.raises(ValueError):
            normalize_relpath(".mops")
        with pytest.raises(ValueError):
            normalize_relpath(".mops/config")

    def test_apply_canonical_headers(self):
        """Test tar header canonicalization."""
        # Create a mock tarinfo
        info = tarfile.TarInfo("test.txt")
        info.uid = 1000
        info.gid = 1000
        info.uname = "user"
        info.gname = "group"
        info.mtime = 1234567890
        info.mode = 0o664
        
        _apply_canonical_headers(info)
        
        assert info.uid == 0
        assert info.gid == 0
        assert info.uname == ""
        assert info.gname == ""
        assert info.mtime == 0
        assert info.mode == 0o644  # Normalized for regular file

    def test_is_external_data_file(self):
        """Test external data file detection."""
        assert not _is_external_data_file(Path("regular.txt"))
        assert not _is_external_data_file(Path("subdir/file.txt"))
        assert _is_external_data_file(Path("dataset.data/file.bin"))
        assert _is_external_data_file(Path("nested/dataset.data/sub/file.bin"))
        assert not _is_external_data_file(Path("dataset/file.data"))  # .data suffix on file, not dir