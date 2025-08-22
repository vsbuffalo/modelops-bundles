"""
Tests for path safety validation.

Tests the shared path_safety module and integration with components
that use it (pointer_writer, runtime).
"""
from __future__ import annotations

import pytest
from pathlib import Path

from modelops_bundles.path_safety import safe_relpath
from modelops_bundles.pointer_writer import write_pointer_file


class TestSafeRelpath:
    """Test safe_relpath function directly."""
    
    def test_safe_paths_allowed(self):
        """Test that safe relative paths are allowed."""
        assert safe_relpath("file.txt") == "file.txt"
        assert safe_relpath("dir/file.txt") == "dir/file.txt"
        assert safe_relpath("deep/nested/path/file.txt") == "deep/nested/path/file.txt"
        assert safe_relpath("data/train.csv") == "data/train.csv"
    
    def test_absolute_paths_rejected(self):
        """Test that absolute paths are rejected."""
        with pytest.raises(ValueError, match="unsafe path: /absolute/path"):
            safe_relpath("/absolute/path")
        
        with pytest.raises(ValueError, match="unsafe path: /etc/passwd"):
            safe_relpath("/etc/passwd")
    
    def test_parent_directory_traversal_rejected(self):
        """Test that parent directory traversal is rejected."""
        with pytest.raises(ValueError, match="unsafe path: ../evil.txt"):
            safe_relpath("../evil.txt")
        
        with pytest.raises(ValueError, match="unsafe path: dir/../../evil.txt"):
            safe_relpath("dir/../../evil.txt")
        
        with pytest.raises(ValueError, match="unsafe path: good/../bad/../../evil.txt"):
            safe_relpath("good/../bad/../../evil.txt")
    
    def test_mops_reserved_prefix_rejected(self):
        """Test that .mops/ reserved prefix is rejected."""
        with pytest.raises(ValueError, match="unsafe path: .mops/config.json"):
            safe_relpath(".mops/config.json")
        
        with pytest.raises(ValueError, match="unsafe path: .mops/ptr/data/file.json"):
            safe_relpath(".mops/ptr/data/file.json")
        
        with pytest.raises(ValueError, match="unsafe path: .mops/anything"):
            safe_relpath(".mops/anything")
        
        # Test bare .mops rejection
        with pytest.raises(ValueError, match="unsafe path: .mops"):
            safe_relpath(".mops")
    
    def test_backslash_paths_rejected(self):
        """Test that paths containing backslashes are rejected (Windows security)."""
        dangerous_paths = [
            "a\\b\\c.txt",
            "..\\..\\etc\\passwd",
            "data\\..\\..\\secrets.txt",
            "normal\\path\\file.txt",
            "mixed/path\\with\\backslashes",
            "\\absolute\\windows\\path",
        ]
        
        for path in dangerous_paths:
            with pytest.raises(ValueError, match="unsafe path"):
                safe_relpath(path)


class TestPointerWriterPathSafety:
    """Test that pointer writer validates paths safely."""
    
    def test_write_pointer_file_rejects_unsafe_original_path(self, tmp_path):
        """Test that write_pointer_file rejects unsafe original_relpath."""
        with pytest.raises(ValueError, match="unsafe path"):
            write_pointer_file(tmp_path, "../evil.txt", "az://c/x", "a"*64, 1, "data")
        
        with pytest.raises(ValueError, match="unsafe path"):
            write_pointer_file(tmp_path, "/absolute/evil.txt", "az://c/x", "a"*64, 1, "data")
        
        with pytest.raises(ValueError, match="unsafe path"):
            write_pointer_file(tmp_path, ".mops/evil.txt", "az://c/x", "a"*64, 1, "data")
    
    def test_write_pointer_file_accepts_safe_paths(self, tmp_path):
        """Test that write_pointer_file accepts safe relative paths."""
        # Should not raise
        result_path = write_pointer_file(
            tmp_path, "data/train.csv", "az://bucket/train.csv", "a"*64, 1024, "data"
        )
        
        # Verify the pointer file was created at expected location
        expected = tmp_path / ".mops" / "ptr" / "data" / "train.csv.json"
        assert result_path == expected
        assert expected.exists()