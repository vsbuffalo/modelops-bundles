"""
CLI smoke tests with fake providers.

Tests basic CLI functionality and command wiring without requiring
real ORAS registries or external storage. Validates that all commands
can be invoked and produce expected output formats.
"""
from __future__ import annotations

import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from modelops_bundles.cli import app
from tests.fakes.fake_provider import FakeProvider


class TestCLISmokeTests:
    """Smoke tests for CLI commands with fake providers."""
    
    def setup_method(self):
        """Set up test environment."""
        self.runner = CliRunner()

    def test_resolve_command_basic(self):
        """Test resolve command with fake provider."""
        result = self.runner.invoke(app, [
            "resolve", "bundle:v1.0.0",
            "--provider", "fake"
        ])
        
        assert result.exit_code == 0
        assert "Manifest:" in result.stdout
        assert "Bundle: bundle:v1.0.0" in result.stdout
        assert "Size:" in result.stdout

    def test_resolve_command_no_cache(self):
        """Test resolve command with caching disabled."""
        result = self.runner.invoke(app, [
            "resolve", "bundle:v1.0.0",
            "--no-cache",
            "--provider", "fake"
        ])
        
        assert result.exit_code == 0
        assert "Bundle: bundle:v1.0.0" in result.stdout

    def test_materialize_command_basic(self):
        """Test materialize command with fake provider."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(app, [
                "materialize", "bundle:v1.0.0", temp_dir,
                "--provider", "fake"
            ])
            
            assert result.exit_code == 0
            assert f"Materialized bundle:v1.0.0 to {temp_dir}" in result.stdout
            assert "Role:" in result.stdout
            assert "Layers:" in result.stdout

    def test_materialize_command_with_options(self):
        """Test materialize command with various options."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(app, [
                "materialize", "bundle:v1.0.0", temp_dir,
                "--role", "runtime",
                "--overwrite",
                "--prefetch-external",
                "--ci",
                "--no-cache",
                "--provider", "fake"
            ])
            
            assert result.exit_code == 0
            assert "Materialized bundle:v1.0.0" in result.stdout

    def test_pull_command_alias(self):
        """Test pull command as alias for materialize."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(app, [
                "pull", "bundle:v1.0.0", temp_dir,
                "--provider", "fake"
            ])
            
            assert result.exit_code == 0
            assert f"Materialized bundle:v1.0.0 to {temp_dir}" in result.stdout

    def test_export_command_basic(self):
        """Test export command with temporary directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src_dir = Path(temp_dir) / "src"
            src_dir.mkdir()
            (src_dir / "test.txt").write_text("test content")
            
            archive_path = Path(temp_dir) / "output.tar"
            
            result = self.runner.invoke(app, [
                "export", str(src_dir), str(archive_path),
                "--compression", "none"
            ])
            
            assert result.exit_code == 0
            assert f"Exported {src_dir} to {archive_path}" in result.stdout
            assert "External data: pointer files only" in result.stdout
            assert archive_path.exists()

    def test_export_command_with_external_data(self):
        """Test export command including external data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            src_dir = Path(temp_dir) / "src"
            src_dir.mkdir()
            (src_dir / "test.txt").write_text("test content")
            
            archive_path = Path(temp_dir) / "output.tar.zst"
            
            result = self.runner.invoke(app, [
                "export", str(src_dir), str(archive_path),
                "--include-external"
            ])
            
            assert result.exit_code == 0
            assert "External data: included" in result.stdout

    def test_scan_command_stub(self):
        """Test scan command stub functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(app, [
                "scan", temp_dir
            ])
            
            assert result.exit_code == 0
            assert "[scan] Command implemented as stub" in result.stdout
            assert f"Scanned {temp_dir} (stub)" in result.stdout

    def test_plan_command_stub(self):
        """Test plan command stub functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(app, [
                "plan", temp_dir,
                "--external-preview"
            ])
            
            assert result.exit_code == 0
            assert "[plan] Command implemented as stub" in result.stdout
            assert f"Storage plan for {temp_dir} with external preview (stub)" in result.stdout

    def test_diff_command_stub(self):
        """Test diff command stub functionality."""
        result = self.runner.invoke(app, [
            "diff", "bundle:v1.0.0"
        ])
        
        assert result.exit_code == 0
        assert "[diff] Command implemented as stub" in result.stdout
        assert "Diff for bundle:v1.0.0 (stub)" in result.stdout

    def test_push_command_stub(self):
        """Test push command stub functionality."""
        with tempfile.TemporaryDirectory() as temp_dir:
            result = self.runner.invoke(app, [
                "push", temp_dir,
                "--bump", "minor"
            ])
            
            assert result.exit_code == 0
            assert "[push] Command implemented as stub" in result.stdout
            assert f"Pushed {temp_dir} with minor bump (stub)" in result.stdout

    def test_invalid_bundle_ref_handling(self):
        """Test that invalid bundle references are handled gracefully."""
        result = self.runner.invoke(app, [
            "resolve", "invalid-ref-format",
            "--provider", "fake"
        ])
        
        # Should exit with error code
        assert result.exit_code != 0

    def test_nonexistent_directory_export(self):
        """Test export command with nonexistent source directory."""
        result = self.runner.invoke(app, [
            "export", "/nonexistent/directory", "/tmp/output.tar"
        ])
        
        assert result.exit_code != 0

    def test_help_messages(self):
        """Test that help messages are displayed correctly."""
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "ModelOps Bundles CLI" in result.stdout
        
        # Test command-specific help
        result = self.runner.invoke(app, ["resolve", "--help"])
        assert result.exit_code == 0
        assert "Resolve bundle identity" in result.stdout

    @patch('modelops_bundles.cli._create_fake_registry')
    def test_provider_injection(self, mock_create_fake_registry):
        """Test that registry injection works correctly."""
        from tests.storage.fakes.fake_oci_registry import FakeOciRegistry
        from modelops_bundles.cli import _add_fake_manifests_oci
        
        mock_registry = FakeOciRegistry()
        _add_fake_manifests_oci(mock_registry)  # Add the expected manifests
        mock_create_fake_registry.return_value = mock_registry
        
        result = self.runner.invoke(app, [
            "resolve", "bundle:v1.0.0", "--provider", "fake"
        ])
        
        mock_create_fake_registry.assert_called_once()
        assert result.exit_code == 0

    def test_default_arguments(self):
        """Test commands with default arguments."""
        # scan defaults to current directory
        result = self.runner.invoke(app, [
            "scan"
        ])
        assert result.exit_code == 0
        
        # plan defaults to current directory  
        result = self.runner.invoke(app, [
            "plan"
        ])
        assert result.exit_code == 0
        
        # push defaults to current directory
        result = self.runner.invoke(app, [
            "push"
        ])
        assert result.exit_code == 0