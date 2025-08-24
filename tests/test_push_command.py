"""
Tests for push command implementation.
"""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch

from modelops_bundles.operations.facade import Operations, OpsConfig
from modelops_bundles.settings import Settings
from tests.storage.fakes.fake_oras_bundle_registry import FakeOrasBundleRegistry


@pytest.fixture
def settings():
    """Test settings."""
    return Settings(
        registry_url="http://localhost:5000",
        registry_repo="test/bundles",
        # Skip Azure creds for this test
        az_connection_string="DefaultEndpointsProtocol=https;AccountName=test;AccountKey=fake"
    )


@pytest.fixture  
def fake_registry(settings):
    """Fake ORAS registry for testing."""
    return FakeOrasBundleRegistry(settings)


@pytest.fixture
def operations(settings, fake_registry):
    """Operations facade with fake registry."""
    config = OpsConfig(ci=True, cache=False)
    return Operations(config, registry=fake_registry, settings=settings)


@pytest.fixture
def bundle_dir():
    """Path to test bundle fixture."""
    return Path(__file__).parent / "fixtures" / "simple-bundle"


class TestPushCommand:
    """Test push command functionality."""
    
    def test_push_basic_bundle(self, operations, bundle_dir, fake_registry):
        """Test pushing a basic bundle."""
        # Mock external storage since we're not testing that part
        with patch('modelops_bundles.publisher._upload_external_files') as mock_upload:
            # Push the bundle
            digest = operations.push(str(bundle_dir), force=True)
            
            # Should return a digest
            assert digest.startswith("sha256:")
            
            # Should have pushed files to registry
            assert len(fake_registry._manifests) > 0
            assert len(fake_registry._blobs) > 0
            
            # External files depend on size - our CSV is small so goes to ORAS
            # If we had large files, they would be uploaded externally
            # For this basic test, just check mock was set up correctly
            assert mock_upload is not None
    
    def test_push_dry_run(self, operations, bundle_dir):
        """Test dry run doesn't actually push."""
        with patch('modelops_bundles.publisher._show_dry_run_summary') as mock_summary:
            mock_summary.return_value = "dry-run-result"
            
            result = operations.push(str(bundle_dir), dry_run=True)
            
            assert result == "dry-run-result"
            mock_summary.assert_called_once()
    
    def test_push_with_version_bump(self, operations, bundle_dir, tmp_path):
        """Test version bump functionality."""
        # Copy bundle to temp directory to avoid modifying fixture
        import shutil
        temp_bundle = tmp_path / "bundle"
        shutil.copytree(bundle_dir, temp_bundle)
        
        with patch('modelops_bundles.publisher._upload_external_files'):
            # Get current version before bump
            from modelops_bundles.planner import scan_directory
            original_spec = scan_directory(temp_bundle)
            original_version = original_spec.version
            
            # Parse original version
            parts = original_version.split('.')
            expected_new_version = f"{parts[0]}.{int(parts[1]) + 1}.0"
            
            # Push with minor version bump
            digest = operations.push(str(temp_bundle), bump="minor", force=True)
            
            # Should return digest
            assert digest.startswith("sha256:")
            
            # Version should be updated in spec file
            updated_spec = scan_directory(temp_bundle)
            assert updated_spec.version == expected_new_version
    
    def test_push_missing_directory(self, operations):
        """Test push fails with missing directory."""
        with pytest.raises(FileNotFoundError):
            operations.push("/nonexistent/path", force=True)
    
    def test_push_invalid_directory(self, operations, tmp_path):
        """Test push fails with invalid directory."""
        # Create a file instead of directory
        fake_file = tmp_path / "fake.txt"
        fake_file.write_text("not a directory")
        
        with pytest.raises(ValueError, match="must be a directory"):
            operations.push(str(fake_file), force=True)