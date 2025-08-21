"""Basic tests to verify the OCI registry fixture works correctly."""
import pytest
import tempfile
import json
import hashlib
from pathlib import Path
from typing import Dict, Tuple, Any

# Note: oci_registry fixture is autodiscovered from tests/fixtures/oci_registry.py

# Import oras client for testing
try:
    import oras.client
    ORAS_AVAILABLE = True
except ImportError:
    ORAS_AVAILABLE = False


def create_test_artifact(tmpdir: Path, name: str = "test.json", content: Dict[str, Any] = None) -> Tuple[Path, str, Dict[str, Any]]:
    """
    Create a test file and return file path, SHA256 hash, and content.
    
    Args:
        tmpdir: Directory to create file in
        name: Filename to create
        content: Content to write (default creates sample content)
        
    Returns:
        Tuple of (file_path, sha256_hash, content_dict)
    """
    if content is None:
        content = {
            "message": "hello from test",
            "version": "1.0.0",
            "timestamp": "2024-01-01T00:00:00Z"
        }
    
    # Create file
    file_path = tmpdir / name
    with open(file_path, 'w') as f:
        json.dump(content, f, indent=2, sort_keys=True)
    
    # Calculate SHA256 hash
    with open(file_path, 'rb') as f:
        file_hash = hashlib.sha256(f.read()).hexdigest()
    
    return file_path, file_hash, content


@pytest.mark.integration
@pytest.mark.skipif(not ORAS_AVAILABLE, reason="oras library not available")
def test_oci_registry_fixture_basic(oci_registry):
    """Test that the registry fixture works and we can connect to it."""
    client = oras.client.OrasClient(insecure=True)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        
        # Create test artifact with hash
        test_file, expected_hash, expected_content = create_test_artifact(tmpdir_path)
        
        # Push to registry
        target = f"{oci_registry}/test-artifact:latest"
        result = client.push(target, files=[str(test_file)], disable_path_validation=True)
        
        assert result is not None
        
        # Pull back from registry
        pulled_files = client.pull(target, str(tmpdir_path / "pulled"))
        
        assert len(pulled_files) > 0
        
        # Verify content integrity using hash
        pulled_file_path = Path(pulled_files[0])
        assert pulled_file_path.exists()
        
        # Calculate hash of pulled file
        with open(pulled_file_path, 'rb') as f:
            actual_hash = hashlib.sha256(f.read()).hexdigest()
        
        # Verify content integrity
        assert actual_hash == expected_hash
        
        # Verify content is readable and correct
        with open(pulled_file_path, 'r') as f:
            actual_content = json.load(f)
        
        assert actual_content == expected_content


@pytest.mark.integration
@pytest.mark.skipif(not ORAS_AVAILABLE, reason="oras library not available")
def test_multiple_artifacts_same_registry(oci_registry):
    """Test that we can push multiple artifacts to the same registry."""
    client = oras.client.OrasClient(insecure=True)
    
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        
        # Create two different test artifacts with hashes
        artifact1_file, hash1, content1 = create_test_artifact(
            tmpdir_path, "artifact1.json", {"name": "artifact1", "type": "bundle"}
        )
        artifact2_file, hash2, content2 = create_test_artifact(
            tmpdir_path, "artifact2.json", {"name": "artifact2", "type": "layer"}
        )
        
        # Push both artifacts
        client.push(f"{oci_registry}/test1:latest", files=[str(artifact1_file)], disable_path_validation=True)
        client.push(f"{oci_registry}/test2:latest", files=[str(artifact2_file)], disable_path_validation=True)
        
        # Pull both back
        files1 = client.pull(f"{oci_registry}/test1:latest", str(tmpdir_path / "pull1"))
        files2 = client.pull(f"{oci_registry}/test2:latest", str(tmpdir_path / "pull2"))
        
        # Verify we got files back
        assert len(files1) > 0
        assert len(files2) > 0
        
        # Verify content integrity using hashes
        pulled1_path = Path(files1[0])
        pulled2_path = Path(files2[0])
        
        assert pulled1_path.exists()
        assert pulled2_path.exists()
        
        # Verify hashes match (content integrity)
        with open(pulled1_path, 'rb') as f:
            actual_hash1 = hashlib.sha256(f.read()).hexdigest()
        assert actual_hash1 == hash1
        
        with open(pulled2_path, 'rb') as f:
            actual_hash2 = hashlib.sha256(f.read()).hexdigest()
        assert actual_hash2 == hash2
        
        # Verify content is correct
        with open(pulled1_path, 'r') as f:
            actual_content1 = json.load(f)
        assert actual_content1 == content1
        
        with open(pulled2_path, 'r') as f:
            actual_content2 = json.load(f)
        assert actual_content2 == content2


def test_oci_registry_fixture_provides_url(oci_registry):
    """Test that the fixture provides a valid URL format."""
    # Basic URL format validation
    assert isinstance(oci_registry, str)
    assert ":" in oci_registry  # Should have host:port format
    
    host, port = oci_registry.split(":", 1)
    assert host  # Host should not be empty
    assert port.isdigit()  # Port should be numeric
    assert 1000 <= int(port) <= 65535  # Port should be in valid range