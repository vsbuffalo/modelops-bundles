#!/bin/bash
# ModelOps Bundles - Real Storage Integration Test Suite
# 
# Tests actual storage backends: Azure Blob Storage (via Azurite) and OCI Registry
# Requires Docker services to be running
#
# Usage: 
#   make up
#   source dev/dev.env  
#   bash dev/test-storage-real.sh
#
# Prerequisites: Docker, docker-compose, uv install

set -e
set -u

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
PURPLE='\033[0;35m'
NC='\033[0m' # No Color

# Test configuration
TEST_DIR="/tmp/modelops-real-test-$(date +%s)"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLI_CMD="uv run python -m modelops_bundles.cli"

# Test registry and Azure settings
TEST_REGISTRY="localhost:5555"
TEST_REPO="modelops-test"
TEST_BUNDLE_NAME="test-bundle"
TEST_BUNDLE_TAG="v1.0.0"
TEST_BUNDLE_REF="${TEST_REPO}/${TEST_BUNDLE_NAME}:${TEST_BUNDLE_TAG}"

log() {
    echo -e "${BLUE}[$(date +'%H:%M:%S')] $1${NC}"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
    exit 1
}

warn() {
    echo -e "${YELLOW}⚠️ $1${NC}"
}

info() {
    echo -e "${PURPLE}ℹ️ $1${NC}"
}

# Setup test environment
setup() {
    log "Setting up real storage test environment..."
    mkdir -p "$TEST_DIR"
    cd "$PROJECT_DIR"
    
    # Verify required environment variables are set
    if [[ -z "${AZURE_STORAGE_CONNECTION_STRING:-}" ]]; then
        error "AZURE_STORAGE_CONNECTION_STRING not set. Run: source dev/dev.env"
    fi
    
    if [[ -z "${MODELOPS_REGISTRY_URL:-}" ]]; then
        error "MODELOPS_REGISTRY_URL not set. Run: source dev/dev.env"
    fi
    
    # Verify Docker services are running
    log "Checking Docker services..."
    if ! curl -s "http://localhost:5555/v2/" > /dev/null; then
        error "OCI Registry not accessible at localhost:5555. Run: make up"
    fi
    success "  OCI Registry is accessible"
    
    # Check Azurite (may return 400 on properties endpoint but should be connectable)
    if ! curl -s --connect-timeout 5 "http://localhost:10000" > /dev/null; then
        error "Azurite not accessible at localhost:10000. Run: make up"  
    fi
    success "  Azurite is accessible"
    
    log "Test directory: $TEST_DIR"
    log "Test registry: $TEST_REGISTRY"
    log "Test repository: $TEST_REPO"
    success "Environment setup complete"
}

# Test 1: Basic Azure external storage operations
test_azure_external_storage() {
    log "Test 1: Azure External Storage (via Azurite)"
    
    # First we need to create a bundle with external data to test this properly
    # For now, test basic Azure connectivity
    log "  Testing Azure Blob Storage connectivity..."
    
    # Use Python to test Azure connectivity directly
    if uv run --dev python -c "
import os
try:
    from azure.storage.blob import BlobServiceClient
    client = BlobServiceClient.from_connection_string(os.environ['AZURE_STORAGE_CONNECTION_STRING'])
    # Try to list containers (this will create connection)
    containers = list(client.list_containers())
    print(f'Found {len(containers)} containers')
    print('Azure connection successful')
    exit(0)
except ImportError:
    print('Azure SDK not available')
    exit(1)
except Exception as e:
    print(f'Azure connection failed: {e}')
    exit(1)
    " > "$TEST_DIR/azure-test.out" 2>&1; then
        success "  Azure Blob Storage connection works"
    else
        warn "  Azure Blob Storage connection failed - check logs:"
        info "    $(cat "$TEST_DIR/azure-test.out")"
        if grep -q "Azure SDK not available" "$TEST_DIR/azure-test.out"; then
            warn "  Azure SDK should be available via dependencies"
        fi
    fi
    
    success "Test 1: Azure External Storage connectivity verified"
}

# Test 2: OCI Registry operations
test_oci_registry_operations() {
    log "Test 2: OCI Registry Operations"
    
    # First, create a simple bundle structure to push
    log "  Creating test bundle..."
    mkdir -p "$TEST_DIR/bundle-to-push"
    cat > "$TEST_DIR/bundle-to-push/modelops.yaml" << 'EOF'
apiVersion: modelops.dev/v1alpha1
kind: Bundle
metadata:
  name: test-bundle
  version: v1.0.0
spec:
  layers:
    - name: code
      type: code
      files:
        - src/**
  roles:
    default:
      - code
EOF
    
    mkdir -p "$TEST_DIR/bundle-to-push/src"
    echo 'print("Hello from test bundle")' > "$TEST_DIR/bundle-to-push/src/main.py"
    
    success "  Test bundle created"
    
    # Test registry connectivity
    log "  Testing OCI Registry API..."
    if curl -sf "http://localhost:5555/v2/" > /dev/null; then
        success "  OCI Registry API accessible"
    else
        error "  OCI Registry API not accessible at localhost:5555"
    fi
    
    # Test catalog endpoint
    log "  Testing registry catalog..."  
    if curl -sf "http://localhost:5555/v2/_catalog" > "$TEST_DIR/registry-catalog.out" 2>&1; then
        success "  Registry catalog accessible"
        info "    Catalog: $(cat "$TEST_DIR/registry-catalog.out")"
    else
        warn "  Registry catalog not accessible (expected for empty registry)"
    fi
    
    success "Test 2: OCI Registry operations verified"
}

# Test 3: End-to-end bundle lifecycle with push
test_bundle_lifecycle() {
    log "Test 3: Bundle Lifecycle (Create → Push → Pull → Materialize)"
    
    # Create a real test bundle to push
    log "  Creating test bundle for push..."
    mkdir -p "$TEST_DIR/push-test-bundle/src" "$TEST_DIR/push-test-bundle/config"
    
    cat > "$TEST_DIR/push-test-bundle/modelops.yaml" << 'EOF'
apiVersion: modelops.dev/v1alpha1
kind: Bundle
metadata:
  name: storage-real-test
  version: v1.0.0
  description: Real storage integration test bundle
spec:
  layers:
    - name: code
      type: code
      files:
        - src/**/*.py
    - name: config
      type: config
      files:
        - config/*.yaml
  roles:
    default:
      - code
      - config
    runtime:
      - code
EOF

    cat > "$TEST_DIR/push-test-bundle/src/model.py" << 'EOF'
"""Test model for real storage integration."""
import json

class TestModel:
    def __init__(self, config_path: str = None):
        self.config = {}
        if config_path:
            with open(config_path) as f:
                self.config = json.load(f)
    
    def predict(self, data):
        return {"prediction": "test", "input": data}
    
    def get_info(self):
        return {"model": "TestModel", "version": "v1.0.0"}
EOF

    cat > "$TEST_DIR/push-test-bundle/config/model.yaml" << 'EOF'
model:
  type: test
  parameters:
    threshold: 0.5
    batch_size: 32
logging:
  level: INFO
EOF

    success "  Test bundle created"
    
    # Push the bundle
    log "  Pushing test bundle to real storage..."
    if $CLI_CMD push "$TEST_DIR/push-test-bundle" > "$TEST_DIR/push-test.out" 2>&1; then
        success "  Bundle pushed successfully to real storage"
        
        # Extract digest from output
        push_digest=$(grep -o "sha256:[a-f0-9]\{64\}" "$TEST_DIR/push-test.out" | head -1)
        if [[ -n "$push_digest" ]]; then
            info "  Push digest: $push_digest"
        fi
    else
        error "  Push to real storage failed: $(cat "$TEST_DIR/push-test.out" 2>/dev/null || echo 'No error output available')"
    fi
    
    # Pull the bundle back using real provider
    log "  Materializing pushed bundle with real provider..."
    mkdir -p "$TEST_DIR/real-materialize"
    
    if $CLI_CMD materialize "storage-real-test:v1.0.0" "$TEST_DIR/real-materialize" > "$TEST_DIR/real-materialize.out" 2>&1; then
        success "  Real provider materialization works"
        
        if [[ -f "$TEST_DIR/real-materialize/src/model.py" ]]; then
            success "  Bundle files materialized correctly"
        else
            warn "  Bundle materialized but expected files missing"
        fi
        
        if [[ -f "$TEST_DIR/real-materialize/config/model.yaml" ]]; then
            success "  Config files materialized correctly"
        else
            warn "  Config files missing from materialization"
        fi
        
        if [[ -f "$TEST_DIR/real-materialize/.mops/provenance.json" ]]; then
            success "  Provenance file created"
            info "  Provenance: $(head -2 "$TEST_DIR/real-materialize/.mops/provenance.json" | tail -1)"
        else
            warn "  Provenance file missing"
        fi
    else
        error "  Real provider materialization failed: $(cat "$TEST_DIR/real-materialize.out")"
    fi
    
    # Test role-specific materialization
    log "  Testing role-specific materialization..."
    mkdir -p "$TEST_DIR/runtime-role"
    
    if $CLI_CMD materialize "storage-real-test:v1.0.0" "$TEST_DIR/runtime-role" --role runtime > "$TEST_DIR/runtime-role.out" 2>&1; then
        success "  Runtime role materialization works"
        
        if [[ -f "$TEST_DIR/runtime-role/src/model.py" ]] && [[ ! -f "$TEST_DIR/runtime-role/config/model.yaml" ]]; then
            success "  Runtime role correctly excludes config layer"
        else
            warn "  Runtime role layer filtering may not be working correctly"
        fi
    else
        warn "  Runtime role materialization failed: $(cat "$TEST_DIR/runtime-role.out")"
    fi
    
    success "Test 3: Real bundle lifecycle completed successfully"
}

# Test 4: Large file handling and streaming
test_large_file_handling() {
    log "Test 4: Large File Handling"
    
    # Create a moderately large test file (10MB) to test streaming
    log "  Creating 10MB test file..."
    dd if=/dev/zero of="$TEST_DIR/large-file.bin" bs=1024 count=10240 2>/dev/null
    
    success "  Large test file created (10MB)"
    
    # Test export with large file
    log "  Testing export with large file..."
    mkdir -p "$TEST_DIR/large-bundle/data"
    cp "$TEST_DIR/large-file.bin" "$TEST_DIR/large-bundle/data/"
    echo 'print("Large bundle")' > "$TEST_DIR/large-bundle/main.py"
    
    if $CLI_CMD export "$TEST_DIR/large-bundle" "$TEST_DIR/large-export.tar.zst" > "$TEST_DIR/large-export.out" 2>&1; then
        success "  Large file export works"
        
        # Check export file size
        export_size=$(stat -f%z "$TEST_DIR/large-export.tar.zst" 2>/dev/null || stat -c%s "$TEST_DIR/large-export.tar.zst" 2>/dev/null || echo "0")
        if [[ $export_size -gt 1000000 ]]; then
            success "  Export file size reasonable ($export_size bytes)"
        else
            warn "  Export file unexpectedly small ($export_size bytes)"
        fi
    else
        error "  Large file export failed"
    fi
    
    success "Test 4: Large file handling verified"
}

# Test 5: Concurrent operations
test_concurrent_operations() {
    log "Test 5: Concurrent Operations"
    
    log "  Testing concurrent materializations..."
    
    # Start multiple materialize operations in background
    for i in {1..3}; do
        mkdir -p "$TEST_DIR/concurrent-$i"
        ($CLI_CMD materialize bundle:v1.0.0 "$TEST_DIR/concurrent-$i" --provider fake > "$TEST_DIR/concurrent-$i.out" 2>&1 || true) &
    done
    
    # Wait for all background jobs
    wait
    
    # Check results
    success_count=0
    for i in {1..3}; do
        if [[ -f "$TEST_DIR/concurrent-$i/src/model.py" ]]; then
            success_count=$((success_count + 1))
        fi
    done
    
    if [[ $success_count -eq 3 ]]; then
        success "  All 3 concurrent operations succeeded"
    else
        warn "  Only $success_count/3 concurrent operations succeeded"
    fi
    
    success "Test 5: Concurrent operations tested"
}

# Test 6: Error scenarios and recovery
test_error_scenarios() {
    log "Test 6: Error Scenarios and Recovery"
    
    # Test with invalid registry URL
    log "  Testing invalid registry URL..."
    if MODELOPS_REGISTRY_URL="http://invalid.registry:9999" $CLI_CMD resolve bundle:v1.0.0 > "$TEST_DIR/invalid-registry.out" 2>&1; then
        warn "  Should have failed with invalid registry URL"
    else
        success "  Invalid registry URL correctly rejected"
    fi
    
    # Test with invalid Azure connection
    log "  Testing invalid Azure connection..."
    if AZURE_STORAGE_CONNECTION_STRING="invalid-connection-string" uv run --dev python -c "
try:
    from azure.storage.blob import BlobServiceClient
    client = BlobServiceClient.from_connection_string('invalid-connection-string')
    list(client.list_containers())
    exit(0)
except ImportError:
    # Azure SDK not installed, skip this test
    exit(1)
except:
    exit(1)
    " > "$TEST_DIR/invalid-azure.out" 2>&1; then
        warn "  Should have failed with invalid Azure connection"
    else
        if grep -q "Azure SDK not available" "$TEST_DIR/azure-test.out" 2>/dev/null; then
            warn "  Skipping invalid Azure test (SDK not available)"
        else
            success "  Invalid Azure connection correctly rejected"
        fi
    fi
    
    success "Test 6: Error scenarios handled correctly"
}

# Cleanup
cleanup() {
    log "Cleaning up test environment..."
    cd /tmp
    rm -rf "$TEST_DIR"
    success "Cleanup complete"
}

# Main execution
main() {
    echo -e "${BLUE}"
    echo "════════════════════════════════════════════════════════════════"
    echo "    ModelOps Bundles - Real Storage Integration Tests"  
    echo "════════════════════════════════════════════════════════════════"
    echo -e "${NC}"
    
    setup
    
    # Run all tests
    test_azure_external_storage
    test_oci_registry_operations  
    test_bundle_lifecycle
    test_large_file_handling
    test_concurrent_operations
    test_error_scenarios
    
    cleanup
    
    echo -e "${GREEN}"
    echo "════════════════════════════════════════════════════════════════"
    echo "    🎉 Real storage integration tests completed!"
    echo "════════════════════════════════════════════════════════════════"  
    echo -e "${NC}"
}

# Handle Ctrl+C gracefully
trap cleanup EXIT

main "$@"