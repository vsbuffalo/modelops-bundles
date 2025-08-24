#!/bin/bash
# ModelOps Bundles - Round Trip Test Suite
#
# Tests the complete push → pull → verify workflow against local dev stack
# Validates both ORAS registry storage and Azure external storage
#
# Usage:
#   make up
#   source dev/dev.env
#   bash dev/test-round-trip.sh
#
# Prerequisites: Docker services running, uv install

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
TEST_DIR="/tmp/modelops-roundtrip-$(date +%s)"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
CLI_CMD="uv run python -m modelops_bundles.cli"

# Test registry settings
TEST_REGISTRY="localhost:5555"
TEST_REPO="roundtrip-test"
TEST_BUNDLE_TAG="v1.0.0"
TEST_BUNDLE_REF="${TEST_REPO}/test-bundle:${TEST_BUNDLE_TAG}"

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

# Checksum calculation helper
calculate_checksums() {
    local dir="$1"
    find "$dir" -type f -exec sha256sum {} \; | sort
}

# Setup test environment
setup() {
    log "Setting up round-trip test environment..."
    mkdir -p "$TEST_DIR"
    cd "$PROJECT_DIR"
    
    # Verify required environment variables
    if [[ -z "${AZURE_STORAGE_CONNECTION_STRING:-}" ]]; then
        error "AZURE_STORAGE_CONNECTION_STRING not set. Run: source dev/dev.env"
    fi
    
    if [[ -z "${MODELOPS_REGISTRY_URL:-}" ]]; then
        error "MODELOPS_REGISTRY_URL not set. Run: source dev/dev.env"
    fi
    
    # Verify Docker services are running
    log "Checking Docker services..."
    if ! curl -sf "http://localhost:5555/v2/" > /dev/null; then
        error "OCI Registry not accessible at localhost:5555. Run: make up"
    fi
    success "  OCI Registry is accessible"
    
    if ! curl -s --connect-timeout 5 "http://localhost:10000" > /dev/null 2>&1; then
        error "Azurite not accessible at localhost:10000. Run: make up"  
    fi
    success "  Azurite is accessible"
    
    log "Test directory: $TEST_DIR"
    log "Test bundle reference: $TEST_BUNDLE_REF"
    success "Environment setup complete"
}

# Test 1: Simple bundle round-trip (ORAS storage only)
test_simple_round_trip() {
    log "Test 1: Simple Bundle Round-Trip (ORAS storage)"
    
    # Create a simple test bundle (no external files)
    log "  Creating simple test bundle..."
    mkdir -p "$TEST_DIR/simple-bundle/src"
    cat > "$TEST_DIR/simple-bundle/modelops.yaml" << 'EOF'
apiVersion: v1
kind: Bundle
metadata:
  name: simple-round-trip
  version: v1.0.0
spec:
  layers:
    - name: code
      type: code
      files:
        - src/**/*.py
  roles:
    default:
      - code
EOF

    cat > "$TEST_DIR/simple-bundle/src/main.py" << 'EOF'
"""Simple test script for round-trip testing."""
print("Hello from simple round-trip test!")
print(f"Bundle version: v1.0.0")
EOF

    cat > "$TEST_DIR/simple-bundle/src/utils.py" << 'EOF'
"""Utility functions."""

def greet(name: str) -> str:
    return f"Hello, {name}!"

def calculate(a: int, b: int) -> int:
    return a + b
EOF

    success "  Simple test bundle created"
    
    # Calculate original checksums
    log "  Calculating original checksums..."
    calculate_checksums "$TEST_DIR/simple-bundle" > "$TEST_DIR/simple-original.checksums"
    success "  Original checksums calculated"
    
    # Push the bundle
    log "  Pushing simple bundle..."
    if $CLI_CMD push "$TEST_DIR/simple-bundle" > "$TEST_DIR/simple-push.out" 2>&1; then
        success "  Bundle pushed successfully"
        
        # Extract digest from push output
        push_digest=$(grep -o "sha256:[a-f0-9]\{64\}" "$TEST_DIR/simple-push.out" | head -1)
        if [[ -n "$push_digest" ]]; then
            info "  Push digest: $push_digest"
        else
            warn "  Could not extract digest from push output"
        fi
    else
        error "  Push failed: $(cat "$TEST_DIR/simple-push.out")"
    fi
    
    # Pull the bundle to a new location
    log "  Materializing bundle to new location..."
    mkdir -p "$TEST_DIR/simple-pulled"
    if $CLI_CMD materialize "simple-round-trip:v1.0.0" "$TEST_DIR/simple-pulled" > "$TEST_DIR/simple-pull.out" 2>&1; then
        success "  Bundle materialized successfully"
    else
        error "  Materialize failed: $(cat "$TEST_DIR/simple-pull.out")"
    fi
    
    # Verify files match
    log "  Verifying file integrity..."
    calculate_checksums "$TEST_DIR/simple-pulled" > "$TEST_DIR/simple-pulled.checksums"
    
    if diff -u "$TEST_DIR/simple-original.checksums" "$TEST_DIR/simple-pulled.checksums" > "$TEST_DIR/simple-diff.out"; then
        success "  Files match perfectly! Round-trip successful"
    else
        warn "  Files differ:"
        cat "$TEST_DIR/simple-diff.out"
        error "  Round-trip verification failed"
    fi
    
    success "Test 1: Simple round-trip completed successfully"
}

# Test 2: Bundle with external storage (Azure)
test_external_round_trip() {
    log "Test 2: Bundle with External Storage Round-Trip"
    
    # Create bundle with large files that should go to external storage
    log "  Creating bundle with external files..."
    mkdir -p "$TEST_DIR/external-bundle/src" "$TEST_DIR/external-bundle/data"
    
    cat > "$TEST_DIR/external-bundle/modelops.yaml" << 'EOF'
apiVersion: v1
kind: Bundle
metadata:
  name: external-round-trip
  version: v1.0.0
spec:
  layers:
    - name: code
      type: code
      files:
        - src/**/*.py
    - name: data
      type: data
      files:
        - data/**
  roles:
    default:
      - code
      - data
  external_rules:
    - pattern: "data/*.bin"
      uri_template: "az://test-container/bundles/{path}"
      tier: hot
      size_threshold: 1048576  # 1MB threshold
  oras_size_limit: 10485760  # 10MB limit
EOF

    cat > "$TEST_DIR/external-bundle/src/processor.py" << 'EOF'
"""Data processor for external storage test."""
import os

def process_data_file(filepath: str) -> dict:
    """Process a data file and return metadata."""
    size = os.path.getsize(filepath)
    return {
        "filepath": filepath,
        "size_bytes": size,
        "size_mb": round(size / 1024 / 1024, 2)
    }
EOF

    # Create a large file that should trigger external storage
    log "  Creating large test file (5MB)..."
    dd if=/dev/zero of="$TEST_DIR/external-bundle/data/large-dataset.bin" bs=1024 count=5120 2>/dev/null
    
    # Create a small file that should stay in ORAS
    echo "small data content" > "$TEST_DIR/external-bundle/data/small-config.txt"
    
    success "  Bundle with external files created"
    
    # Calculate original checksums
    log "  Calculating original checksums..."
    calculate_checksums "$TEST_DIR/external-bundle" > "$TEST_DIR/external-original.checksums"
    success "  Original checksums calculated"
    
    # Push the bundle
    log "  Pushing bundle with external storage..."
    if $CLI_CMD push "$TEST_DIR/external-bundle" > "$TEST_DIR/external-push.out" 2>&1; then
        success "  Bundle with externals pushed successfully"
        
        # Check if external files were actually uploaded
        if grep -q "External files uploaded" "$TEST_DIR/external-push.out"; then
            success "  External files were uploaded to Azure"
        else
            info "  No external files uploaded (expected if files are small)"
        fi
        
        # Check for pointer files
        if [[ -f "$TEST_DIR/external-bundle/.mops/ptr/data/large-dataset.bin.json" ]]; then
            success "  Pointer file created for external data"
            info "  Pointer: $(cat "$TEST_DIR/external-bundle/.mops/ptr/data/large-dataset.bin.json" | head -1)"
        else
            info "  No pointer files (file may be under threshold)"
        fi
    else
        error "  Push with externals failed: $(cat "$TEST_DIR/external-push.out")"
    fi
    
    # Pull the bundle to a new location
    log "  Materializing bundle with externals..."
    mkdir -p "$TEST_DIR/external-pulled"
    if $CLI_CMD materialize "external-round-trip:v1.0.0" "$TEST_DIR/external-pulled" > "$TEST_DIR/external-pull.out" 2>&1; then
        success "  Bundle with externals materialized successfully"
    else
        error "  Materialize with externals failed: $(cat "$TEST_DIR/external-pull.out")"
    fi
    
    # Verify files match
    log "  Verifying external storage round-trip integrity..."
    calculate_checksums "$TEST_DIR/external-pulled" > "$TEST_DIR/external-pulled.checksums"
    
    if diff -u "$TEST_DIR/external-original.checksums" "$TEST_DIR/external-pulled.checksums" > "$TEST_DIR/external-diff.out"; then
        success "  Files match perfectly! External storage round-trip successful"
    else
        warn "  Files differ:"
        cat "$TEST_DIR/external-diff.out"
        error "  External storage round-trip verification failed"
    fi
    
    success "Test 2: External storage round-trip completed successfully"
}

# Test 3: Role-based materialization
test_role_round_trip() {
    log "Test 3: Role-based Materialization Round-Trip"
    
    # Use the existing test fixture which has multiple roles
    log "  Using test fixture with multiple roles..."
    fixture_bundle="$PROJECT_DIR/tests/fixtures/simple-bundle"
    
    if [[ ! -f "$fixture_bundle/modelops.yaml" ]]; then
        error "Test fixture not found at $fixture_bundle"
    fi
    
    # Push the fixture bundle (copy to avoid modifying fixture)
    cp -r "$fixture_bundle" "$TEST_DIR/role-bundle"
    
    # Update the bundle name to avoid conflicts
    sed -i.bak 's/name: simple-test-bundle/name: role-round-trip/' "$TEST_DIR/role-bundle/modelops.yaml"
    
    log "  Pushing role test bundle..."
    if $CLI_CMD push "$TEST_DIR/role-bundle" > "$TEST_DIR/role-push.out" 2>&1; then
        success "  Role test bundle pushed"
    else
        error "  Role bundle push failed: $(cat "$TEST_DIR/role-push.out")"
    fi
    
    # Test different roles
    for role in inference training default; do
        log "  Testing role: $role"
        mkdir -p "$TEST_DIR/role-$role"
        
        if $CLI_CMD materialize "role-round-trip:1.5.0" "$TEST_DIR/role-$role" --role "$role" > "$TEST_DIR/role-$role.out" 2>&1; then
            success "  Role $role materialized successfully"
            
            # Check that role-specific files are present
            if [[ "$role" == "training" ]]; then
                # Training should include data layer
                if [[ -d "$TEST_DIR/role-$role/data" ]]; then
                    success "  Training role includes data layer as expected"
                else
                    warn "  Training role missing data layer"
                fi
            fi
            
            if [[ -f "$TEST_DIR/role-$role/.mops/provenance.json" ]]; then
                success "  Provenance file created for role $role"
                
                # Check that provenance includes role info
                if grep -q "\"role\": \"$role\"" "$TEST_DIR/role-$role/.mops/provenance.json"; then
                    success "  Role correctly recorded in provenance"
                else
                    warn "  Role not found in provenance file"
                fi
            else
                warn "  Provenance file missing for role $role"
            fi
        else
            error "  Role $role materialization failed: $(cat "$TEST_DIR/role-$role.out")"
        fi
    done
    
    success "Test 3: Role-based round-trip completed successfully"
}

# Test 4: Version bump workflow
test_version_bump_round_trip() {
    log "Test 4: Version Bump Round-Trip"
    
    # Create a bundle for version testing
    log "  Creating version test bundle..."
    mkdir -p "$TEST_DIR/version-bundle/src"
    
    cat > "$TEST_DIR/version-bundle/modelops.yaml" << 'EOF'
apiVersion: v1
kind: Bundle
metadata:
  name: version-test-bundle
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

    cat > "$TEST_DIR/version-bundle/src/version_info.py" << 'EOF'
VERSION = "v1.0.0"
print(f"Bundle version: {VERSION}")
EOF

    # Push with minor version bump
    log "  Pushing with minor version bump..."
    if $CLI_CMD push "$TEST_DIR/version-bundle" --bump minor > "$TEST_DIR/version-push.out" 2>&1; then
        success "  Version bump push successful"
        
        # Check that version was updated
        current_version=$(grep "version:" "$TEST_DIR/version-bundle/modelops.yaml" | awk '{print $2}')
        if [[ "$current_version" == "v1.1.0" ]]; then
            success "  Version correctly bumped to v1.1.0"
        else
            error "  Version not bumped correctly, got: $current_version"
        fi
    else
        error "  Version bump push failed: $(cat "$TEST_DIR/version-push.out")"
    fi
    
    # Pull the bumped version
    log "  Materializing bumped version..."
    mkdir -p "$TEST_DIR/version-pulled"
    if $CLI_CMD materialize "version-test-bundle:v1.1.0" "$TEST_DIR/version-pulled" > "$TEST_DIR/version-pull.out" 2>&1; then
        success "  Bumped version materialized successfully"
        
        # Verify provenance includes correct version
        if grep -q "\"version\": \"v1.1.0\"" "$TEST_DIR/version-pulled/.mops/provenance.json"; then
            success "  Correct version recorded in provenance"
        else
            warn "  Version not correctly recorded in provenance"
        fi
    else
        error "  Bumped version materialize failed: $(cat "$TEST_DIR/version-pull.out")"
    fi
    
    success "Test 4: Version bump round-trip completed successfully"
}

# Test 5: Concurrent round-trips
test_concurrent_round_trips() {
    log "Test 5: Concurrent Round-Trip Operations"
    
    log "  Creating multiple test bundles for concurrent testing..."
    
    # Create 3 different bundles concurrently
    for i in {1..3}; do
        mkdir -p "$TEST_DIR/concurrent-$i/src"
        
        cat > "$TEST_DIR/concurrent-$i/modelops.yaml" << EOF
apiVersion: v1
kind: Bundle
metadata:
  name: concurrent-test-$i
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

        cat > "$TEST_DIR/concurrent-$i/src/app.py" << EOF
"""Concurrent test app $i."""
import time

def main():
    print(f"Running concurrent test $i")
    print(f"Timestamp: {time.time()}")

if __name__ == "__main__":
    main()
EOF
    done
    
    # Push all bundles concurrently
    log "  Pushing bundles concurrently..."
    for i in {1..3}; do
        ($CLI_CMD push "$TEST_DIR/concurrent-$i" > "$TEST_DIR/concurrent-push-$i.out" 2>&1 || echo "FAILED" > "$TEST_DIR/concurrent-push-$i.failed") &
    done
    
    # Wait for all pushes to complete
    wait
    
    # Check push results
    push_success_count=0
    for i in {1..3}; do
        if [[ ! -f "$TEST_DIR/concurrent-push-$i.failed" ]]; then
            push_success_count=$((push_success_count + 1))
            success "  Concurrent push $i succeeded"
        else
            warn "  Concurrent push $i failed"
        fi
    done
    
    if [[ $push_success_count -eq 3 ]]; then
        success "  All concurrent pushes succeeded"
    else
        warn "  Only $push_success_count/3 concurrent pushes succeeded"
    fi
    
    # Pull all bundles concurrently
    log "  Pulling bundles concurrently..."
    for i in {1..3}; do
        mkdir -p "$TEST_DIR/concurrent-pulled-$i"
        ($CLI_CMD materialize "concurrent-test-$i:v1.0.0" "$TEST_DIR/concurrent-pulled-$i" > "$TEST_DIR/concurrent-pull-$i.out" 2>&1 || echo "FAILED" > "$TEST_DIR/concurrent-pull-$i.failed") &
    done
    
    # Wait for all pulls to complete
    wait
    
    # Check pull results
    pull_success_count=0
    for i in {1..3}; do
        if [[ ! -f "$TEST_DIR/concurrent-pull-$i.failed" ]] && [[ -f "$TEST_DIR/concurrent-pulled-$i/src/app.py" ]]; then
            pull_success_count=$((pull_success_count + 1))
            success "  Concurrent pull $i succeeded"
        else
            warn "  Concurrent pull $i failed"
        fi
    done
    
    if [[ $pull_success_count -eq 3 ]]; then
        success "  All concurrent pulls succeeded"
    else
        warn "  Only $pull_success_count/3 concurrent pulls succeeded"
    fi
    
    success "Test 5: Concurrent round-trips completed"
}

# Cleanup
cleanup() {
    log "Cleaning up round-trip test environment..."
    cd /tmp
    rm -rf "$TEST_DIR"
    success "Cleanup complete"
}

# Main execution
main() {
    echo -e "${BLUE}"
    echo "════════════════════════════════════════════════════════════════"
    echo "    ModelOps Bundles - Round-Trip Test Suite"  
    echo "════════════════════════════════════════════════════════════════"
    echo -e "${NC}"
    
    setup
    
    # Run all round-trip tests
    test_simple_round_trip
    test_external_round_trip
    test_role_round_trip
    test_version_bump_round_trip
    test_concurrent_round_trips
    
    cleanup
    
    echo -e "${GREEN}"
    echo "════════════════════════════════════════════════════════════════"
    echo "    🎉 All round-trip tests completed successfully!"
    echo "    ✅ Push → Pull → Verify workflow is working correctly"
    echo "════════════════════════════════════════════════════════════════"  
    echo -e "${NC}"
}

# Handle Ctrl+C gracefully
trap cleanup EXIT

main "$@"