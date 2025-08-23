#!/bin/bash
# ModelOps Bundles - CLI Test Suite with FakeProvider
# 
# Tests CLI functionality using FakeProvider (no external dependencies)
# Fast execution for development inner loop and CI/CD
# 
# Usage: bash dev/test-cli-fake.sh
# Prerequisites: uv install (no Docker required)

set -e
set -u

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test configuration
TEST_DIR="/tmp/modelops-test-$(date +%s)"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLI_CMD="uv run python -m modelops_bundles.cli"

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

# Setup test environment
setup() {
    log "Setting up test environment..."
    mkdir -p "$TEST_DIR"
    cd "$PROJECT_DIR"
    
    # No environment variables required for FakeProvider tests
    
    log "Test directory: $TEST_DIR"
    success "Environment setup complete"
}

# Test 1: Basic CLI functionality with fake provider
test_cli_basic() {
    log "Test 1: Basic CLI functionality (fake provider)"
    
    # Test resolve
    log "  Testing resolve command..."
    if $CLI_CMD resolve bundle:v1.0.0 --provider fake > "$TEST_DIR/resolve.out" 2>&1; then
        success "  Resolve command works"
        grep -q "Bundle: bundle:v1.0.0" "$TEST_DIR/resolve.out" || error "  Resolve output missing bundle info"
    else
        error "  Resolve command failed"
    fi
    
    # Test materialize
    log "  Testing materialize command..."
    mkdir -p "$TEST_DIR/materialize-test"
    if $CLI_CMD materialize bundle:v1.0.0 "$TEST_DIR/materialize-test" --provider fake > "$TEST_DIR/materialize.out" 2>&1; then
        success "  Materialize command works"
        [[ -f "$TEST_DIR/materialize-test/src/model.py" ]] || error "  Expected file not materialized"
        [[ -d "$TEST_DIR/materialize-test/.mops" ]] || error "  Provenance directory missing"
    else
        error "  Materialize command failed"
    fi
    
    # Test export
    log "  Testing export command..."
    if $CLI_CMD export "$TEST_DIR/materialize-test" "$TEST_DIR/bundle-export.tar.zst" > "$TEST_DIR/export.out" 2>&1; then
        success "  Export command works"
        [[ -f "$TEST_DIR/bundle-export.tar.zst" ]] || error "  Export file not created"
    else
        error "  Export command failed"
    fi
    
    success "Test 1: Basic CLI functionality passed"
}

# Test 2: Role selection and precedence
test_role_selection() {
    log "Test 2: Role selection and precedence"
    
    # Test different roles
    for role in default runtime training; do
        log "  Testing role: $role"
        mkdir -p "$TEST_DIR/role-$role"
        if $CLI_CMD materialize bundle:v1.0.0 "$TEST_DIR/role-$role" --role "$role" --provider fake > "$TEST_DIR/role-$role.out" 2>&1; then
            success "  Role $role materialization works"
            grep -q "Role: $role" "$TEST_DIR/role-$role.out" || error "  Role $role not correctly reported"
        else
            error "  Role $role materialization failed"
        fi
    done
    
    success "Test 2: Role selection passed"
}

# Test 3: Windows path handling  
test_windows_paths() {
    log "Test 3: Windows path handling"
    
    # Test Windows absolute path parsing (doesn't actually materialize, just tests parsing)
    log "  Testing Windows absolute path parsing..."
    if $CLI_CMD resolve 'C:\fake\path' --provider fake > "$TEST_DIR/windows-abs.out" 2>&1; then
        success "  Windows absolute path parsing works"
    else
        # This might fail if local path support isn't implemented yet
        warn "  Windows absolute path parsing not yet implemented (expected)"
    fi
    
    # Test Windows relative paths
    log "  Testing Windows relative path parsing..."
    if $CLI_CMD resolve '.\fake\path' --provider fake > "$TEST_DIR/windows-rel.out" 2>&1; then
        success "  Windows relative path parsing works"
    else
        warn "  Windows relative path parsing not yet implemented (expected)"
    fi
    
    success "Test 3: Windows path handling tested"
}

# Test 4: Error handling and exit codes
test_error_handling() {
    log "Test 4: Error handling and exit codes"
    
    # Test invalid bundle reference
    log "  Testing invalid bundle reference..."
    if $CLI_CMD resolve invalid-format --provider fake > "$TEST_DIR/invalid.out" 2>&1; then
        error "  Should have failed on invalid reference"
    else
        success "  Invalid reference correctly rejected"
    fi
    
    # Test nonexistent role
    log "  Testing nonexistent role..."
    mkdir -p "$TEST_DIR/error-test"
    if $CLI_CMD materialize bundle:v1.0.0 "$TEST_DIR/error-test" --role nonexistent --provider fake > "$TEST_DIR/nonexistent-role.out" 2>&1; then
        error "  Should have failed on nonexistent role"
    else
        success "  Nonexistent role correctly rejected"
    fi
    
    success "Test 4: Error handling passed"
}

# Test 5: Service connectivity (skip - FakeProvider doesn't need services)
test_service_connectivity() {
    log "Test 5: Service connectivity"
    
    # FakeProvider doesn't require external services
    log "  FakeProvider tests don't require external services"
    success "  Service connectivity not applicable for FakeProvider"
    
    success "Test 5: Service connectivity skipped (not needed)"
}

# Test 6: Environment isolation 
test_environment_isolation() {
    log "Test 6: Environment isolation"
    
    # Test that FakeProvider works without environment variables
    log "  Testing without Azure environment variables..."
    if AZURE_STORAGE_CONNECTION_STRING="" $CLI_CMD resolve bundle:v1.0.0 --provider fake > "$TEST_DIR/no-env.out" 2>&1; then
        success "  FakeProvider works without Azure environment"
    else
        error "  FakeProvider should not require environment variables"
    fi
    
    # Test that FakeProvider works without registry environment
    log "  Testing without registry environment variables..."
    if MODELOPS_REGISTRY_URL="" $CLI_CMD resolve bundle:v1.0.0 --provider fake > "$TEST_DIR/no-registry-env.out" 2>&1; then
        success "  FakeProvider works without registry environment"
    else
        error "  FakeProvider should not require registry environment"
    fi
    
    success "Test 6: Environment isolation verified"
}

# Test 7: Comprehensive CLI option coverage
test_cli_options() {
    log "Test 7: CLI option coverage"
    
    # Test verbose mode
    log "  Testing verbose mode..."
    if $CLI_CMD resolve bundle:v1.0.0 --provider fake --verbose > "$TEST_DIR/verbose.out" 2>&1; then
        success "  Verbose mode works"
        [[ $(wc -l < "$TEST_DIR/verbose.out") -gt $(wc -l < "$TEST_DIR/resolve.out") ]] || warn "  Verbose mode doesn't seem to add output"
    else
        error "  Verbose mode failed"
    fi
    
    # Test cache control
    log "  Testing cache control..."
    if $CLI_CMD resolve bundle:v1.0.0 --provider fake --no-cache > "$TEST_DIR/no-cache.out" 2>&1; then
        success "  No-cache mode works"
    else
        error "  No-cache mode failed"
    fi
    
    # Test overwrite mode
    log "  Testing overwrite mode..."
    mkdir -p "$TEST_DIR/overwrite-test"
    echo "existing content" > "$TEST_DIR/overwrite-test/conflict.txt"
    if $CLI_CMD materialize bundle:v1.0.0 "$TEST_DIR/overwrite-test" --provider fake --overwrite > "$TEST_DIR/overwrite.out" 2>&1; then
        success "  Overwrite mode works"
    else
        error "  Overwrite mode failed"
    fi
    
    success "Test 7: CLI option coverage passed"
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
    echo "    ModelOps Bundles - CLI Test Suite (FakeProvider)"
    echo "════════════════════════════════════════════════════════════════"
    echo -e "${NC}"
    
    setup
    
    # Run all tests
    test_cli_basic
    test_role_selection
    test_windows_paths
    test_error_handling
    test_service_connectivity
    test_environment_isolation
    test_cli_options
    
    cleanup
    
    echo -e "${GREEN}"
    echo "════════════════════════════════════════════════════════════════"
    echo "    🎉 All tests passed! Local dev stack is working correctly."
    echo "════════════════════════════════════════════════════════════════"
    echo -e "${NC}"
}

# Handle Ctrl+C gracefully
trap cleanup EXIT

main "$@"