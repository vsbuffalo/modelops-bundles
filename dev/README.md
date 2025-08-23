# ModelOps Bundles Local Development Stack

This directory contains configuration for the local development environment with real storage backends.

## Components

### 🟦 Azurite (Azure Blob Storage Emulator)
- **Port**: 10000 (Blob), 10001 (Queue), 10002 (Table)
- **Purpose**: Local Azure Blob Storage for external data testing
- **Web Interface**: None (use Azure Storage Explorer or CLI)

### 📦 Local OCI Registry  
- **Port**: 5555 (avoiding conflict with Apple AirPlay on 5000)
- **Purpose**: Local container registry for bundle storage
- **API**: Full OCI Distribution Specification v1.1

### 🖥️ Registry UI
- **Port**: 8080
- **Purpose**: Web interface to browse registry contents
- **URL**: http://localhost:8080

## Quick Start

1. **Start the dev stack**:
   ```bash
   # From project root - starts all services
   make up
   ```

2. **Run tests**:
   ```bash
   # Run all tests (starts services automatically)
   make test
   
   # Or run specific test suites
   make test-fast    # CLI tests (no Docker needed)  
   make test-real    # Storage integration tests
   make unit         # Python unit tests
   ```

3. **Check service status**:
   ```bash
   make ps           # Service status
   make logs         # View all logs
   ```

4. **Clean up**:
   ```bash
   make down         # Stop services
   make clean        # Full cleanup
   ```

## Test Suites

We provide two complementary test suites for different development needs:

### 🚀 Fast CLI Tests (`test-cli-fake.sh`)
- **Purpose**: CLI functionality validation with FakeProvider
- **Speed**: ~10 seconds
- **Dependencies**: None (just `uv install`)
- **Use Cases**: 
  - Development inner loop
  - CI/CD for fast feedback
  - Regression testing CLI interface

**Run**: `make test-fast`

**Tests**:
- Basic CLI commands (resolve, materialize, export)
- Role selection and precedence
- Windows path handling
- Error scenarios and exit codes
- CLI options (verbose, cache, overwrite)

### 🔧 Real Storage Tests (`test-storage-real.sh`)
- **Purpose**: Integration testing with real storage backends
- **Speed**: ~2-3 minutes
- **Dependencies**: Docker services running (`make up`)
- **Azure SDK**: Already included in project dependencies
- **Use Cases**:
  - Pre-deployment validation
  - Storage integration testing
  - Performance benchmarking

**Run**: `make test-real` (starts services automatically)

**Tests**:
- Azure Blob Storage connectivity (via Azurite)
- OCI Registry operations
- Bundle lifecycle (create → push → pull)
- Large file handling and streaming
- Concurrent operations
- Error scenarios and recovery

## Sample Test Bundles

The `dev/test-bundles/` directory contains sample bundles for testing:

- **`simple-bundle/`**: Basic 2KB bundle for quick testing
- **`azure-bundle/`**: Bundle with Azure external data references  
- **`large-bundle/`**: 75MB bundle for performance testing

See `dev/test-bundles/README.md` for detailed usage instructions.

## Testing Workflows

### Development Workflow
```bash
# 1. Quick validation during development
make test-fast

# 2. Test specific functionality
uv run python -m modelops_bundles.cli resolve bundle:v1.0.0 --provider fake
uv run python -m modelops_bundles.cli materialize bundle:v1.0.0 /tmp/test --provider fake

# 3. Real storage integration (when needed)
make test-real
```

### Pre-Commit Workflow
```bash
# Run fast tests before committing
make test-fast
make unit
```

### Release Testing Workflow
```bash
# Full test suite before release
make test
```

## Development Tips

- **Registry UI**: Browse bundles at http://localhost:8080
- **Azurite Data**: Persisted in Docker volume `azurite_data`  
- **Registry Data**: Persisted in Docker volume `registry_data`
- **Reset Storage**: `make clean` to clear all data and volumes
- **Logs**: `make logs` to follow all service logs, or `make logs-azurite`/`make logs-registry` for specific services

## Configuration Files

- `dev/docker-compose.yml`: Service definitions
- `dev/registry-config.yml`: OCI registry configuration  
- `dev/dev.env`: Environment variables for development
- `dev/test-cli-fake.sh`: Fast CLI tests with FakeProvider
- `dev/test-storage-real.sh`: Real storage integration tests
- `dev/test-bundles/`: Sample test bundles for development

## Troubleshooting

### Services won't start
```bash
# Check if ports are in use
lsof -i :5555 -i :8080 -i :10000

# Check service logs
make logs
```

### Connection errors
```bash  
# Verify services are healthy
make ps

# Test connectivity
curl -v http://localhost:5555/v2/
curl -v "http://localhost:10000/devstoreaccount1?comp=properties"
```

### Storage issues
```bash
# Reset all data
make clean
make up

# Check storage usage
docker system df
```