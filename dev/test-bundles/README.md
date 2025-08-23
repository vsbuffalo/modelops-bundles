# Test Bundles for Local Development

This directory contains sample bundles for testing the ModelOps Bundles system with real storage backends.

## Bundle Types

### 1. Simple Bundle (`simple-bundle/`)
- **Size**: ~2KB
- **Purpose**: Basic functionality testing
- **Layers**: code, config
- **Roles**: default, runtime
- **External Data**: None

**Usage**:
```bash
# Test basic materialization
uv run python -m modelops_bundles.cli materialize simple-bundle /tmp/simple-test

# Test export
uv run python -m modelops_bundles.cli export /tmp/simple-test /tmp/simple.tar.zst
```

### 2. Azure Bundle (`azure-bundle/`) 
- **Size**: ~3KB (code only)
- **Purpose**: External data integration testing
- **Layers**: code, data (external)
- **Roles**: default, training, runtime
- **External Data**: Azure Blob Storage references

**External Data Sources**:
- `training_data/` → `azure://devstoreaccount1/testdata/training/`
- `models/pretrained.pkl` → `azure://devstoreaccount1/models/pretrained-v1.0.0.pkl`

**Usage**:
```bash
# Test without external data
uv run python -m modelops_bundles.cli materialize azure-bundle /tmp/azure-test --role default

# Test with external data prefetching (requires Azurite)
uv run python -m modelops_bundles.cli materialize azure-bundle /tmp/azure-test --role training --prefetch-external
```

### 3. Large Bundle (`large-bundle/`)
- **Size**: ~75MB
- **Purpose**: Performance and stress testing
- **Layers**: code, data, models
- **Roles**: default, training, inference, full
- **Files**: 50MB training data + 25MB model file

**Usage**:
```bash
# Test large file handling
uv run python -m modelops_bundles.cli materialize large-bundle /tmp/large-test --role full

# Test streaming export
uv run python -m modelops_bundles.cli export /tmp/large-test /tmp/large.tar.zst
```

## Setting up Test Data for Azure Bundle

To test the Azure bundle with actual external data, you need to upload test files to Azurite:

```bash
# Start Azurite
docker-compose -f dev/docker-compose.yml up -d azurite

# Azure SDK is already included in project dependencies

# Upload test data to Azurite
uv run --dev python -c "
from azure.storage.blob import BlobServiceClient
import os

# Connect to Azurite
client = BlobServiceClient.from_connection_string(os.environ['AZURE_STORAGE_CONNECTION_STRING'])

# Create container
try:
    client.create_container('testdata')
    print('Created testdata container')
except:
    print('testdata container already exists')

try:
    client.create_container('models')
    print('Created models container')
except:
    print('models container already exists')

# Upload dummy training data
blob_client = client.get_blob_client(container='testdata', blob='training/sample.csv')
blob_client.upload_blob('id,label\n1,positive\n2,negative', overwrite=True)
print('Uploaded training data')

# Upload dummy model
blob_client = client.get_blob_client(container='models', blob='pretrained-v1.0.0.pkl')
blob_client.upload_blob(b'dummy_model_data', overwrite=True)
print('Uploaded pretrained model')
"
```

## Testing Strategy

1. **Development Loop**: Use `simple-bundle` for quick iteration and basic testing
2. **Integration Testing**: Use `azure-bundle` to test external data handling
3. **Performance Testing**: Use `large-bundle` to test streaming, compression, and performance
4. **CI/CD**: Run all bundles in automated test suites

## Integration with Test Suites

- **test-cli-fake.sh**: Uses FakeProvider, no real bundles needed
- **test-storage-real.sh**: Uses these test bundles with real Azure + OCI registry backends