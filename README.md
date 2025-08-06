# Redpanda Connect Iceberg Output Plugin

This plugin provides a Redpanda Connect output that writes data to Apache Iceberg tables.

## Features

- **Unified Plugin**: Single `iceberg` output type that supports both basic and advanced configurations
- **Catalog Support**: Apache Iceberg REST catalogs are supported
- **Flexible Schema Handling**: Automatic schema inference or explicit schema definition
- **Batch Processing**: Configurable batch size for optimal performance
- **Snapshot Properties**: Custom metadata for Iceberg snapshots

## Compatibility

### Cloud Storage

| Provider | Status |
|----------|--------|
| Azure    | ✅ Supported |
| AWS      | ❌ Not Supported |
| GCP      | ❌ Not Supported |

### Catalog Implementations Tested

| Implementation | Status |
|----------------|--------|
| [Apache Polaris](https://github.com/apache/polaris) | ✅ Tested |
| [Unity Catalog](https://github.com/unitycatalog/unitycatalog) | ❌ Not Tested |
| [Snowflake Open Catalog](https://other-docs.snowflake.com/en/opencatalog/overview) | ❌ Not Tested |

### Iceberg Catalog Types Tested

| Catalog Type | Status |
|--------------|--------|
| REST         | ✅ Supported |
| Glue         | ❌ Not Supported |
| SQL          | ❌ Not Supported |
| Hive         | ❌ Not Supported |

## Configuration

### Basic Configuration

```yaml
output:
  iceberg:
    catalog_type: rest  # Only "rest" is supported
    catalog_properties:
      uri: http://localhost:8181/api/catalog
      credential: your_bearer_token
      token_type: bearer  # Only "bearer" is supported
      scope: your_scope
      warehouse: your_warehouse_name
    table_identifier: "my_namespace.my_table"
    batch_size: 1000
    snapshot_properties:
      operation: append
      source: redpanda-connect
```

### Advanced Configuration

```yaml
output:
  iceberg:
    catalog_type: rest  # Only "rest" is supported
    catalog_properties:
      uri: http://localhost:8181/api/catalog
      credential: your_bearer_token
      token_type: bearer  # Only "bearer" is supported
      scope: your_scope
      warehouse: your_warehouse_name
    table_identifier: "my_namespace.my_table"
    batch_size: 1000
    schema_inference: false
    explicit_schema:
      fields:
        - name: id
          type: int32
          required: true
        - name: name
          type: string
          required: false
        - name: email
          type: string
          required: false
        - name: age
          type: int32
          required: false
        - name: active
          type: boolean
          required: false
        - name: created_at
          type: timestamp
          required: false
    azure_blob_storage:
      storage_account: your_storage_account
      storage_access_key: your_storage_access_key
    snapshot_properties:
      operation: append
      source: redpanda-connect
      batch_id: "batch-001"
```

## Configuration Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `catalog_type` | string | Yes | - | Must be "rest" (only REST catalog type is supported) |
| `catalog_properties` | map | Yes | - | Properties for REST catalog configuration (token_type must be "bearer") |
| `table_identifier` | string | Yes | - | Table identifier (namespace.table_name) |
| `batch_size` | int | No | 1000 | Number of records to batch before writing |
| `snapshot_properties` | map | No | - | Optional properties for snapshot metadata |
| `schema_inference` | bool | No | true | Whether to infer schema from JSON messages |
| `explicit_schema` | map | No | - | Explicit schema definition |

### Required catalog_properties for REST catalog

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `uri` | string | Yes | REST catalog URI |
| `credential` | string | Yes | Bearer token for authentication |
| `token_type` | string | Yes | Must be "bearer" |
| `scope` | string | Yes | OAuth2 scope (e.g., "PRINCIPAL_ROLE:ALL") |
| `warehouse` | string | Yes | Warehouse name |

## Examples

See the `config/` directory for example configurations:

- `iceberg_basic_example.yaml` - Basic configuration
- `iceberg_example.yaml` - Standard configuration
- `iceberg_advanced_example.yaml` - Advanced configuration with explicit schema

## Building

```bash
go build -o redpanda-connect-iceberg .
```

## Testing

```bash
go test ./...
```

## Usage

1. Build the plugin
2. Use it with Redpanda Connect
3. Configure your Iceberg catalog and table
4. Start streaming data to your Iceberg table

## License

Apache License 2.0
