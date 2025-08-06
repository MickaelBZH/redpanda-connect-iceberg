package output

import (
	"context"
	"fmt"
	"strings"
	"encoding/json"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/apache/iceberg-go"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/apache/iceberg-go/catalog/rest"
)

func init() {
	err := service.RegisterOutput(
		"iceberg", service.NewConfigSpec().
			Beta().
			Categories("Integration").
			Summary("Writes messages to an Iceberg table via REST catalog with bearer token authentication.").
			Description(`
This output writes messages to an Iceberg table. **Only REST catalog type is supported**, and **only bearer token authentication is supported** for the REST catalog.

## Supported Configuration

- ` + "`catalog_name`" + `: The name of the catalog to use
- ` + "`catalog_type`" + `: Must be "rest" (only REST catalog type is supported)
- ` + "`catalog_properties`" + `: Properties for the REST catalog configuration (token_type must be "bearer")
- ` + "`table_identifier`" + `: The table identifier (namespace.table_name)
- ` + "`batch_size`" + `: Number of records to batch before writing (default: 1000)
- ` + "`snapshot_properties`" + `: Optional properties for snapshot metadata
- ` + "`schema_inference`" + `: Whether to infer schema from JSON messages (default: true, optional)
- ` + "`explicit_schema`" + `: Explicit schema definition (optional)

## Limitations

- **Catalog Type**: Only "rest" catalog type is supported
- **Authentication**: Only "bearer" token_type is supported for REST catalog authentication

## Examples

Basic configuration:
` + "```yaml" + `
output:
  iceberg:
    catalog_name: my-catalog
    catalog_type: rest
    catalog_properties:
      uri: http://localhost:8181/api/catalog
      token_type: bearer
      credential: your-bearer-token
      scope: PRINCIPAL_ROLE:engineer
      warehouse: my-warehouse
    table_identifier: "default.my_table"
    batch_size: 1000
` + "```" + `

Advanced configuration:
` + "```yaml" + `
output:
  iceberg:
    catalog_name: my-catalog
    catalog_type: rest
    catalog_properties:
      uri: http://localhost:8181/api/catalog
      token_type: bearer
      credential: your-bearer-token
      scope: PRINCIPAL_ROLE:engineer
      warehouse: my-warehouse
    table_identifier: "default.my_table"
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
    snapshot_properties:
      operation: append
      source: redpanda-connect
      batch_id: "batch-001"
` + "```" + `
`).
			Fields(
				service.NewStringField("catalog_name").
					Description("The name of the catalog to use").
					Example("my-catalog"),
				service.NewStringField("catalog_type").
					Description("The type of catalog to use (only rest supported)").
					Example("rest"),
				service.NewAnyMapField("catalog_properties").
					Description("Properties for the catalog configuration").
					Example(map[string]interface{}{
						"uri":      "http://localhost:8181/api/catalog",
						"token_type": "bearer",
						"credential": "your-bearer-token",
						"scope": "PRINCIPAL_ROLE:engineer",
						"warehouse": "my-warehouse",
					}),
				service.NewStringField("table_identifier").
					Description("The table identifier in format namespace.table_name").
					Example("default.my_table"),
				service.NewIntField("batch_size").
					Description("Number of records to batch before writing").
					Default(1000),
				service.NewAnyMapField("snapshot_properties").
					Description("Optional properties for snapshot metadata").
					Optional(),
				service.NewBoolField("schema_inference").
					Description("Whether to infer schema from JSON messages").
					Default(true).
					Optional(),
				service.NewAnyMapField("explicit_schema").
					Description("Explicit schema definition").
					Optional(),
				service.NewAnyMapField("azure_blob_storage").
					Description("Azure Blob Storage authentication configuration").
					Optional(),
			),
		func(conf *service.ParsedConfig, mgr *service.Resources) (out service.Output, maxInFlight int, err error) {
			return newIcebergOutput(conf)
		})
	if err != nil {
		panic(err)
	}
}

// convertParsedConfigMap converts a map of *service.ParsedConfig to map[string]interface{}.
// This is necessary because FieldAnyMap returns *service.ParsedConfig values,
// but the struct expects interface{}.
func convertParsedConfigMap(config map[string]*service.ParsedConfig) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	for k, v := range config {
		var val interface{}
		val, _ = v.FieldAny() // Get raw value
		result[k] = val
	}
	return result, nil
}

//------------------------------------------------------------------------------

type icebergOutput struct {
	catalogName        string
	catalogType        string
	catalogProperties  map[string]interface{}
	tableIdentifier    string
	batchSize          int
	snapshotProperties map[string]interface{}
	schemaInference    bool
	explicitSchema     map[string]interface{}
	azureBlobStorage   map[string]interface{}

	catalog catalog.Catalog
	table   *table.Table
	buffer  []map[string]interface{}
	schema  *arrow.Schema
}

func newIcebergOutput(conf *service.ParsedConfig) (service.Output, int, error) {
	catalogName, err := conf.FieldString("catalog_name")
	if err != nil {
		return nil, 0, err
	}

	catalogType, err := conf.FieldString("catalog_type")
	if err != nil {
		return nil, 0, err
	}

	rawCatalogProperties, err := conf.FieldAnyMap("catalog_properties")
	if err != nil {
		return nil, 0, err
	}
	catalogProperties, err := convertParsedConfigMap(rawCatalogProperties)
	if err != nil {
		return nil, 0, err
	}

	tableIdentifier, err := conf.FieldString("table_identifier")
	if err != nil {
		return nil, 0, err
	}

	batchSize, err := conf.FieldInt("batch_size")
	if err != nil {
		return nil, 0, err
	}

	rawSnapshotProperties, _ := conf.FieldAnyMap("snapshot_properties")
	snapshotProperties, err := convertParsedConfigMap(rawSnapshotProperties)
	if err != nil {
		return nil, 0, err
	}
	schemaInference, _ := conf.FieldBool("schema_inference")
	rawExplicitSchema, _ := conf.FieldAnyMap("explicit_schema")
	explicitSchema, err := convertParsedConfigMap(rawExplicitSchema)
	if err != nil {
		return nil, 0, err
	}

	rawAzureBlobStorage, _ := conf.FieldAnyMap("azure_blob_storage")
	azureBlobStorage, err := convertParsedConfigMap(rawAzureBlobStorage)
	if err != nil {
		return nil, 0, err
	}

	return &icebergOutput{
		catalogName:        catalogName,
		catalogType:        catalogType,
		catalogProperties:  catalogProperties,
		tableIdentifier:    tableIdentifier,
		batchSize:          batchSize,
		snapshotProperties: snapshotProperties,
		schemaInference:    schemaInference,
		explicitSchema:     explicitSchema,
		azureBlobStorage:   azureBlobStorage,
		buffer:             make([]map[string]interface{}, 0),
	}, 1, nil
}

func (i *icebergOutput) Connect(ctx context.Context) error {
	// Validate catalog type - only REST is supported
	if i.catalogType != "rest" {
		return fmt.Errorf("unsupported catalog type: %s, only 'rest' catalog type is supported", i.catalogType)
	}

	// Validate token_type for REST catalog - only bearer is supported
	if tokenType, ok := i.catalogProperties["token_type"].(string); ok && tokenType != "bearer" {
		return fmt.Errorf("unsupported token_type: %s, only 'bearer' token_type is supported for REST catalog", tokenType)
	}

	parts := strings.SplitN(i.tableIdentifier, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid table identifier: %s, expected format namespace.table_name", i.tableIdentifier)
	}
	namespace := parts[0]
	tableName := parts[1]

	// Convert catalog properties to iceberg.Properties
	props := make(iceberg.Properties)
	for k, v := range i.catalogProperties {
		if str, ok := v.(string); ok {
			props[k] = str
		} else {
			props[k] = fmt.Sprintf("%v", v)
		}
	}

	// Add Azure account name to catalog properties for iceberg-go
	if i.azureBlobStorage != nil {
		if accName, ok := i.azureBlobStorage["storage_account"].(string); ok && accName != "" {
			props["adls.auth.shared-key.account.name"] = accName
		}

		// Add Azure access key to catalog properties for iceberg-go
		if accKey, ok := i.azureBlobStorage["storage_access_key"].(string); ok && accKey != "" {
			props["adls.auth.shared-key.account.key"] = accKey
		}

		// Add Azure protocol configuration
		props["adls.protocol"] = "https"
		
		// Add Azure Data Lake Storage Gen2 specific properties
		props["adls.endpoint-suffix"] = "core.windows.net"
		if accName, ok := i.azureBlobStorage["storage_account"].(string); ok && accName != "" {
			props["adls.account-name"] = accName
		}
	}
	
	// Tell iceberg-go to use catalog authentication for underlying storage
	props["io.use-catalog-auth"] = "true"

	// Determine catalog type from URI if not explicitly set
	if catalogType := props["catalog_type"]; catalogType == "" {
		if strings.HasPrefix(props["uri"], "http://") || strings.HasPrefix(props["uri"], "https://") {
			props["catalog_type"] = "rest"
		}
	}

	// Load the catalog
	cat, err := catalog.Load(ctx, i.catalogName, props)
	if err != nil {
		return fmt.Errorf("failed to load catalog: %w", err)
	}

	i.catalog = cat

	// Create table identifier
	tableIdent := table.Identifier{namespace, tableName}

	// Try to load existing table
	table, err := cat.LoadTable(ctx, tableIdent, props)
	if err != nil {
		// If table doesn't exist, create it
		if strings.Contains(err.Error(), "not found") || strings.Contains(err.Error(), "does not exist") {
			table, err = i.createTable(ctx, cat, tableIdent, props)
			if err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}
		} else {
			return fmt.Errorf("failed to load table: %w", err)
		}
	}

	i.table = table

	// Log connection details
	fmt.Printf("Connected to Iceberg catalog: %s (%s)\n", i.catalogName, i.catalogType)
	fmt.Printf("Table: %s.%s\n", namespace, tableName)
	fmt.Printf("Batch size: %d\n", i.batchSize)
	fmt.Printf("Schema inference: %v\n", i.schemaInference)

	return nil
}

func (i *icebergOutput) createTable(ctx context.Context, cat catalog.Catalog, tableIdent table.Identifier, props iceberg.Properties) (*table.Table, error) {
	// Create a simple schema that can be inferred from data
	// This will be updated when we receive the first message
	fields := []iceberg.NestedField{
		iceberg.NestedField{Name: "id", Type: iceberg.PrimitiveTypes.String, Required: false},
	}

	icebergSchema := iceberg.NewSchema(1, fields...)

	// Create the table
	return cat.CreateTable(ctx, tableIdent, icebergSchema)
}

func (i *icebergOutput) Write(ctx context.Context, msg *service.Message) error {
	// Get message as JSON
	content, err := msg.AsBytes()
	if err != nil {
		return fmt.Errorf("failed to get message bytes: %w", err)
	}

	// Parse JSON into map
	var record map[string]interface{}
	if err := json.Unmarshal(content, &record); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	// Infer schema from first record if needed
	if i.schema == nil && i.schemaInference {
		i.schema = i.inferSchema(record)
		fmt.Printf("Inferred schema from first record: %v\n", i.schema)
	}

	// Add to buffer
	i.buffer = append(i.buffer, record)

	// Flush if buffer is full
	if len(i.buffer) >= i.batchSize {
		return i.flushBuffer(ctx)
	}

	return nil
}

func (i *icebergOutput) Close(ctx context.Context) error {
	// Flush any remaining records
	if len(i.buffer) > 0 {
		if err := i.flushBuffer(ctx); err != nil {
			return fmt.Errorf("failed to flush final buffer: %w", err)
		}
	}
	return nil
}

func (i *icebergOutput) flushBuffer(ctx context.Context) error {
	if len(i.buffer) == 0 {
		return nil
	}

	fmt.Printf("Writing batch of %d records to table %s\n", len(i.buffer), i.tableIdentifier)

	// Convert records to Arrow format
	pool := memory.NewGoAllocator()
	records := make([]arrow.Record, 0, len(i.buffer))

	for _, record := range i.buffer {
		// Convert map to Arrow record
		arrowRecord, err := i.convertToArrowRecord(record, pool)
		if err != nil {
			return fmt.Errorf("failed to convert to Arrow record: %w", err)
		}
		records = append(records, arrowRecord)
	}

	// Combine all records into a single Arrow table
	if len(records) > 0 {
		// Create a table from the records
		arrowTable := array.NewTableFromRecords(i.schema, records)
		defer arrowTable.Release()

		// Convert snapshot properties
		snapshotProps := make(iceberg.Properties)
		for k, v := range i.snapshotProperties {
			if str, ok := v.(string); ok {
				snapshotProps[k] = str
			} else {
				snapshotProps[k] = fmt.Sprintf("%v", v)
			}
		}

		// Write to Iceberg table using AppendTable
		newTable, err := i.table.AppendTable(ctx, arrowTable, int64(i.batchSize), snapshotProps)
		if err != nil {
			return fmt.Errorf("failed to append table: %w", err)
		}

		// Update our table reference
		i.table = newTable

		fmt.Printf("Successfully wrote %d records to Iceberg table\n", len(records))
	}

	// Clear buffer
	i.buffer = i.buffer[:0]

	return nil
}

func (i *icebergOutput) inferSchema(data map[string]interface{}) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(data))
	
	for key, value := range data {
		fieldType := i.inferArrowType(value)
		fields = append(fields, arrow.Field{
			Name: key,
			Type: fieldType,
		})
	}
	
	return arrow.NewSchema(fields, nil)
}

func (i *icebergOutput) inferArrowType(value interface{}) arrow.DataType {
	switch v := value.(type) {
	case string:
		// Try to parse as timestamp with multiple formats
		formats := []string{
			time.RFC3339,                    // "2006-01-02T15:04:05Z07:00"
			"2006-01-02 15:04:05",          // "2004-06-06 13:12:13"
			"2006-01-02T15:04:05",          // ISO format without timezone
			"2006-01-02",                   // Date only
			"15:04:05",                     // Time only
		}
		
		for _, format := range formats {
			if _, err := time.Parse(format, v); err == nil {
				return &arrow.TimestampType{Unit: arrow.Millisecond}
			}
		}
		
		return arrow.BinaryTypes.String
	case int, int32, int64:
		return arrow.PrimitiveTypes.Int64
	case float32, float64:
		return arrow.PrimitiveTypes.Float64
	case bool:
		return arrow.FixedWidthTypes.Boolean
	case nil:
		return arrow.BinaryTypes.String // Default to string for null values
	default:
		// For complex types, convert to string
		return arrow.BinaryTypes.String
	}
}

func (i *icebergOutput) convertToArrowRecord(data map[string]interface{}, pool memory.Allocator) (arrow.Record, error) {
	// Use inferred schema or create one if not available
	schema := i.schema
	if schema == nil {
		schema = i.inferSchema(data)
	}

	builder := array.NewRecordBuilder(pool, schema)
	defer builder.Release()

	// Convert each field according to the schema
	for j, field := range schema.Fields() {
		value, exists := data[field.Name]
		if !exists {
			value = nil
		}

		if err := i.appendValue(builder.Field(j), value, field.Type); err != nil {
			return nil, fmt.Errorf("failed to append value for field %s: %w", field.Name, err)
		}
	}

	return builder.NewRecord(), nil
}

func (i *icebergOutput) appendValue(builder array.Builder, value interface{}, dataType arrow.DataType) error {
	if value == nil {
		builder.AppendNull()
		return nil
	}

	switch dataType.(type) {
	case *arrow.StringType:
		strBuilder := builder.(*array.StringBuilder)
		switch v := value.(type) {
		case string:
			strBuilder.Append(v)
		default:
			strBuilder.Append(fmt.Sprintf("%v", v))
		}

	case *arrow.Int64Type:
		intBuilder := builder.(*array.Int64Builder)
		switch v := value.(type) {
		case int:
			intBuilder.Append(int64(v))
		case int32:
			intBuilder.Append(int64(v))
		case int64:
			intBuilder.Append(v)
		case float64:
			intBuilder.Append(int64(v))
		default:
			intBuilder.Append(0)
		}

	case *arrow.Float64Type:
		floatBuilder := builder.(*array.Float64Builder)
		switch v := value.(type) {
		case float32:
			floatBuilder.Append(float64(v))
		case float64:
			floatBuilder.Append(v)
		case int:
			floatBuilder.Append(float64(v))
		case int32:
			floatBuilder.Append(float64(v))
		case int64:
			floatBuilder.Append(float64(v))
		default:
			floatBuilder.Append(0.0)
		}

	case *arrow.BooleanType:
		boolBuilder := builder.(*array.BooleanBuilder)
		switch v := value.(type) {
		case bool:
			boolBuilder.Append(v)
		default:
			boolBuilder.Append(false)
		}

	case *arrow.TimestampType:
		timestampBuilder := builder.(*array.TimestampBuilder)
		switch v := value.(type) {
		case string:
			// Try multiple timestamp formats
			formats := []string{
				time.RFC3339,                    // "2006-01-02T15:04:05Z07:00"
				"2006-01-02 15:04:05",          // "2004-06-06 13:12:13"
				"2006-01-02T15:04:05",          // ISO format without timezone
				"2006-01-02",                   // Date only
				"15:04:05",                     // Time only
			}
			
			for _, format := range formats {
				if t, err := time.Parse(format, v); err == nil {
					timestampBuilder.Append(arrow.Timestamp(t.UnixMilli()))
					return nil
				}
			}
			
			// If all parsing fails, log the value and use zero timestamp
			timestampBuilder.Append(arrow.Timestamp(0))
		default:
			timestampBuilder.Append(arrow.Timestamp(0))
		}

	default:
		// For unknown types, convert to string
		strBuilder := builder.(*array.StringBuilder)
		strBuilder.Append(fmt.Sprintf("%v", value))
	}

	return nil
} 
