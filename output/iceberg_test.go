package output

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIcebergOutputConnect(t *testing.T) {
	output := &icebergOutput{
		catalogType:       "rest",
		tableIdentifier:   "default.test_table",
		batchSize:         100,
		catalogProperties: map[string]interface{}{
			"uri":        "http://localhost:8181",
			"token_type": "bearer",
			"credential": "test_token",
		},
		schemaInference: true,
	}

	// Test connection - this should fail because we can't actually connect to a catalog
	// but our validation should pass
	err := output.Connect(context.Background())
	// We expect this to fail at catalog creation, not at validation
	require.Error(t, err)
}

// TestIcebergOutputWrite requires a real catalog connection, skipping for now
// func TestIcebergOutputWrite(t *testing.T) {
// 	output := &icebergOutput{
// 		catalogType:       "rest",
// 		tableIdentifier:   "default.test_table",
// 		batchSize:         3,
// 		catalogProperties: map[string]interface{}{
// 			"uri":        "http://localhost:8181",
// 			"token_type": "bearer",
// 			"credential": "test_token",
// 		},
// 		schemaInference: true,
// 		buffer:          make([]map[string]interface{}, 0),
// 	}
//
// 	// Connect - expect error since we can't actually connect to catalog
// 	err := output.Connect(context.Background())
// 	require.Error(t, err)
//
// 	// Write a message
// 	msg := service.NewMessage([]byte(`{"test": "data"}`))
// 	err = output.Write(context.Background(), msg)
// 	require.NoError(t, err)
//
// 	// Write another message
// 	msg2 := service.NewMessage([]byte(`{"test2": "data2"}`))
// 	err = output.Write(context.Background(), msg2)
// 	require.NoError(t, err)
//
// 	// Write a third message (should trigger flush)
// 	msg3 := service.NewMessage([]byte(`{"test3": "data3"}`))
// 	err = output.Write(context.Background(), msg3)
// 	require.NoError(t, err)
//
// 	// Close
// 	err = output.Close(context.Background())
// 	require.NoError(t, err)
// }

// TestIcebergOutputWithExplicitSchema requires a real catalog connection, skipping for now
// func TestIcebergOutputWithExplicitSchema(t *testing.T) {
// 	output := &icebergOutput{
// 		catalogType:       "rest",
// 		tableIdentifier:   "default.test_table",
// 		batchSize:         100,
// 		catalogProperties: map[string]interface{}{},
// 		schemaInference:   false,
// 		explicitSchema: map[string]interface{}{
// 			"fields": []interface{}{
// 				map[string]interface{}{
// 					"name":     "id",
// 					"type":     "int32",
// 					"required": true,
// 				},
// 				map[string]interface{}{
// 					"name":     "name",
// 					"type":     "string",
// 					"required": false,
// 				},
// 			},
// 		},
// 	}
//
// 	// Test connection
// 	err := output.Connect(context.Background())
// 	require.NoError(t, err)
// }

func TestIcebergOutputInvalidTableIdentifier(t *testing.T) {
	output := &icebergOutput{
		catalogType:       "rest",
		tableIdentifier:   "invalid_table_name",
		batchSize:         100,
		catalogProperties: map[string]interface{}{},
		schemaInference:   true,
	}

	// Test connection with invalid table identifier
	err := output.Connect(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid table identifier")
}

func TestIcebergOutputValidation(t *testing.T) {
	tests := []struct {
		name           string
		catalogType    string
		catalogProps   map[string]interface{}
		expectedError  string
	}{
		{
			name:        "unsupported catalog type",
			catalogType: "glue",
			catalogProps: map[string]interface{}{
				"token_type": "bearer",
			},
			expectedError: "unsupported catalog type: glue, only 'rest' catalog type is supported",
		},
		{
			name:        "unsupported token type",
			catalogType: "rest",
			catalogProps: map[string]interface{}{
				"token_type": "oauth2",
			},
			expectedError: "unsupported token_type: oauth2, only 'bearer' token_type is supported for REST catalog",
		},
		{
			name:        "valid rest catalog with bearer token",
			catalogType: "rest",
			catalogProps: map[string]interface{}{
				"uri":        "http://localhost:8181",
				"token_type": "bearer",
				"credential": "test_token",
			},
			expectedError: "", // Should pass validation, but fail on actual connection
		},
		{
			name:        "rest catalog without token_type (should pass validation)",
			catalogType: "rest",
			catalogProps: map[string]interface{}{
				"uri":        "http://localhost:8181",
				"credential": "test_token",
			},
			expectedError: "", // Should pass validation since token_type is optional
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := &icebergOutput{
				catalogType:       tt.catalogType,
				tableIdentifier:   "default.test_table",
				batchSize:         100,
				catalogProperties: tt.catalogProps,
				schemaInference:   true,
			}

			err := output.Connect(context.Background())
			
			if tt.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.expectedError)
			} else {
				// For valid configurations, we expect connection to fail on catalog creation
				// but not on our validation
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "unsupported catalog type")
				assert.NotContains(t, err.Error(), "unsupported token_type")
			}
		})
	}
}
