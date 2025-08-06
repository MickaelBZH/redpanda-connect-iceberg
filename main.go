package main

import (
	"context"

	"github.com/redpanda-data/benthos/v4/public/service"

	// Import full suite of FOSS connect plugins
	_ "github.com/redpanda-data/connect/public/bundle/free/v4"

	// Import our iceberg output plugin
	_ "github.com/redpanda-data/redpanda-connect-iceberg-output/output"
	
)

func main() {
	service.RunCLI(context.Background())
}
