// Package dialect provides manually-maintained metadata for the Databricks dialect.
// This file contains function classifications that cannot be extracted from documentation,
// such as generator functions (functions that produce values without input columns).
package dialect

// databricksGenerators contains functions that produce values without reading columns.
// These are not classified in Databricks documentation, so we maintain them manually.
var databricksGenerators = []string{
	"current_catalog",
	"current_database",
	"current_date",
	"current_metastore",
	"current_recipient",
	"current_schema",
	"current_timestamp",
	"current_timezone",
	"current_user",
	"current_version",
	"curdate",
	"e",
	"getdate",
	"monotonically_increasing_id",
	"now",
	"pi",
	"rand",
	"randn",
	"random",
	"randstr",
	"session_user",
	"spark_partition_id",
	"uniform",
	"user",
	"uuid",
	"version",
}

// --- Generated content ---
// The following variables are defined in generated files:
//   - functions_gen.go: databricksAggregates, databricksWindows, databricksTableFunctions, databricksFunctionDocs
//   - keywords_gen.go: databricksCompletionKeywords, databricksReservedWords
//   - types_gen.go: databricksTypes
//
// To regenerate, run:
//   go run ./scripts/gendatabricks -gen=all -outdir=pkg/adapters/databricks/dialect/
