package engine

// helpers.go - Utility functions for the engine package

// pathToTableName converts a model path to a SQL table name.
// e.g., "staging.customers" -> "staging.customers"
func pathToTableName(path string) string {
	return path
}
