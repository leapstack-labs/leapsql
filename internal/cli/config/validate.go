package config

import (
	"fmt"
	"os"

	intconfig "github.com/leapstack-labs/leapsql/internal/config"
)

// DefaultSchemaForType returns the default schema for a database type.
// This is a convenience wrapper that delegates to the shared config function.
func DefaultSchemaForType(dbType string) string {
	return intconfig.DefaultSchemaForType(dbType)
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.ModelsDir == "" {
		return fmt.Errorf("models_dir is required")
	}

	// Only validate directory existence if we're running a command that needs it
	// This allows help commands to work without a valid directory
	return nil
}

// ValidateDirectories checks if required directories exist.
func (c *Config) ValidateDirectories() error {
	if _, err := os.Stat(c.ModelsDir); os.IsNotExist(err) {
		return fmt.Errorf("models directory does not exist: %s\nHint: Create the directory or use --models-dir to specify a different path", c.ModelsDir)
	}
	return nil
}
