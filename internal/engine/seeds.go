package engine

// seeds.go - CSV seed data loading

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LoadSeeds loads all CSV files from the seeds directory into the database.
func (e *Engine) LoadSeeds(ctx context.Context) error {
	if e.seedsDir == "" {
		return nil
	}

	e.logger.Debug("loading seeds", "seeds_dir", e.seedsDir)

	// Ensure database is connected before loading seeds
	if err := e.ensureDBConnected(ctx); err != nil {
		return err
	}

	entries, err := os.ReadDir(e.seedsDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No seeds directory is OK
		}
		return fmt.Errorf("failed to read seeds directory: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".csv") {
			continue
		}

		tableName := strings.TrimSuffix(entry.Name(), ".csv")
		csvPath := filepath.Join(e.seedsDir, entry.Name())

		e.logger.Debug("loading seed file", "table", tableName, "path", csvPath)

		if err := e.db.LoadCSV(ctx, tableName, csvPath); err != nil {
			return fmt.Errorf("failed to load seed %s: %w", entry.Name(), err)
		}
	}

	return nil
}
