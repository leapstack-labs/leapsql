// Package docs provides SQLite database generation for documentation.
package docs

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"

	_ "modernc.org/sqlite" // SQLite driver (pure Go)
)

// CopyFromState copies the state database to the output path for docs.
// This is the production approach: state.db already has all the data and views,
// so we just copy it and optimize for HTTP range requests.
func CopyFromState(statePath, outputPath string) error {
	// Copy the file
	if err := copyFile(statePath, outputPath); err != nil {
		return fmt.Errorf("failed to copy state database: %w", err)
	}

	// Open and VACUUM for optimization (better compression, contiguous pages)
	db, err := sql.Open("sqlite", outputPath)
	if err != nil {
		return fmt.Errorf("failed to open copied database: %w", err)
	}
	defer func() { _ = db.Close() }()

	// VACUUM to optimize for range requests
	if _, err := db.ExecContext(context.Background(), "VACUUM"); err != nil {
		return fmt.Errorf("failed to vacuum database: %w", err)
	}

	return nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	srcFile, err := os.Open(src) //nolint:gosec // G304: src is from trusted source
	if err != nil {
		return err
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.Create(dst) //nolint:gosec // G304: dst is from trusted source
	if err != nil {
		return err
	}
	defer func() { _ = dstFile.Close() }()

	_, err = io.Copy(dstFile, srcFile)
	return err
}

// extractFolder extracts the folder from a model path (e.g., "staging.customers" -> "staging").
func extractFolder(path string) string {
	for i := 0; i < len(path); i++ {
		if path[i] == '.' {
			return path[:i]
		}
	}
	return "default"
}

// nullString returns a sql.NullString for optional string fields.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}
