package state

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/leapstack-labs/leapsql/internal/state/sqlcgen"
)

// GetContentHash retrieves the content hash for a file path.
func (s *SQLiteStore) GetContentHash(filePath string) (string, error) {
	if s.db == nil {
		return "", fmt.Errorf("database not opened")
	}

	hash, err := s.queries.GetContentHash(ctx(), filePath)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil // Not found, return empty string
	}
	if err != nil {
		return "", fmt.Errorf("failed to get content hash: %w", err)
	}

	return hash, nil
}

// SetContentHash stores the content hash for a file path.
func (s *SQLiteStore) SetContentHash(filePath, hash, fileType string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.SetContentHash(ctx(), sqlcgen.SetContentHashParams{
		FilePath:    filePath,
		ContentHash: hash,
		FileType:    fileType,
	})
}

// DeleteContentHash removes the content hash for a file path.
func (s *SQLiteStore) DeleteContentHash(filePath string) error {
	if s.db == nil {
		return fmt.Errorf("database not opened")
	}

	return s.queries.DeleteContentHash(ctx(), filePath)
}
