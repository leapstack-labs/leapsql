-- +goose Up
-- Add sql_content, raw_content, description columns to models table

ALTER TABLE models ADD COLUMN sql_content TEXT DEFAULT '';
ALTER TABLE models ADD COLUMN raw_content TEXT DEFAULT '';
ALTER TABLE models ADD COLUMN description TEXT DEFAULT '';

-- +goose Down
-- SQLite doesn't support DROP COLUMN easily before 3.35.0
-- For older SQLite versions, this would need table recreation
-- Since we're in dev, users can delete .leapsql/ to reset
