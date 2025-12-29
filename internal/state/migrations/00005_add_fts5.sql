-- +goose Up
-- Add FTS5 search capabilities

-- Full-text search on models
CREATE VIRTUAL TABLE models_fts USING fts5(
    name,
    path,
    description,
    sql_content,
    content='models',
    content_rowid='rowid'
);

-- Triggers to keep FTS in sync
-- +goose StatementBegin
CREATE TRIGGER models_fts_insert AFTER INSERT ON models BEGIN
    INSERT INTO models_fts(rowid, name, path, description, sql_content)
    VALUES (new.rowid, new.name, new.path, new.description, new.sql_content);
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER models_fts_delete AFTER DELETE ON models BEGIN
    INSERT INTO models_fts(models_fts, rowid, name, path, description, sql_content)
    VALUES('delete', old.rowid, old.name, old.path, old.description, old.sql_content);
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER models_fts_update AFTER UPDATE ON models BEGIN
    INSERT INTO models_fts(models_fts, rowid, name, path, description, sql_content)
    VALUES('delete', old.rowid, old.name, old.path, old.description, old.sql_content);
    INSERT INTO models_fts(rowid, name, path, description, sql_content)
    VALUES (new.rowid, new.name, new.path, new.description, new.sql_content);
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER IF EXISTS models_fts_update;
DROP TRIGGER IF EXISTS models_fts_delete;
DROP TRIGGER IF EXISTS models_fts_insert;
DROP TABLE IF EXISTS models_fts;
