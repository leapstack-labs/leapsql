-- +goose Up
-- Add render_ms column to track template rendering time separately from execution time
ALTER TABLE model_runs ADD COLUMN render_ms INTEGER DEFAULT 0;

-- +goose Down
ALTER TABLE model_runs DROP COLUMN render_ms;
