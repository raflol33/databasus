-- +goose Up
-- +goose StatementBegin
CREATE TABLE backup_wal_segments (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    backup_id    UUID             NOT NULL,
    storage_id   UUID             NOT NULL,
    wal_filename TEXT             NOT NULL,
    wal_size_mb  DOUBLE PRECISION NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

ALTER TABLE backup_wal_segments
    ADD CONSTRAINT fk_backup_wal_segments_backup_id
    FOREIGN KEY (backup_id)
    REFERENCES backups (id)
    ON DELETE CASCADE;

ALTER TABLE backup_wal_segments
    ADD CONSTRAINT fk_backup_wal_segments_storage_id
    FOREIGN KEY (storage_id)
    REFERENCES storages (id)
    ON DELETE RESTRICT;

CREATE INDEX idx_backup_wal_segments_backup_id ON backup_wal_segments (backup_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS backup_wal_segments;
-- +goose StatementEnd
