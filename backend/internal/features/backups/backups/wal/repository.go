package wal

import (
	"errors"
	"time"

	"databasus-backend/internal/storage"

	"github.com/google/uuid"
)

type BackupWalRepository struct{}

func (r *BackupWalRepository) Save(segment *BackupWalSegment) error {
	if segment.BackupID == uuid.Nil || segment.StorageID == uuid.Nil {
		return errors.New("backup ID and storage ID are required")
	}

	if segment.ID == uuid.Nil {
		segment.ID = uuid.New()
	}

	if segment.CreatedAt.IsZero() {
		segment.CreatedAt = time.Now().UTC()
	}

	return storage.GetDb().Create(segment).Error
}

func (r *BackupWalRepository) FindByBackupID(backupID uuid.UUID) ([]*BackupWalSegment, error) {
	var segments []*BackupWalSegment

	if err := storage.
		GetDb().
		Where("backup_id = ?", backupID).
		Order("created_at ASC").
		Find(&segments).Error; err != nil {
		return nil, err
	}

	return segments, nil
}

func (r *BackupWalRepository) DeleteByID(id uuid.UUID) error {
	return storage.GetDb().Delete(&BackupWalSegment{}, "id = ?", id).Error
}

func (r *BackupWalRepository) DeleteByBackupID(backupID uuid.UUID) error {
	return storage.GetDb().Delete(&BackupWalSegment{}, "backup_id = ?", backupID).Error
}
