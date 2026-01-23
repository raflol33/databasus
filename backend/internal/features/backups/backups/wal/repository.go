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

func (r *BackupWalRepository) SumByBackupIDs(backupIDs []uuid.UUID) (map[uuid.UUID]float64, error) {
	type walSizeResult struct {
		BackupID       uuid.UUID `gorm:"column:backup_id"`
		TotalWalSizeMb float64   `gorm:"column:total_wal_size_mb"`
	}

	if len(backupIDs) == 0 {
		return map[uuid.UUID]float64{}, nil
	}

	var results []walSizeResult
	if err := storage.GetDb().
		Model(&BackupWalSegment{}).
		Select("backup_id, COALESCE(SUM(wal_size_mb), 0) AS total_wal_size_mb").
		Where("backup_id IN ?", backupIDs).
		Group("backup_id").
		Scan(&results).Error; err != nil {
		return nil, err
	}

	totals := make(map[uuid.UUID]float64, len(results))
	for _, result := range results {
		totals[result.BackupID] = result.TotalWalSizeMb
	}

	return totals, nil
}

func (r *BackupWalRepository) DeleteByID(id uuid.UUID) error {
	return storage.GetDb().Delete(&BackupWalSegment{}, "id = ?", id).Error
}

func (r *BackupWalRepository) DeleteByBackupID(backupID uuid.UUID) error {
	return storage.GetDb().Delete(&BackupWalSegment{}, "backup_id = ?", backupID).Error
}
