package wal

import (
	"time"

	"github.com/google/uuid"
)

type BackupWalSegment struct {
	ID uuid.UUID `json:"id" gorm:"column:id;type:uuid;primaryKey"`

	BackupID  uuid.UUID `json:"backupId"  gorm:"column:backup_id;type:uuid;not null"`
	StorageID uuid.UUID `json:"storageId" gorm:"column:storage_id;type:uuid;not null"`

	WalFilename string  `json:"walFilename" gorm:"column:wal_filename;type:text;not null"`
	WalSizeMb   float64 `json:"walSizeMb"   gorm:"column:wal_size_mb;default:0"`

	CreatedAt time.Time `json:"createdAt" gorm:"column:created_at"`
}

func (BackupWalSegment) TableName() string {
	return "backup_wal_segments"
}
