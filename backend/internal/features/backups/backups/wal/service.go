package wal

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	backups_core "databasus-backend/internal/features/backups/backups/core"
	"databasus-backend/internal/features/storages"
	"databasus-backend/internal/util/encryption"

	"github.com/google/uuid"
)

type BackupWalService struct {
	walRepository  *BackupWalRepository
	storageService *storages.StorageService
	fieldEncryptor encryption.FieldEncryptor
	logger         *slog.Logger
}

func (s *BackupWalService) CreateWalSegment(
	ctx context.Context,
	backupID uuid.UUID,
	storageID uuid.UUID,
	walFilename string,
	filePath string,
) (*BackupWalSegment, error) {
	storage, err := s.storageService.GetStorageByID(storageID)
	if err != nil {
		return nil, err
	}

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			s.logger.Error("Failed to close WAL file", "error", closeErr)
		}
	}()

	info, err := file.Stat()
	if err != nil {
		return nil, err
	}

	segment := &BackupWalSegment{
		ID:          uuid.New(),
		BackupID:    backupID,
		StorageID:   storageID,
		WalFilename: walFilename,
		WalSizeMb:   float64(info.Size()) / (1024 * 1024),
		CreatedAt:   time.Now().UTC(),
	}

	if err := storage.SaveFile(ctx, s.fieldEncryptor, s.logger, segment.ID, file); err != nil {
		return nil, err
	}

	if err := s.walRepository.Save(segment); err != nil {
		return nil, err
	}

	return segment, nil
}

func (s *BackupWalService) DeleteWalSegmentsForBackup(backup *backups_core.Backup) error {
	segments, err := s.walRepository.FindByBackupID(backup.ID)
	if err != nil {
		return err
	}

	for _, segment := range segments {
		storage, storageErr := s.storageService.GetStorageByID(segment.StorageID)
		if storageErr != nil {
			return storageErr
		}

		if err := storage.DeleteFile(s.fieldEncryptor, segment.ID); err != nil {
			s.logger.Error(
				"Failed to delete WAL segment file",
				"segmentId",
				segment.ID,
				"error",
				err,
			)
		}

		if err := s.walRepository.DeleteByID(segment.ID); err != nil {
			return err
		}
	}

	return nil
}

func (s *BackupWalService) OnBeforeBackupRemove(backup *backups_core.Backup) error {
	if err := s.DeleteWalSegmentsForBackup(backup); err != nil {
		return fmt.Errorf("failed to delete WAL segments: %w", err)
	}

	return nil
}
