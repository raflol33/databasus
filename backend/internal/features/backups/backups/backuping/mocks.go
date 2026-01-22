package backuping

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	common "databasus-backend/internal/features/backups/backups/common"
	backups_config "databasus-backend/internal/features/backups/config"
	"databasus-backend/internal/features/databases"
	"databasus-backend/internal/features/notifiers"
	"databasus-backend/internal/features/storages"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
)

type MockNotificationSender struct {
	mock.Mock
}

func (m *MockNotificationSender) SendNotification(
	notifier *notifiers.Notifier,
	title string,
	message string,
) {
	m.Called(notifier, title, message)
}

type CreateFailedBackupUsecase struct{}

func (uc *CreateFailedBackupUsecase) Execute(
	ctx context.Context,
	backupID uuid.UUID,
	backupType backups_core.BackupType,
	backupConfig *backups_config.BackupConfig,
	database *databases.Database,
	storage *storages.Storage,
	backupProgressListener func(completedMBs float64),
) (*common.BackupMetadata, error) {
	backupProgressListener(10)
	return nil, errors.New("backup failed")
}

type CreateSuccessBackupUsecase struct{}

func (uc *CreateSuccessBackupUsecase) Execute(
	ctx context.Context,
	backupID uuid.UUID,
	backupType backups_core.BackupType,
	backupConfig *backups_config.BackupConfig,
	database *databases.Database,
	storage *storages.Storage,
	backupProgressListener func(completedMBs float64),
) (*common.BackupMetadata, error) {
	backupProgressListener(10)
	return &common.BackupMetadata{
		EncryptionSalt: nil,
		EncryptionIV:   nil,
		Encryption:     backups_config.BackupEncryptionNone,
	}, nil
}

// CreateLargeBackupUsecase simulates a large backup (10000 MB)
type CreateLargeBackupUsecase struct{}

func (uc *CreateLargeBackupUsecase) Execute(
	ctx context.Context,
	backupID uuid.UUID,
	backupType backups_core.BackupType,
	backupConfig *backups_config.BackupConfig,
	database *databases.Database,
	storage *storages.Storage,
	backupProgressListener func(completedMBs float64),
) (*common.BackupMetadata, error) {
	backupProgressListener(10000)
	return &common.BackupMetadata{
		EncryptionSalt: nil,
		EncryptionIV:   nil,
		Encryption:     backups_config.BackupEncryptionNone,
	}, nil
}

// CreateProgressiveBackupUsecase simulates progressive size updates that exceed limit
type CreateProgressiveBackupUsecase struct{}

func (uc *CreateProgressiveBackupUsecase) Execute(
	ctx context.Context,
	backupID uuid.UUID,
	backupType backups_core.BackupType,
	backupConfig *backups_config.BackupConfig,
	database *databases.Database,
	storage *storages.Storage,
	backupProgressListener func(completedMBs float64),
) (*common.BackupMetadata, error) {
	// Simulate progressive backup that grows beyond limit
	backupProgressListener(1)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	backupProgressListener(3)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	backupProgressListener(5)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	backupProgressListener(10) // This exceeds the 5 MB limit
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Should not reach here due to cancellation
	return &common.BackupMetadata{
		EncryptionSalt: nil,
		EncryptionIV:   nil,
		Encryption:     backups_config.BackupEncryptionNone,
	}, nil
}

// CreateMediumBackupUsecase simulates a 50 MB backup
type CreateMediumBackupUsecase struct{}

func (uc *CreateMediumBackupUsecase) Execute(
	ctx context.Context,
	backupID uuid.UUID,
	backupType backups_core.BackupType,
	backupConfig *backups_config.BackupConfig,
	database *databases.Database,
	storage *storages.Storage,
	backupProgressListener func(completedMBs float64),
) (*common.BackupMetadata, error) {
	backupProgressListener(50)
	return &common.BackupMetadata{
		EncryptionSalt: nil,
		EncryptionIV:   nil,
		Encryption:     backups_config.BackupEncryptionNone,
	}, nil
}

// MockTrackingBackupUsecase tracks backup use case calls for testing parallel execution
type MockTrackingBackupUsecase struct {
	callCount       atomic.Int32
	calledBackupIDs chan uuid.UUID
}

func NewMockTrackingBackupUsecase() *MockTrackingBackupUsecase {
	return &MockTrackingBackupUsecase{
		calledBackupIDs: make(chan uuid.UUID, 10),
	}
}

func (m *MockTrackingBackupUsecase) Execute(
	ctx context.Context,
	backupID uuid.UUID,
	backupType backups_core.BackupType,
	backupConfig *backups_config.BackupConfig,
	database *databases.Database,
	storage *storages.Storage,
	backupProgressListener func(completedMBs float64),
) (*common.BackupMetadata, error) {
	m.callCount.Add(1)

	// Send backup ID to channel (non-blocking)
	select {
	case m.calledBackupIDs <- backupID:
	default:
	}

	// Simulate backup work
	time.Sleep(100 * time.Millisecond)
	backupProgressListener(10)

	return &common.BackupMetadata{
		EncryptionSalt: nil,
		EncryptionIV:   nil,
		Encryption:     backups_config.BackupEncryptionNone,
	}, nil
}

func (m *MockTrackingBackupUsecase) GetCallCount() int32 {
	return m.callCount.Load()
}

func (m *MockTrackingBackupUsecase) GetCalledBackupIDs() []uuid.UUID {
	ids := []uuid.UUID{}
	for {
		select {
		case id := <-m.calledBackupIDs:
			ids = append(ids, id)
		default:
			return ids
		}
	}
}
