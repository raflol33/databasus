package wal

import (
	"databasus-backend/internal/features/storages"
	"databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/logger"
)

var backupWalRepository = &BackupWalRepository{}

var backupWalService = &BackupWalService{
	backupWalRepository,
	storages.GetStorageService(),
	encryption.GetFieldEncryptor(),
	logger.GetLogger(),
}

func GetBackupWalService() *BackupWalService {
	return backupWalService
}

func GetBackupWalRepository() *BackupWalRepository {
	return backupWalRepository
}
