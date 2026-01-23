package backuping

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"databasus-backend/internal/config"
	backups_core "databasus-backend/internal/features/backups/backups/core"
	backups_wal "databasus-backend/internal/features/backups/backups/wal"
	"databasus-backend/internal/features/databases"
	postgresql "databasus-backend/internal/features/databases/databases/postgresql"
	"databasus-backend/internal/features/storages"
	util_encryption "databasus-backend/internal/util/encryption"
	"databasus-backend/internal/util/tools"

	"github.com/google/uuid"
)

const (
	walScanInterval = 5 * time.Second
)

type WalReceiverService struct {
	storageService *storages.StorageService
	walService     *backups_wal.BackupWalService
	fieldEncryptor util_encryption.FieldEncryptor
	logger         *slog.Logger

	mu        sync.Mutex
	receivers map[uuid.UUID]context.CancelFunc
}

func (s *WalReceiverService) StartForBackup(
	backup *backups_core.Backup,
	database *databases.Database,
) error {
	if database.Type != databases.DatabaseTypePostgres || database.Postgresql == nil {
		return errors.New("WAL receiver is only supported for PostgreSQL databases")
	}

	if backup.Type != backups_core.BackupTypePITR {
		return nil
	}

	storage, err := s.storageService.GetStorageByID(backup.StorageID)
	if err != nil {
		return err
	}

	s.stopReceiver(database.ID)

	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.receivers[database.ID] = cancel
	s.mu.Unlock()

	go s.runReceiver(ctx, backup, database, storage)

	return nil
}

func (s *WalReceiverService) StopForDatabase(databaseID uuid.UUID) {
	s.stopReceiver(databaseID)
}

func (s *WalReceiverService) OnBeforeBackupRemove(backup *backups_core.Backup) error {
	s.stopReceiver(backup.DatabaseID)
	return nil
}

func (s *WalReceiverService) runReceiver(
	ctx context.Context,
	backup *backups_core.Backup,
	database *databases.Database,
	storage *storages.Storage,
) {
	tempFolder := config.GetEnv().TempFolder
	if err := os.MkdirAll(tempFolder, 0700); err != nil {
		s.logger.Error("Failed to ensure WAL temp folder exists", "error", err)
		return
	}

	tempDir, err := os.MkdirTemp(tempFolder, "wal_"+backup.ID.String())
	if err != nil {
		s.logger.Error("Failed to create WAL temp directory", "error", err)
		return
	}
	defer func() {
		if removeErr := os.RemoveAll(tempDir); removeErr != nil {
			s.logger.Error("Failed to remove WAL temp directory", "error", removeErr)
		}
	}()

	password, err := s.fieldEncryptor.Decrypt(database.ID, database.Postgresql.Password)
	if err != nil {
		s.logger.Error("Failed to decrypt database password for WAL receiver", "error", err)
		return
	}

	pgpassFile, err := s.createTempPgpassFile(database.Postgresql, password)
	if err != nil {
		s.logger.Error("Failed to create .pgpass file for WAL receiver", "error", err)
		return
	}
	defer func() {
		if pgpassFile != "" {
			_ = os.RemoveAll(filepath.Dir(pgpassFile))
		}
	}()

	cmd := exec.CommandContext(ctx, s.resolveReceiveWalPath(database.Postgresql.Version),
		"-D", tempDir,
		"--no-password",
		"-h", database.Postgresql.Host,
		"-p", fmt.Sprintf("%d", database.Postgresql.Port),
		"-U", database.Postgresql.Username,
	)
	cmd.Env = append(os.Environ(), "PGPASSFILE="+pgpassFile)

	if database.Postgresql.IsHttps {
		cmd.Env = append(cmd.Env, "PGSSLMODE=require")
	} else {
		cmd.Env = append(cmd.Env, "PGSSLMODE=prefer")
	}

	if err := cmd.Start(); err != nil {
		s.logger.Error("Failed to start pg_receivewal", "error", err)
		return
	}

	s.logger.Info("WAL receiver started", "backupId", backup.ID, "databaseId", database.ID)

	ticker := time.NewTicker(walScanInterval)
	defer ticker.Stop()

	go func() {
		if waitErr := cmd.Wait(); waitErr != nil {
			s.logger.Error("pg_receivewal exited", "error", waitErr)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			s.uploadWalFiles(ctx, tempDir, backup, storage)
			s.logger.Info("WAL receiver stopped", "backupId", backup.ID, "databaseId", database.ID)
			return
		case <-ticker.C:
			s.uploadWalFiles(ctx, tempDir, backup, storage)
		}
	}
}

func (s *WalReceiverService) uploadWalFiles(
	ctx context.Context,
	dir string,
	backup *backups_core.Backup,
	storage *storages.Storage,
) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		s.logger.Error("Failed to read WAL directory", "error", err)
		return
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if strings.HasSuffix(name, ".partial") {
			continue
		}

		path := filepath.Join(dir, name)
		info, err := entry.Info()
		if err != nil {
			s.logger.Error("Failed to stat WAL file", "file", name, "error", err)
			continue
		}

		if info.Size() == 0 {
			continue
		}

		if err := s.uploadSingleWal(ctx, backup, storage, name, path, info); err != nil {
			s.logger.Error("Failed to upload WAL file", "file", name, "error", err)
			continue
		}

		if err := os.Remove(path); err != nil {
			s.logger.Error("Failed to remove WAL file after upload", "file", name, "error", err)
		}
	}
}

func (s *WalReceiverService) uploadSingleWal(
	ctx context.Context,
	backup *backups_core.Backup,
	storage *storages.Storage,
	filename string,
	path string,
	info fs.FileInfo,
) error {
	_, err := s.walService.CreateWalSegment(
		ctx,
		backup.ID,
		storage.ID,
		filename,
		path,
	)
	if err != nil {
		return err
	}

	s.logger.Info(
		"WAL segment uploaded",
		"backupId",
		backup.ID,
		"walFilename",
		filename,
		"sizeBytes",
		info.Size(),
	)

	return nil
}

func (s *WalReceiverService) stopReceiver(databaseID uuid.UUID) {
	s.mu.Lock()
	cancel, exists := s.receivers[databaseID]
	if exists {
		delete(s.receivers, databaseID)
	}
	s.mu.Unlock()

	if exists {
		cancel()
	}
}

func (s *WalReceiverService) resolveReceiveWalPath(version tools.PostgresqlVersion) string {
	pgBin := tools.GetPostgresqlExecutable(
		version,
		"pg_receivewal",
		config.GetEnv().EnvMode,
		config.GetEnv().PostgresesInstallDir,
	)

	if _, err := os.Stat(pgBin); err == nil {
		return pgBin
	}

	fallbackBin, err := exec.LookPath("pg_receivewal")
	if err == nil {
		s.logger.Warn(
			"pg_receivewal not found for requested PostgreSQL version, falling back to PATH",
			"version",
			version,
			"fallback",
			fallbackBin,
			"requestedPath",
			pgBin,
		)
		return fallbackBin
	}

	s.logger.Error(
		"pg_receivewal not found for requested PostgreSQL version",
		"version",
		version,
		"requestedPath",
		pgBin,
	)

	return pgBin
}

func (s *WalReceiverService) createTempPgpassFile(
	pgConfig *postgresql.PostgresqlDatabase,
	password string,
) (string, error) {
	if pgConfig == nil || password == "" {
		return "", nil
	}

	escapedHost := tools.EscapePgpassField(pgConfig.Host)
	escapedUsername := tools.EscapePgpassField(pgConfig.Username)
	escapedPassword := tools.EscapePgpassField(password)

	pgpassContent := fmt.Sprintf("%s:%d:*:%s:%s",
		escapedHost,
		pgConfig.Port,
		escapedUsername,
		escapedPassword,
	)

	tempFolder := config.GetEnv().TempFolder
	if err := os.MkdirAll(tempFolder, 0700); err != nil {
		return "", fmt.Errorf("failed to ensure temp folder exists: %w", err)
	}
	if err := os.Chmod(tempFolder, 0700); err != nil {
		return "", fmt.Errorf("failed to set temp folder permissions: %w", err)
	}

	tempDir, err := os.MkdirTemp(tempFolder, "pgpass_"+uuid.New().String())
	if err != nil {
		return "", fmt.Errorf("failed to create temporary directory: %w", err)
	}

	if err := os.Chmod(tempDir, 0700); err != nil {
		_ = os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to set temporary directory permissions: %w", err)
	}

	pgpassFile := filepath.Join(tempDir, ".pgpass")
	if err := os.WriteFile(pgpassFile, []byte(pgpassContent), 0600); err != nil {
		_ = os.RemoveAll(tempDir)
		return "", fmt.Errorf("failed to write temporary .pgpass file: %w", err)
	}

	return pgpassFile, nil
}
